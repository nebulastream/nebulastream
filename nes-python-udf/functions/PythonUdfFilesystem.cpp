/*
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

#include <PythonUdfFilesystem.hpp>

#include <algorithm>
#include <cctype>
#include <chrono>
#include <cstddef>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <ios>
#include <memory>
#include <ratio>
#include <set>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>
#include <NESCodonPlugin/Registry.hpp>
#include <Util/Logger/Logger.hpp>
#include <codon/cir/llvm/optimize.h>
#include <codon/compiler/compiler.h>
#include <codon/parser/ast/error.h>
#include <codon/parser/ast/node.h>
#include <codon/parser/ast/stmt.h>
#include <codon/parser/cache.h>
#include <codon/parser/common.h>
#include <codon/parser/peg/peg.h>
#include <codon/parser/visitors/scoping/scoping.h>
#include <codon/parser/visitors/translate/translate.h>
#include <codon/parser/visitors/translate/translate_ctx.h>
#include <codon/parser/visitors/typecheck/ctx.h>
#include <codon/parser/visitors/typecheck/typecheck.h>
#include <codon/parser/visitors/visitor.h>
#include <codon/util/common.h>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <llvm/ADT/SmallVector.h>
#include <llvm/Bitcode/BitcodeReader.h>
#include <llvm/Bitcode/BitcodeWriter.h>
#include <llvm/IR/DebugInfoMetadata.h>
#include <llvm/IR/GlobalVariable.h>
#include <llvm/IR/IRBuilder.h>
#include <llvm/IR/InstIterator.h>
#include <llvm/IR/Instructions.h>
#include <llvm/IR/Module.h>
#include <llvm/Linker/Linker.h>
#include <llvm/Passes/PassBuilder.h>
#include <llvm/Support/Error.h>
#include <llvm/Support/MemoryBuffer.h>
#include <llvm/Support/raw_ostream.h>
#include <llvm/Transforms/IPO/GlobalDCE.h>

namespace NES
{
namespace
{
std::string_view pluginModuleSource(const std::filesystem::path& path)
{
    const auto normalizedPath = path.lexically_normal();
    return findCodonPluginModule(normalizedPath.generic_string());
}

/// nes-codon-compat-plugin provides Python builtins Codon lacks (bytes, a stand-in for bytearray). While it is loaded,
/// every UDF module imports them, so UDF code that names those builtins compiles unchanged.
constexpr std::string_view compatModulePath = "nes_compat.codon";
constexpr std::string_view compatImport = "from nes_compat import *";

bool isCompatPluginLoaded()
{
    return !findCodonPluginModule(compatModulePath).empty();
}

std::vector<std::string> splitIntoLines(const std::string_view source)
{
    std::vector<std::string> lines;
    size_t position = 0;
    while (position <= source.size())
    {
        const auto lineEnd = source.find('\n', position);
        lines.emplace_back(source.substr(position, lineEnd == std::string_view::npos ? source.size() - position : lineEnd - position));
        if (lineEnd == std::string_view::npos)
        {
            break;
        }
        position = lineEnd + 1;
    }
    return lines;
}

class PythonUdfFilesystem final : public codon::ast::ResourceFilesystem
{
public:
    PythonUdfFilesystem(const std::string& argv0, const std::string& module0, const std::vector<std::string>& importPaths)
        : ResourceFilesystem(argv0, module0, !importPaths.empty())
    {
        for (const auto& importPath : importPaths)
        {
            add_search_path(importPath);
            udfDirectories.push_back(std::filesystem::absolute(importPath).lexically_normal());
        }
    }

    [[nodiscard]] std::vector<std::string> read_lines(const path_t& path) const override
    {
        if (const auto source = pluginModuleSource(path); !source.empty())
        {
            return splitIntoLines(source);
        }
        auto lines = ResourceFilesystem::read_lines(path);
        if (isUdfModule(path) && isCompatPluginLoaded())
        {
            /// Prepended rather than merged into the first line, so compile errors in the module are one line off.
            lines.insert(lines.begin(), std::string{compatImport});
        }
        return lines;
    }

    [[nodiscard]] bool exists(const path_t& path) const override
    {
        return !pluginModuleSource(path).empty() || ResourceFilesystem::exists(path);
    }

    [[nodiscard]] path_t canonical(const path_t& path) const override
    {
        /// One path per plugin module. Codon would otherwise load a module once per importing directory, and the classes
        /// of those copies would be different types.
        if (const auto modulePath = findCodonPluginModulePath(path.lexically_normal().generic_string()); !modulePath.empty())
        {
            return std::filesystem::path{"/nes-codon-plugins"} / modulePath;
        }
        return ResourceFilesystem::canonical(path);
    }

    /// A module loaded from a configured UDF import path, as opposed to Codon's stdlib or a plugin module.
    [[nodiscard]] bool isUdfModule(const path_t& path) const
    {
        const auto file = std::filesystem::absolute(path).lexically_normal();
        return std::ranges::any_of(
            udfDirectories,
            [&](const auto& directory)
            {
                const auto relative = file.lexically_relative(directory);
                return !relative.empty() && *relative.begin() != "..";
            });
    }

private:
    std::vector<std::filesystem::path> udfDirectories;
};

/// Finds import statements inside function bodies. `from C import` declares an external C function and imports no module.
class FunctionLocalImportFinder final : public codon::ast::CallbackASTVisitor<bool, bool>
{
public:
    struct LocalImport
    {
        std::string function;
        int line;
    };

    bool transform(codon::ast::Expr* expression) override
    {
        if (expression != nullptr)
        {
            expression->accept(*this);
        }
        return true;
    }

    bool transform(codon::ast::Stmt* statement) override
    {
        if (statement != nullptr)
        {
            statement->accept(*this);
        }
        return true;
    }

    void visit(codon::ast::FunctionStmt* statement) override
    {
        functions.push_back(statement->getName());
        CallbackASTVisitor::visit(statement);
        functions.pop_back();
    }

    void visit(codon::ast::ImportStmt* statement) override
    {
        if (functions.empty())
        {
            return;
        }
        if (const auto* from = codon::ast::cast<codon::ast::IdExpr>(statement->getFrom()); from != nullptr && from->getValue() == "C")
        {
            return;
        }
        imports.push_back({.function = functions.back(), .line = statement->getSrcInfo().line});
    }

    [[nodiscard]] const std::vector<LocalImport>& found() const { return imports; }

private:
    std::vector<std::string> functions;
    std::vector<LocalImport> imports;
};

/// The functions in the LLVM module, as (source file, function name) from their debug information. Codon emits a function
/// only if the program uses it.
using CompiledFunctions = std::set<std::pair<std::string, std::string>>;

CompiledFunctions compiledFunctions(const llvm::Module& module)
{
    CompiledFunctions functions;
    for (const auto& function : module)
    {
        if (const auto* subprogram = function.getSubprogram(); subprogram != nullptr && !function.isDeclaration())
        {
            const auto path
                = (std::filesystem::path{subprogram->getDirectory().str()} / subprogram->getFilename().str()).lexically_normal();
            functions.emplace(path.string(), subprogram->getName().str());
        }
    }
    return functions;
}

/// Codon names a method or a nested function with a prefix, e.g. `Scalar.addnoise`. A match on the last part may also match
/// another function of the same name in the file, which only rejects more UDFs.
bool isCompiled(const CompiledFunctions& functions, const std::string& file, const std::string& function)
{
    const auto path = std::filesystem::path{file}.lexically_normal().string();
    return std::ranges::any_of(
        functions,
        [&](const auto& compiled)
        {
            const auto& [compiledFile, name] = compiled;
            if (compiledFile != path || !name.ends_with(function))
            {
                return false;
            }
            const auto prefixLength = name.size() - function.size();
            return prefixLength == 0 || name[prefixLength - 1] == '.' || name[prefixLength - 1] == ':';
        });
}

/// An INLINE UDF must import modules at file level. Codon runs the top-level code of a module that a function imports at the
/// first call of that function. NES runs that call with the per-invocation arena, which it reuses, so the module's globals
/// would point into memory that later calls overwrite. A module imported at file level runs in the module initializer,
/// whose allocations NES keeps (see addModuleInitializationPrologue).
/// The check reads the source, not the LLVM IR: Codon emits no import for a function-local import of a module that is
/// already imported, so the IR cannot show every function-local import. The IR only tells which functions the UDF uses:
/// an unused function of an imported module does not cause a rejection.
void rejectFunctionLocalImports(
    codon::ast::Cache& cache, const CompiledFunctions& functions, const std::string& file, const std::string& source)
{
    auto statement = codon::ast::parseCode(&cache, file, source);
    if (!statement)
    {
        /// Codon parsed the module already, so this does not happen. The compilation reports parse errors.
        llvm::consumeError(statement.takeError());
        return;
    }
    FunctionLocalImportFinder finder;
    finder.transform(*statement);
    const auto used
        = std::ranges::find_if(finder.found(), [&](const auto& localImport) { return isCompiled(functions, file, localImport.function); });
    if (used == finder.found().end())
    {
        return;
    }
    const auto lines = splitIntoLines(source);
    const auto& [function, line] = *used;
    const auto text = line >= 1 && static_cast<size_t>(line) <= lines.size() ? lines[static_cast<size_t>(line) - 1] : std::string{};
    const auto trimmed = text.substr(std::min(text.find_first_not_of(" \t"), text.size()));
    throw std::runtime_error(
        fmt::format(
            "Python UDF for EXECUTION INLINE imports a module inside function '{}' ({}:{}: {}). Import the module at the top of the "
            "file. Codon would initialize a module imported inside a function at the first call, in memory that NES reuses.",
            function,
            file,
            line,
            trimmed));
}

std::string readFile(const std::filesystem::path& path)
{
    const std::ifstream file{path, std::ios::binary};
    std::ostringstream content;
    content << file.rdbuf();
    return content.str();
}

/// codon::ast::Cache allocates the vector that owns every AST node with `new` and has no destructor, so destroying a
/// codon::Compiler leaves the whole parsed program (stdlib and imported modules included) allocated. The member is
/// private; an explicit template instantiation may name it.
using CacheNodesMember = std::vector<std::unique_ptr<codon::ast::ASTNode>>* codon::ast::Cache::*;
CacheNodesMember cacheNodesMember();

template <CacheNodesMember Member>
struct CacheNodesAccess
{
    friend CacheNodesMember cacheNodesMember() { return Member; }
};

template struct CacheNodesAccess<&codon::ast::Cache::_nodes>;

/// The AST nodes are deleted after the compiler, as the compiler's members may still refer to them while being destroyed.
void deleteCodonCompiler(codon::Compiler* compiler)
{
    auto* nodes = compiler->getCache()->*cacheNodesMember();
    delete compiler; /// NOLINT(cppcoreguidelines-owning-memory)
    delete nodes; /// NOLINT(cppcoreguidelines-owning-memory)
}

/// Codon runs top-level code, including the stdlib's module globals (e.g. the module cache of `from python import`), in
/// the initializer its `main` invokes, which NES never calls. The entry point calls it once per module instead. This runs
/// before Codon's LLVM optimization, which would otherwise inline the initializer into the discarded `main`.
/// The initializer that Codon's `main` runs through codon.proxy_main: the top-level code of the program.
llvm::Function* findCodonInitializer(llvm::Module& module)
{
    auto* proxyMain = module.getFunction("codon.proxy_main");
    if (proxyMain == nullptr || proxyMain->isDeclaration())
    {
        return nullptr;
    }
    llvm::Function* initializer = nullptr;
    for (auto& instruction : llvm::instructions(*proxyMain))
    {
        if (const auto* call = llvm::dyn_cast<llvm::CallBase>(&instruction))
        {
            if (auto* callee = call->getCalledFunction(); callee != nullptr && !callee->isDeclaration())
            {
                initializer = callee;
            }
        }
    }
    return initializer;
}

void addModuleInitializationPrologue(llvm::Module& module, const std::string& symbol, llvm::Function* initializer)
{
    auto* entryPoint = module.getFunction(symbol);
    if (initializer == nullptr || entryPoint == nullptr || entryPoint->isDeclaration())
    {
        return;
    }

    auto& context = module.getContext();
    auto* int8Type = llvm::Type::getInt8Ty(context);
    auto* pointerType = llvm::PointerType::getUnqual(context);
    auto* initialized = new llvm::GlobalVariable(
        module, int8Type, false, llvm::GlobalValue::InternalLinkage, llvm::ConstantInt::get(int8Type, 0), symbol + ".initialized");
    const auto ensureInitialized = module.getOrInsertFunction(
        "nes_codon_ensure_module_initialized", llvm::FunctionType::get(llvm::Type::getVoidTy(context), {pointerType, pointerType}, false));
    llvm::IRBuilder<> builder(&*entryPoint->getEntryBlock().getFirstInsertionPt());
    builder.CreateCall(ensureInitialized, {initializer, initialized});

    /// The initializer and the top-level code of each module (%_import_<module>_<n>_call) run once per pipeline, so they are
    /// not optimized: a large literal at module level, e.g. a set of 3800 strings, becomes one basic block of 15000
    /// instructions, on which the LLVM optimizations and the instruction scheduler take tens of seconds.
    const auto skipOptimization = [](llvm::Function& function)
    {
        function.removeFnAttr(llvm::Attribute::AlwaysInline);
        function.addFnAttr(llvm::Attribute::NoInline);
        function.addFnAttr(llvm::Attribute::OptimizeNone);
    };
    skipOptimization(*initializer);
    for (auto& function : module)
    {
        if (!function.isDeclaration() && function.getName().starts_with("%_import_"))
        {
            skipOptimization(function);
        }
    }
}

double elapsedMilliseconds(const std::chrono::steady_clock::time_point start, const std::chrono::steady_clock::time_point end)
{
    return std::chrono::duration<double, std::milli>{end - start}.count();
}

std::string writeBitcode(const llvm::Module& module)
{
    llvm::SmallVector<char, 0> bitcode;
    llvm::raw_svector_ostream bitcodeStream(bitcode);
    llvm::WriteBitcodeToFile(module, bitcodeStream);
    return {bitcode.data(), bitcode.size()};
}

std::string parserErrorMessage(const codon::exc::ParserException& exception)
{
    return exception.what();
}

/// An LLVM module with its context. The module is destroyed first: std::pair, which Codon's takeModule returns, destroys
/// the context first.
struct OwnedLlvmModule
{
    explicit OwnedLlvmModule(std::pair<std::unique_ptr<llvm::Module>, std::unique_ptr<llvm::LLVMContext>> taken)
        : context(std::move(taken.second)), module(std::move(taken.first))
    {
    }

    std::unique_ptr<llvm::LLVMContext> context;
    std::unique_ptr<llvm::Module> module;
};

/// The state of Codon's type checker and translator, as codon::jit::JITState keeps it (codon/compiler/jit.h, which needs
/// Codon's JIT engine).
class CodonStateSnapshot
{
public:
    explicit CodonStateSnapshot(codon::ast::Cache& cache)
        : cache(cache)
        , mainContext(*cache.imports[MAIN_IMPORT].ctx)
        , stdlibContext(*cache.imports[STDLIB_IMPORT].ctx)
        , typeContext(*cache.typeCtx)
        , translateContext(*cache.codegenCtx)
    {
    }

    void restore(codon::ast::Cache& target) const
    {
        target = cache;
        *target.imports[MAIN_IMPORT].ctx = mainContext;
        *target.imports[STDLIB_IMPORT].ctx = stdlibContext;
        *target.typeCtx = typeContext;
        *target.codegenCtx = translateContext;
    }

private:
    codon::ast::Cache cache;
    codon::ast::TypeContext mainContext;
    codon::ast::TypeContext stdlibContext;
    codon::ast::TypeContext typeContext;
    codon::ast::TranslateContext translateContext;
};

/// Rejects imports inside functions, makes the entry point run the initializer once, and optimizes the module.
void finishCodonModule(
    llvm::Module& module,
    llvm::Function* initializer,
    codon::ast::Cache& cache,
    const PythonUdfFilesystem& filesystem,
    const std::string& sourcePath,
    const std::string& source,
    const std::string& entrySymbol)
{
    const auto functions = compiledFunctions(module);
    rejectFunctionLocalImports(cache, functions, sourcePath, source);
    for (const auto& [name, imported] : cache.imports)
    {
        if (!imported.filename.empty() && imported.filename != sourcePath && filesystem.isUdfModule(imported.filename))
        {
            rejectFunctionLocalImports(cache, functions, imported.filename, readFile(imported.filename));
        }
    }
    addModuleInitializationPrologue(module, entrySymbol, initializer);
}

/// A Codon compiler that lives as long as the worker. It parses and type checks the stdlib once, and compiles each UDF as
/// a cell of Codon's JIT mode, which type checks only the new code. A fresh compiler parses the stdlib again for each UDF,
/// which takes most of the time of a small UDF (about 3.5 s).
///
/// In JIT mode, Codon emits for each cell an LLVM module with the functions that are new since the previous cell; the
/// others are declarations. The stdlib module, emitted once, holds the rest, so linking the two gives a complete module.
/// After each UDF, the compiler returns to its base state (as JITState::undo does), and the IR of the UDF is removed: each
/// UDF imports its modules again, so a changed UDF file takes effect, and the stdlib functions that a UDF realizes are in
/// its own module. The base state is the stdlib, and the stdlib and plugin modules that earlier UDFs imported, e.g. numpy:
/// after a UDF, the compiler imports these into the base state (extendBase), so later UDFs do not parse and type check
/// them again.
///
/// NES_CODON_FRESH_COMPILER=1 compiles each UDF with a new compiler instead.
class PersistentCodonCompiler
{
public:
    PersistentCodonCompiler()
        : compiler(
              new codon::Compiler( /// NOLINT(cppcoreguidelines-owning-memory)
                  "nes-worker",
                  codon::Compiler::Mode::JIT,
                  std::vector<std::string>{},
                  false,
                  false,
                  false,
                  std::make_shared<PythonUdfFilesystem>("nes-worker", "", std::vector<std::string>{})),
              &deleteCodonCompiler)
    {
        if (const auto* debug = std::getenv("CODON_DEBUG")) /// TEMPORARY profiling
        {
            codon::getLogger().parse(debug);
        }
        auto* cache = compiler->getCache();
        auto* visitor = compiler->getLLVMVisitor();
        visitor->setDebug(true);
        visitor->setJIT(true);
        try
        {
            cache->isJit = true;
            auto* typechecked = codon::ast::TypecheckVisitor::apply(
                cache, cache->N<codon::ast::SuiteStmt>(), std::string{stdlibFile}, {}, compiler->getEarlyDefines());
            /// As in codon::jit::JIT::init: Codon creates the main function only while isJit is false.
            cache->isJit = false;
            codon::ast::TranslateVisitor::apply(cache, typechecked);
            cache->isJit = true;
        }
        catch (const codon::exc::ParserException& exception)
        {
            throw std::runtime_error("Could not load the Codon stdlib: " + parserErrorMessage(exception));
        }
        compiler->getModule()->setSrcInfo({std::string{stdlibFile}, 0, 0, 0});
        compiler->getPassManager()->run(compiler->getModule());
        compiler->getModule()->accept(*visitor);
        const OwnedLlvmModule stdlib{visitor->takeModule(compiler->getModule())};
        auto* initializer = findCodonInitializer(*stdlib.module);
        if (initializer == nullptr)
        {
            throw std::runtime_error("The Codon stdlib module has no initializer");
        }
        /// The stdlib's initializer is internal. UDF modules link it from the base module by name.
        initializer->setLinkage(llvm::GlobalValue::ExternalLinkage);
        baseInitializers.push_back(initializer->getName().str());
        baseCellSymbols.push_back(definedSymbols(*stdlib.module));
        baseBitcode = writeBitcode(*stdlib.module);
        markBase();
    }

    /// Compiles a generated UDF source. The caller holds the Codon compiler mutex.
    CodonCompilationResult compile(
        const std::string& sourcePath,
        const std::string& source,
        const std::string& entrySymbol,
        const std::vector<std::string>& importPaths)
    {
        auto* cache = compiler->getCache();
        auto* irModule = compiler->getModule();
        auto filesystem = std::make_shared<PythonUdfFilesystem>("nes-worker", sourcePath, importPaths);
        cache->fs = filesystem;

        /// Restores the state after the stdlib, also when the UDF does not compile.
        const auto restore = [&]
        {
            snapshot->restore(*cache);
            std::vector<codon::ir::Var*> added;
            for (auto* node : *irModule)
            {
                if (!baseNodes.contains(node->getId()))
                {
                    added.push_back(node);
                }
            }
            for (auto* node : added)
            {
                irModule->remove(node);
            }
            /// The IR types are looked up by name, and a type of an earlier UDF would apply to a later one: each UDF defines a
            /// body function of the same name, whose type depends on the UDF.
            std::vector<codon::ir::types::Type*> addedTypes;
            for (auto type = irModule->types_begin(); type != irModule->types_end(); ++type)
            {
                if (!baseTypes.contains((*type)->getName()))
                {
                    addedTypes.push_back(*type);
                }
            }
            for (auto* type : addedTypes)
            {
                irModule->remove(type);
            }
        };
        CodonCompilationResult result;
        std::set<std::string> newModules;
        try
        {
            result = compileCell(*filesystem, sourcePath, source, entrySymbol);
            newModules = modulesBeyondBase(*filesystem);
            const auto tr0 = std::chrono::steady_clock::now();
            restore();
            fprintf(stderr, "TMPTIME restore %.0f\n", elapsedMilliseconds(tr0, std::chrono::steady_clock::now()));
        }
        catch (...)
        {
            restore();
            throw;
        }
        if (!newModules.empty())
        {
            try
            {
                const auto te0 = std::chrono::steady_clock::now();
                extendBase(newModules);
                fprintf(stderr, "TMPTIME extend %.0f %s\n", elapsedMilliseconds(te0, std::chrono::steady_clock::now()), fmt::format("{}", fmt::join(newModules, ",")).c_str());
            }
            catch (const std::exception& exception)
            {
                restore();
                NES_WARNING("Could not keep the modules {} in the Codon base state: {}", fmt::join(newModules, ", "), exception.what());
            }
        }
        return result;
    }

private:
    static constexpr std::string_view stdlibFile = "<nes-codon-stdlib>";

    /// Parses, type checks and translates a cell: the code of a UDF, or imports for the base state.
    codon::ir::Func* frontEnd(const std::string& file, const std::string& source)
    {
        auto* cache = compiler->getCache();
        codon::ir::Func* cell = nullptr;
        try
        {
            auto parsed = codon::ast::parseCode(cache, file, source);
            if (!parsed)
            {
                throw std::runtime_error("Could not parse Python UDF: " + llvm::toString(parsed.takeError()));
            }
            const auto& context = cache->imports[MAIN_IMPORT].ctx;
            if (auto error = codon::ast::ScopingVisitor::apply(cache, *parsed, &context->globalShadows))
            {
                throw std::runtime_error("Could not parse Python UDF: " + llvm::toString(std::move(error)));
            }
            auto* typechecked = codon::ast::TypecheckVisitor::apply(context, *parsed, file);
            std::vector<codon::ast::Stmt*> statements{typechecked};
            for (const auto& [function, realization] : cache->pendingRealizations)
            {
                statements.push_back(cache->functions[function].ast);
            }
            cell = codon::ast::TranslateVisitor::apply(cache, cache->N<codon::ast::SuiteStmt>(statements));
            ++cache->jitCell;
        }
        catch (const codon::exc::ParserException& exception)
        {
            throw std::runtime_error("Could not parse Python UDF: " + parserErrorMessage(exception));
        }
        return cell;
    }

    /// The modules of the UDF that are neither UDF modules nor in the base state: stdlib modules such as numpy and plugin
    /// modules such as cv2, as top-level packages. internal is the stdlib itself.
    std::set<std::string> modulesBeyondBase(const PythonUdfFilesystem& filesystem) const
    {
        std::set<std::string> modules;
        for (const auto& [key, imported] : compiler->getCache()->imports)
        {
            if (baseImports.contains(key) || imported.name.empty() || imported.name.starts_with(".")
                || filesystem.isUdfModule(imported.filename))
            {
                continue;
            }
            /// A plugin module's name follows its canonical path (/nes-codon-plugins/<path>), so take the package from the path.
            constexpr std::string_view pluginRoot = "/nes-codon-plugins/";
            std::string package = imported.filename.starts_with(pluginRoot)
                ? imported.filename.substr(pluginRoot.size(), imported.filename.find_first_of("/.", pluginRoot.size()) - pluginRoot.size())
                : imported.name.substr(0, imported.name.find('.'));
            const bool identifier = !package.empty() && !std::isdigit(static_cast<unsigned char>(package[0]))
                && std::ranges::all_of(package, [](char c) { return std::isalnum(static_cast<unsigned char>(c)) || c == '_'; });
            if (identifier && package != "internal")
            {
                modules.insert(package);
            }
        }
        return modules;
    }

    /// Imports the modules into the base state, so that later UDFs do not parse and type check them again. Their top-level
    /// code runs in the initializer of each UDF, after the stdlib's, as a module imported by the UDF would.
    void extendBase(const std::set<std::string>& modules)
    {
        auto* cache = compiler->getCache();
        const auto file = fmt::format("<nes-codon-base-{}>", baseInitializers.size());
        cache->fs = std::make_shared<PythonUdfFilesystem>("nes-worker", file, std::vector<std::string>{});
        std::string source;
        for (const auto& module : modules)
        {
            source += "import " + module + "\n";
        }
        auto* cell = frontEnd(file, source);
        compiler->getPassManager()->run(compiler->getModule());
        const OwnedLlvmModule owned{compiler->getLLVMVisitor()->takeModule(compiler->getModule())};
        auto symbols = definedSymbols(*owned.module);
        linkBase(*owned.module, *owned.context, llvm::Linker::Flags::None);
        baseInitializers.push_back(codon::ir::LLVMVisitor::getNameForFunction(cell));
        baseCellSymbols.push_back(std::move(symbols));
        baseBitcode = writeBitcode(*owned.module);
        markBase();
    }

    /// Links the base module into the module of a cell, which declares the functions of the base that it uses. With
    /// LinkOnlyNeeded, only these functions and what they use are read from the bitcode and linked.
    void linkBase(llvm::Module& module, llvm::LLVMContext& context, const unsigned flags) const
    {
        auto base = llvm::getLazyBitcodeModule(llvm::MemoryBufferRef{baseBitcode, "nes-codon-base"}, context);
        if (!base)
        {
            throw std::runtime_error("Could not read the Codon base module: " + llvm::toString(base.takeError()));
        }
        if (llvm::Linker::linkModules(module, std::move(*base), flags))
        {
            throw std::runtime_error("Could not link the Codon base module");
        }
    }

    /// Runs Codon's IR passes only on the functions and global variables that are new since the base state. The base ones
    /// are already optimized, and their LLVM code is in the base module; the analyses treat them conservatively.
    void runPassesOnNewCode()
    {
        auto* irModule = compiler->getModule();
        if (std::getenv("NES_CODON_ALL_PASSES") == nullptr) /// TEMPORARY comparison
        {
            irModule->hideVars([this](const codon::ir::Var* var) { return baseNodes.contains(var->getId()); });
        }
        try
        {
            compiler->getPassManager()->run(irModule);
        }
        catch (...)
        {
            irModule->unhideVars();
            throw;
        }
        irModule->unhideVars();
    }

    /// Records the current state as the base state, which each UDF returns to.
    void markBase()
    {
        baseNodes.clear();
        for (const auto* node : *compiler->getModule())
        {
            baseNodes.insert(node->getId());
        }
        baseTypes.clear();
        for (auto type = compiler->getModule()->types_begin(); type != compiler->getModule()->types_end(); ++type)
        {
            baseTypes.insert((*type)->getName());
        }
        baseImports.clear();
        for (const auto& [key, imported] : compiler->getCache()->imports)
        {
            baseImports.insert(key);
        }
        snapshot = std::make_unique<CodonStateSnapshot>(*compiler->getCache());
    }

    CodonCompilationResult compileCell(
        const PythonUdfFilesystem& filesystem, const std::string& sourcePath, const std::string& source, const std::string& entrySymbol)
    {
        const auto parseStart = std::chrono::steady_clock::now();
        auto* cell = frontEnd(sourcePath, source);
        auto* cache = compiler->getCache();

        const auto compileStart = std::chrono::steady_clock::now();
        runPassesOnNewCode();
        const auto tPasses = std::chrono::steady_clock::now();
        const OwnedLlvmModule owned{compiler->getLLVMVisitor()->takeModule(compiler->getModule())};
        const auto tTake = std::chrono::steady_clock::now();
        auto& module = owned.module;
        const auto initializers = declareUsedBaseInitializers(*module);
        linkBase(*module, *owned.context, llvm::Linker::Flags::LinkOnlyNeeded);
        const auto tLink = std::chrono::steady_clock::now();
        const auto cellName = codon::ir::LLVMVisitor::getNameForFunction(cell);
        auto needed = neededBaseCells(*module, initializers, cellName, entrySymbol);
        if (std::getenv("NES_TMP_SKIP_BASE_INIT") != nullptr) /// TEMPORARY: measures LLVM without base top-level code
        {
            needed = {0};
        }
        fprintf(stderr, "TMPTIME passes %.0f take %.0f link %.0f cells %zu/%zu/%zu funcs %zu\n", elapsedMilliseconds(compileStart, tPasses), elapsedMilliseconds(tPasses, tTake), elapsedMilliseconds(tTake, tLink), needed.size(), initializers, baseInitializers.size(), module->size());
        allocateVtablesPersistently(*module);
        auto* initializer = createInitializer(*module, needed, cellName);
        finishCodonModule(*module, initializer, *cache, filesystem, sourcePath, source, entrySymbol);

        const auto optimizeStart = std::chrono::steady_clock::now();
        keepOnlyWhatEntryUses(*module, entrySymbol);
        {
            std::size_t instructions = 0;
            std::size_t functions = 0;
            for (auto& function : *module)
            {
                functions += function.isDeclaration() ? 0 : 1;
                instructions += function.getInstructionCount();
            }
            fprintf(stderr, "TMPTIME after-dce functions %zu instructions %zu\n", functions, instructions);
        }
        /// One O1 run, not Codon's release setting (two O3 runs): Nautilus links the UDF into the pipeline and optimizes it
        /// again with O3. Codon's own passes (AllocationRemover, AllocationHoister) run only here and already run at O1;
        /// without them, UDFs that allocate per row lose up to a quarter of their throughput (E60).
        auto optimizationOptions = codon::ir::LLVMOptimizationOptions::release();
        optimizationOptions.level = llvm::OptimizationLevel::O1;
        optimizationOptions.runs = 1;
        optimizationOptions.preserveDebugInfo = true;
        codon::ir::optimize(module.get(), optimizationOptions);
        const auto optimizeEnd = std::chrono::steady_clock::now();
        return {
            .llvmBitcode = writeBitcode(*module),
            .parseMilliseconds = elapsedMilliseconds(parseStart, compileStart),
            .compileMilliseconds = elapsedMilliseconds(compileStart, optimizeStart),
            .optimizeMilliseconds = elapsedMilliseconds(optimizeStart, optimizeEnd)};
    }

    /// In JIT mode, the top-level code of each cell sets up the vtables (RTTIType._init_vtables), which Codon allocates with
    /// alloc_atomic_uncollectable. NES rejects uncollectable allocations in UDF code, which would leak at each call. The
    /// initializer runs once per pipeline, and NES keeps what it allocates, so the vtables use seq_alloc_atomic instead.
    static void allocateVtablesPersistently(llvm::Module& module)
    {
        auto& context = module.getContext();
        auto* pointerType = llvm::PointerType::getUnqual(context);
        const auto allocate = module.getOrInsertFunction(
            "seq_alloc_atomic", llvm::FunctionType::get(pointerType, {llvm::Type::getInt64Ty(context)}, false));
        for (auto& function : module)
        {
            if (!function.getName().starts_with("RTTIType._init_vtables"))
            {
                continue;
            }
            for (auto& instruction : llvm::instructions(function))
            {
                if (auto* call = llvm::dyn_cast<llvm::CallBase>(&instruction); call != nullptr && call->getCalledFunction() != nullptr
                    && call->getCalledFunction()->getName().contains("alloc_atomic_uncollectable"))
                {
                    call->setCalledFunction(allocate);
                }
            }
        }
    }

    /// The base module holds all functions of the base state, which Codon's JIT mode emits with external linkage. Internal,
    /// those the UDF does not use are removed before the optimizations, which would otherwise optimize them all.
    static void keepOnlyWhatEntryUses(llvm::Module& module, const std::string& entrySymbol)
    {
        for (auto& function : module.functions())
        {
            if (!function.isDeclaration() && function.getName() != entrySymbol)
            {
                function.setLinkage(llvm::GlobalValue::InternalLinkage);
            }
        }
        for (auto& global : module.globals())
        {
            if (!global.isDeclaration() && !global.getName().starts_with("llvm."))
            {
                global.setLinkage(llvm::GlobalValue::InternalLinkage);
            }
        }
        llvm::LoopAnalysisManager loopAnalysisManager;
        llvm::FunctionAnalysisManager functionAnalysisManager;
        llvm::CGSCCAnalysisManager cgsccAnalysisManager;
        llvm::ModuleAnalysisManager moduleAnalysisManager;
        llvm::PassBuilder passBuilder;
        passBuilder.registerModuleAnalyses(moduleAnalysisManager);
        passBuilder.registerCGSCCAnalyses(cgsccAnalysisManager);
        passBuilder.registerFunctionAnalyses(functionAnalysisManager);
        passBuilder.registerLoopAnalyses(loopAnalysisManager);
        passBuilder.crossRegisterProxies(loopAnalysisManager, functionAnalysisManager, cgsccAnalysisManager, moduleAnalysisManager);
        llvm::ModulePassManager passes;
        passes.addPass(llvm::GlobalDCEPass());
        passes.run(module, moduleAnalysisManager);
    }

    /// The functions and global variables that a module defines.
    static std::set<std::string> definedSymbols(const llvm::Module& module)
    {
        std::set<std::string> symbols;
        for (const auto& value : module.global_values())
        {
            if (!value.isDeclaration())
            {
                symbols.insert(value.getName().str());
            }
        }
        return symbols;
    }

    /// The base cells whose top-level code the UDF needs: the stdlib, and each cell up to the last one that defines a
    /// function or global variable the UDF uses. A cell can use the modules of the cells before it, so these run too.
    /// The UDF module declares the initializers, so that the linker links them.
    std::size_t declareUsedBaseInitializers(llvm::Module& module) const
    {
        std::size_t used = 1;
        for (const auto& value : module.global_values())
        {
            if (!value.isDeclaration())
            {
                continue;
            }
            for (std::size_t cell = used; cell < baseCellSymbols.size(); ++cell)
            {
                if (baseCellSymbols[cell].contains(value.getName().str()))
                {
                    used = cell + 1;
                }
            }
        }
        auto& context = module.getContext();
        for (std::size_t cell = 0; cell < used; ++cell)
        {
            module.getOrInsertFunction(baseInitializers[cell], llvm::FunctionType::get(llvm::Type::getVoidTy(context), false));
        }
        return used;
    }

    /// The base cells whose top-level code must run before the UDF: the stdlib, and each linked cell that defines a global
    /// variable that the UDF, or the top-level code of a needed cell, can reach. A cell can define functions that the UDF
    /// uses without the need to run its top-level code: e.g. the numpy cell holds the stdlib functions that numpy realized.
    std::vector<std::size_t> neededBaseCells(
        llvm::Module& module, const std::size_t linkedCells, const std::string& cellName, const std::string& entrySymbol) const
    {
        std::vector<bool> needed(linkedCells, false);
        needed[0] = true;
        std::set<const llvm::Value*> visited;
        std::vector<const llvm::Value*> open;
        const auto push = [&](const llvm::Value* value)
        {
            if (value != nullptr && visited.insert(value).second)
            {
                open.push_back(value);
            }
        };
        push(module.getFunction(cellName));
        push(module.getFunction(entrySymbol));
        while (!open.empty())
        {
            const auto* value = open.back();
            open.pop_back();
            if (const auto* function = llvm::dyn_cast<llvm::Function>(value))
            {
                for (const auto& instruction : llvm::instructions(*function))
                {
                    for (const auto& operand : instruction.operands())
                    {
                        if (llvm::isa<llvm::Constant>(operand.get()))
                        {
                            push(operand.get());
                        }
                    }
                }
            }
            else if (const auto* global = llvm::dyn_cast<llvm::GlobalVariable>(value))
            {
                if (!global->isConstant() && !global->isDeclaration())
                {
                    for (std::size_t cell = 1; cell < linkedCells; ++cell)
                    {
                        if (!needed[cell] && baseCellSymbols[cell].contains(global->getName().str()))
                        {
                            needed[cell] = true;
                            push(module.getFunction(baseInitializers[cell]));
                        }
                    }
                }
                if (global->hasInitializer())
                {
                    push(global->getInitializer());
                }
            }
            else if (const auto* constant = llvm::dyn_cast<llvm::Constant>(value))
            {
                for (const auto& operand : constant->operands())
                {
                    push(operand.get());
                }
            }
        }
        std::vector<std::size_t> cells;
        for (std::size_t cell = 0; cell < linkedCells; ++cell)
        {
            if (needed[cell])
            {
                cells.push_back(cell);
            }
        }
        return cells;
    }

    /// The initializer of a UDF: the top-level code of the stdlib, of the base cells that it needs, then of the UDF's cell.
    llvm::Function* createInitializer(llvm::Module& module, const std::vector<std::size_t>& baseCells, const std::string& cellName) const
    {
        std::vector<llvm::Function*> steps;
        for (const auto cell : baseCells)
        {
            steps.push_back(module.getFunction(baseInitializers[cell]));
        }
        steps.push_back(module.getFunction(cellName));
        if (std::ranges::contains(steps, nullptr))
        {
            throw std::runtime_error("The Codon module of the UDF lacks its initializers");
        }
        auto* stdlibMain = steps.front();
        auto& context = module.getContext();
        auto* initializer = llvm::Function::Create(
            llvm::FunctionType::get(llvm::Type::getVoidTy(context), false),
            llvm::GlobalValue::InternalLinkage,
            "nes_codon_udf_initializer",
            module);
        initializer->setPersonalityFn(stdlibMain->hasPersonalityFn() ? stdlibMain->getPersonalityFn() : nullptr);
        initializer->addFnAttr(llvm::Attribute::UWTable);
        llvm::IRBuilder<> builder(llvm::BasicBlock::Create(context, "entry", initializer));
        for (auto* step : steps)
        {
            builder.CreateCall(step->getFunctionType(), step, {});
        }
        builder.CreateRetVoid();
        return initializer;
    }

    std::unique_ptr<codon::Compiler, decltype(&deleteCodonCompiler)> compiler;
    /// The base state: the stdlib, and the modules that earlier UDFs imported (extendBase). Its LLVM module, the functions
    /// that run its top-level code, and its IR nodes, IR types, imports and type checker state.
    std::string baseBitcode;
    std::vector<std::string> baseInitializers;
    /// The functions and global variables that each base cell defines (the stdlib is the first cell).
    std::vector<std::set<std::string>> baseCellSymbols;
    std::set<codon::ir::id_t> baseNodes;
    std::set<std::string> baseTypes;
    std::set<std::string> baseImports;
    std::unique_ptr<CodonStateSnapshot> snapshot;
};
}

CodonCompilationResult compileWithCodon(
    const std::string& sourcePath,
    const std::string& generatedSource,
    const std::string& entrySymbol,
    const std::vector<std::string>& importPaths)
{
    codon::ir::setNativeOptimizationEnabled(false);
    /// Inline UDF bodies are part of the generated source, so it imports the compat stand-ins like UDF modules do.
    const auto source = isCompatPluginLoaded() ? std::string{compatImport} + "\n" + generatedSource : generatedSource;
    std::ofstream sourceFile{sourcePath, std::ios::binary | std::ios::trunc};
    if (!sourceFile)
    {
        throw std::runtime_error("Could not create Python UDF debug source file: " + sourcePath);
    }
    sourceFile.write(source.data(), static_cast<std::streamsize>(source.size()));
    if (!sourceFile)
    {
        throw std::runtime_error("Could not write Python UDF debug source file: " + sourcePath);
    }
    sourceFile.close();

    /// NOLINTNEXTLINE(concurrency-mt-unsafe)
    if (const char* fresh = std::getenv("NES_CODON_FRESH_COMPILER"); fresh == nullptr || std::string_view{fresh} != "1")
    {
        /// Created at the first UDF, after the plugins are loaded. The caller holds the Codon compiler mutex.
        static std::unique_ptr<PersistentCodonCompiler> persistent;
        if (!persistent)
        {
            persistent = std::make_unique<PersistentCodonCompiler>();
        }
        return persistent->compile(sourcePath, source, entrySymbol, importPaths);
    }

    auto filesystem = std::make_shared<PythonUdfFilesystem>("nes-worker", sourcePath, importPaths);

    const std::unique_ptr<codon::Compiler, decltype(&deleteCodonCompiler)> compilerOwner{
        new codon::Compiler( /// NOLINT(cppcoreguidelines-owning-memory)
            "nes-worker",
            codon::Compiler::Mode::RELEASE,
            std::vector<std::string>{},
            false,
            false,
            false,
            filesystem),
        &deleteCodonCompiler};
    auto& compiler = *compilerOwner;
    compiler.getLLVMVisitor()->setDebug(true);
    const auto parseStart = std::chrono::steady_clock::now();
    if (auto error = compiler.parseCode(sourcePath, source))
    {
        throw std::runtime_error("Could not parse Python UDF: " + llvm::toString(std::move(error)));
    }
    const auto compileStart = std::chrono::steady_clock::now();
    if (auto error = compiler.compile())
    {
        throw std::runtime_error("Could not compile Python UDF: " + llvm::toString(std::move(error)));
    }
    auto& llvmModule = *compiler.getLLVMVisitor()->getModule();
    finishCodonModule(llvmModule, findCodonInitializer(llvmModule), *compiler.getCache(), *filesystem, sourcePath, source, entrySymbol);
    const auto optimizeStart = std::chrono::steady_clock::now();
    auto optimizationOptions = codon::ir::LLVMOptimizationOptions::release();
    optimizationOptions.preserveDebugInfo = true;
    compiler.getLLVMVisitor()->optimizeLLVM(optimizationOptions);
    const auto optimizeEnd = std::chrono::steady_clock::now();

    const auto* module = compiler.getLLVMVisitor()->getModule();
    if (module == nullptr)
    {
        throw std::runtime_error("Codon did not create an LLVM module");
    }
    llvm::SmallVector<char, 0> bitcode;
    llvm::raw_svector_ostream bitcodeStream(bitcode);
    llvm::WriteBitcodeToFile(*module, bitcodeStream);
    return {
        .llvmBitcode = {bitcode.data(), bitcode.size()},
        .parseMilliseconds = elapsedMilliseconds(parseStart, compileStart),
        .compileMilliseconds = elapsedMilliseconds(compileStart, optimizeStart),
        .optimizeMilliseconds = elapsedMilliseconds(optimizeStart, optimizeEnd)};
}
}
