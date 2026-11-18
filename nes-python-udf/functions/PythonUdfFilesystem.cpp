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
#include <fmt/format.h>
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
#include <llvm/Support/Error.h>
#include <llvm/Support/MemoryBuffer.h>
#include <llvm/Support/raw_ostream.h>

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
/// After each UDF, the compiler returns to the state after the stdlib (JITState::undo), and the IR of the UDF is removed:
/// each UDF imports its modules again, so a changed UDF file takes effect, and the stdlib functions that a UDF realizes
/// are in its own module. The stdlib modules that the stdlib itself does not import, e.g. numpy, are also parsed again for
/// each UDF that imports them.
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
        stdlibInitializer = initializer->getName().str();
        stdlibBitcode = writeBitcode(*stdlib.module);
        for (const auto* node : *compiler->getModule())
        {
            stdlibNodes.insert(node->getId());
        }
        for (auto type = compiler->getModule()->types_begin(); type != compiler->getModule()->types_end(); ++type)
        {
            stdlibTypes.insert((*type)->getName());
        }
        snapshot = std::make_unique<CodonStateSnapshot>(*cache);
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
                if (!stdlibNodes.contains(node->getId()))
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
                if (!stdlibTypes.contains((*type)->getName()))
                {
                    addedTypes.push_back(*type);
                }
            }
            for (auto* type : addedTypes)
            {
                irModule->remove(type);
            }
        };
        try
        {
            auto result = compileCell(*filesystem, sourcePath, source, entrySymbol);
            restore();
            return result;
        }
        catch (...)
        {
            restore();
            throw;
        }
    }

private:
    static constexpr std::string_view stdlibFile = "<nes-codon-stdlib>";

    CodonCompilationResult compileCell(
        const PythonUdfFilesystem& filesystem, const std::string& sourcePath, const std::string& source, const std::string& entrySymbol)
    {
        auto* cache = compiler->getCache();
        const auto parseStart = std::chrono::steady_clock::now();
        codon::ir::Func* cell = nullptr;
        try
        {
            auto parsed = codon::ast::parseCode(cache, sourcePath, source);
            if (!parsed)
            {
                throw std::runtime_error("Could not parse Python UDF: " + llvm::toString(parsed.takeError()));
            }
            const auto& context = cache->imports[MAIN_IMPORT].ctx;
            if (auto error = codon::ast::ScopingVisitor::apply(cache, *parsed, &context->globalShadows))
            {
                throw std::runtime_error("Could not parse Python UDF: " + llvm::toString(std::move(error)));
            }
            auto* typechecked = codon::ast::TypecheckVisitor::apply(context, *parsed, sourcePath);
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

        const auto compileStart = std::chrono::steady_clock::now();
        compiler->getPassManager()->run(compiler->getModule());
        const OwnedLlvmModule owned{compiler->getLLVMVisitor()->takeModule(compiler->getModule())};
        auto& module = owned.module;
        auto stdlib = llvm::parseBitcodeFile(llvm::MemoryBufferRef{stdlibBitcode, "nes-codon-stdlib"}, *owned.context);
        if (!stdlib)
        {
            throw std::runtime_error("Could not read the Codon stdlib module: " + llvm::toString(stdlib.takeError()));
        }
        if (llvm::Linker::linkModules(*module, std::move(*stdlib)))
        {
            throw std::runtime_error("Could not link the Codon stdlib module");
        }
        allocateVtablesPersistently(*module);
        auto* initializer = createInitializer(*module, codon::ir::LLVMVisitor::getNameForFunction(cell));
        finishCodonModule(*module, initializer, *cache, filesystem, sourcePath, source, entrySymbol);

        const auto optimizeStart = std::chrono::steady_clock::now();
        auto optimizationOptions = codon::ir::LLVMOptimizationOptions::release();
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

    /// The initializer of a UDF: the top-level code of the stdlib, then that of the UDF's cell.
    llvm::Function* createInitializer(llvm::Module& module, const std::string& cellName) const
    {
        auto* stdlibMain = module.getFunction(stdlibInitializer);
        auto* cellFunction = module.getFunction(cellName);
        if (stdlibMain == nullptr || cellFunction == nullptr)
        {
            throw std::runtime_error("The Codon module of the UDF lacks its initializers");
        }
        auto& context = module.getContext();
        auto* initializer = llvm::Function::Create(
            llvm::FunctionType::get(llvm::Type::getVoidTy(context), false),
            llvm::GlobalValue::InternalLinkage,
            "nes_codon_udf_initializer",
            module);
        initializer->setPersonalityFn(stdlibMain->hasPersonalityFn() ? stdlibMain->getPersonalityFn() : nullptr);
        initializer->addFnAttr(llvm::Attribute::UWTable);
        llvm::IRBuilder<> builder(llvm::BasicBlock::Create(context, "entry", initializer));
        builder.CreateCall(stdlibMain->getFunctionType(), stdlibMain, {});
        builder.CreateCall(cellFunction->getFunctionType(), cellFunction, {});
        builder.CreateRetVoid();
        return initializer;
    }

    std::unique_ptr<codon::Compiler, decltype(&deleteCodonCompiler)> compiler;
    std::string stdlibBitcode;
    std::string stdlibInitializer;
    /// The IR nodes and types, and the type checker state after the stdlib.
    std::set<codon::ir::id_t> stdlibNodes;
    std::set<std::string> stdlibTypes;
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
