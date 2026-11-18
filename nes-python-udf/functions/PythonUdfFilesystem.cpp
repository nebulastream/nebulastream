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
#include <codon/parser/ast/node.h>
#include <codon/parser/ast/stmt.h>
#include <codon/parser/cache.h>
#include <codon/parser/common.h>
#include <codon/parser/peg/peg.h>
#include <codon/parser/visitors/visitor.h>
#include <fmt/format.h>
#include <llvm/ADT/SmallVector.h>
#include <llvm/Bitcode/BitcodeWriter.h>
#include <llvm/IR/DebugInfoMetadata.h>
#include <llvm/IR/GlobalVariable.h>
#include <llvm/IR/IRBuilder.h>
#include <llvm/IR/InstIterator.h>
#include <llvm/IR/Instructions.h>
#include <llvm/IR/Module.h>
#include <llvm/Support/Error.h>
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
void addModuleInitializationPrologue(llvm::Module& module, const std::string& symbol)
{
    auto* proxyMain = module.getFunction("codon.proxy_main");
    auto* entryPoint = module.getFunction(symbol);
    if (proxyMain == nullptr || proxyMain->isDeclaration() || entryPoint == nullptr || entryPoint->isDeclaration())
    {
        return;
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
    if (initializer == nullptr)
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
    const auto functions = compiledFunctions(*compiler.getLLVMVisitor()->getModule());
    rejectFunctionLocalImports(*compiler.getCache(), functions, sourcePath, source);
    for (const auto& [name, module] : compiler.getCache()->imports)
    {
        if (!module.filename.empty() && module.filename != sourcePath && filesystem->isUdfModule(module.filename))
        {
            rejectFunctionLocalImports(*compiler.getCache(), functions, module.filename, readFile(module.filename));
        }
    }
    addModuleInitializationPrologue(*compiler.getLLVMVisitor()->getModule(), entrySymbol);
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
