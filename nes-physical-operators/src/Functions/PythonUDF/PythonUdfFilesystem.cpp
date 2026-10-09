/*
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

#include <Functions/PythonUDF/PythonUdfFilesystem.hpp>

#include <algorithm>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>
#include <NESCodonPlugin/Registry.hpp>
#include <codon/cir/llvm/optimize.h>
#include <codon/compiler/compiler.h>
#include <codon/parser/common.h>
#include <llvm/ADT/SmallVector.h>
#include <llvm/Bitcode/BitcodeWriter.h>
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

/// nes-codon-compat-plugin provides stand-ins for Python builtins Codon lacks (bytes, bytearray). While it is loaded,
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
    PythonUdfFilesystem(std::string argv0, std::string module0, const std::vector<std::string>& importPaths)
        : ResourceFilesystem(std::move(argv0), std::move(module0), !importPaths.empty())
    {
        for (const auto& importPath : importPaths)
        {
            add_search_path(importPath);
            udfDirectories.push_back(std::filesystem::absolute(importPath).lexically_normal());
        }
    }

    std::vector<std::string> read_lines(const path_t& path) const override
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

    bool exists(const path_t& path) const override { return !pluginModuleSource(path).empty() || ResourceFilesystem::exists(path); }

    path_t canonical(const path_t& path) const override
    {
        if (!pluginModuleSource(path).empty())
        {
            return path.lexically_normal();
        }
        return ResourceFilesystem::canonical(path);
    }

private:
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

    std::vector<std::filesystem::path> udfDirectories;
};

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

    codon::Compiler compiler("nes-worker", codon::Compiler::Mode::RELEASE, std::vector<std::string>{}, false, false, false, filesystem);
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
