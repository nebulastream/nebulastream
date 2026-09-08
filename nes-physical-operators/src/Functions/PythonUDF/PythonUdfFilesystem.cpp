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

#include <chrono>
#include <filesystem>
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
    PythonUdfFilesystem(std::string argv0, std::string module0, const bool allowExternal)
        : ResourceFilesystem(std::move(argv0), std::move(module0), allowExternal)
    {
    }

    std::vector<std::string> read_lines(const path_t& path) const override
    {
        if (const auto source = pluginModuleSource(path); !source.empty())
        {
            return splitIntoLines(source);
        }
        return ResourceFilesystem::read_lines(path);
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
};

double elapsedMilliseconds(const std::chrono::steady_clock::time_point start, const std::chrono::steady_clock::time_point end)
{
    return std::chrono::duration<double, std::milli>{end - start}.count();
}
}

CodonCompilationResult
compileWithCodon(const std::string& sourcePath, const std::string& source, const std::vector<std::string>& importPaths)
{
    codon::ir::setNativeOptimizationEnabled(false);
    auto filesystem = std::make_shared<PythonUdfFilesystem>("nes-worker", sourcePath, !importPaths.empty());
    for (const auto& importPath : importPaths)
    {
        filesystem->add_search_path(importPath);
    }

    codon::Compiler compiler("nes-worker", codon::Compiler::Mode::RELEASE, std::vector<std::string>{}, false, false, false, filesystem);
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
    const auto optimizeStart = std::chrono::steady_clock::now();
    compiler.getLLVMVisitor()->optimizeLLVM();
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
