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

#include <HiPyUdfCompiler.hpp>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <sstream>
#include <string>
#include <string_view>
#include <vector>
#include <unistd.h>
#include <DataTypes/DataType.hpp>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <magic_enum/magic_enum.hpp>
#include <nlohmann/json.hpp>
#include <ErrorHandling.hpp>
#include <PythonUdfCompiler.hpp>
#include <UdfCompilerRegistry.hpp>
#include <UdfDescriptor.hpp>

namespace NES
{
namespace
{
std::string shellQuote(const std::string_view text)
{
    std::string result = "'";
    for (const char character : text)
    {
        result += character == '\'' ? std::string{"'\\''"} : std::string{character};
    }
    return result + "'";
}

std::string readFile(const std::filesystem::path& path)
{
    std::ifstream file(path, std::ios::binary);
    return {std::istreambuf_iterator<char>(file), std::istreambuf_iterator<char>()};
}

/// The last non-empty lines of Python's error output: the exception, and HiPy's explanation before it.
std::string lastLines(const std::string& text, const size_t count)
{
    std::vector<std::string> lines;
    std::istringstream stream(text);
    for (std::string line; std::getline(stream, line);)
    {
        if (!line.empty())
        {
            lines.push_back(line);
        }
    }
    const auto begin = lines.size() > count ? lines.end() - static_cast<std::ptrdiff_t>(count) : lines.begin();
    return fmt::format("{}", fmt::join(begin, lines.end(), " | "));
}

std::string environment(const char* name, const std::string_view fallback)
{
    /// NOLINTNEXTLINE(concurrency-mt-unsafe)
    const auto* const value = std::getenv(name);
    return value == nullptr || *value == '\0' ? std::string{fallback} : std::string{value};
}
}

CompiledInlineUdf HiPyUdfCompiler::createHiPy(UdfCompilerRegistryArguments arguments)
{
    /// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
    static std::atomic_uint64_t nextRegisteredUdfId = 0;
    const auto id = nextRegisteredUdfId.fetch_add(1);
    const auto& udf = arguments.udf;
    if (!udf.getBytesArguments().empty())
    {
        throw QueryCompilerError("HiPy UDF '{}': BYTES arguments are not supported by the HiPy compiler", udf.getName());
    }
    const auto hipyDir = environment("NES_HIPY_DIR", "");
    if (hipyDir.empty())
    {
        throw QueryCompilerError("HiPy UDF '{}': COMPILER HIPY needs NES_HIPY_DIR, the directory of a HiPy checkout", udf.getName());
    }
    const auto python = environment("NES_HIPY_PYTHON", "python3");

    std::vector<std::string> typeNames;
    for (const auto& type : udf.getArgTypes())
    {
        typeNames.emplace_back(magic_enum::enum_name(type.type));
    }
    const auto& source = udf.getSource();
    const nlohmann::json request = {
        {"entrypoint", source.has_value() ? std::string{} : udf.getEntrypoint()},
        {"body", source.has_value() ? source->body : std::string{}},
        {"parameterNames", source.has_value() ? source->parameterNames : std::vector<std::string>{}},
        {"argumentTypes", typeNames},
        {"returnType", std::string{magic_enum::enum_name(udf.getReturnType().type)}},
        {"importPaths", codonImportPaths(arguments.importPaths)},
        {"hipyDir", hipyDir},
    };

    const auto directory = std::filesystem::temp_directory_path() / fmt::format("nes-hipy-{}-{}", ::getpid(), id);
    std::filesystem::create_directories(directory);
    const auto requestPath = directory / "request.json";
    const auto outputPath = directory / "udf.json";
    const auto errorsPath = directory / "errors.txt";
    {
        std::ofstream file(requestPath);
        file << request.dump();
    }
    const auto start = std::chrono::steady_clock::now();
    const auto command = fmt::format(
        "{} {} {} {} 2> {}",
        shellQuote(python),
        shellQuote(NES_HIPY_COMPILE_SCRIPT),
        shellQuote(requestPath.string()),
        shellQuote(outputPath.string()),
        shellQuote(errorsPath.string()));
    /// NOLINTNEXTLINE(concurrency-mt-unsafe,cert-env33-c)
    const auto status = std::system(command.c_str());
    const auto errorOutput = readFile(errorsPath);
    if (status != 0 || !std::filesystem::exists(outputPath))
    {
        std::filesystem::remove_all(directory);
        throw QueryCompilerError("HiPy could not compile UDF '{}': {}", udf.getName(), lastLines(errorOutput, 6));
    }
    auto output = readFile(outputPath);
    std::filesystem::remove_all(directory);
    const auto entry = nlohmann::json::parse(output).at("entry").get<std::string>();
    fmt::print(
        stderr,
        "[PythonUDF] Compiled '{}' with HiPy in {:.3f} ms\n",
        udf.getName(),
        std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - start).count());
    return CompiledInlineUdf{.symbol = entry, .pipelineBitcode = std::move(output), .interpreterBitcode = {}};
}

}
