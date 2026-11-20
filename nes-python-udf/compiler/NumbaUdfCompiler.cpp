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

#include <NumbaUdfCompiler.hpp>

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
#include <ErrorHandling.hpp>
#include <PythonUdfCompiler.hpp>
#include <UdfCompilerRegistry.hpp>
#include <UdfDescriptor.hpp>

namespace NES
{
namespace
{
std::string jsonString(const std::string_view text)
{
    std::string result = "\"";
    for (const char character : text)
    {
        switch (character)
        {
            case '"':
                result += "\\\"";
                break;
            case '\\':
                result += "\\\\";
                break;
            case '\n':
                result += "\\n";
                break;
            case '\t':
                result += "\\t";
                break;
            case '\r':
                result += "\\r";
                break;
            default:
                if (static_cast<unsigned char>(character) < 0x20)
                {
                    result += fmt::format("\\u{:04x}", static_cast<unsigned char>(character));
                }
                else
                {
                    result += character;
                }
        }
    }
    return result + "\"";
}

std::string jsonList(const std::vector<std::string>& values)
{
    std::vector<std::string> quoted;
    quoted.reserve(values.size());
    for (const auto& value : values)
    {
        quoted.push_back(jsonString(value));
    }
    return fmt::format("[{}]", fmt::join(quoted, ", "));
}

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

/// The last lines of Python's error output: the exception, and Numba's explanation before it.
std::string lastLines(const std::string& text, const size_t count)
{
    std::vector<std::string> lines;
    std::istringstream stream(text);
    for (std::string line; std::getline(stream, line);)
    {
        if (!line.empty() && line.find("Warning") == std::string::npos)
        {
            lines.push_back(line);
        }
    }
    const auto begin = lines.size() > count ? lines.end() - static_cast<std::ptrdiff_t>(count) : lines.begin();
    return fmt::format("{}", fmt::join(begin, lines.end(), " | "));
}
}

CompiledInlineUdf NumbaUdfCompiler::createNumba(UdfCompilerRegistryArguments arguments)
{
    /// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
    static std::atomic_uint64_t nextRegisteredUdfId = 0;
    const auto id = nextRegisteredUdfId.fetch_add(1);
    const auto& udf = arguments.udf;
    const auto symbol = fmt::format("__nes_numba_udf_registered_{}", id);
    if (!udf.getBytesArguments().empty())
    {
        throw QueryCompilerError("Numba UDF '{}': BYTES arguments are not supported by the Numba compiler", udf.getName());
    }
    /// As for Codon (CodonUdfCompiler): the entry takes every argument as nullable, the function receives the ones declared
    /// nullable as Optional, and the result is nullable.
    auto argumentTypes = udf.getArgTypes();
    std::vector<std::string> typeNames;
    std::vector<std::string> bodyNullable;
    for (auto& type : argumentTypes)
    {
        typeNames.emplace_back(magic_enum::enum_name(type.type));
        bodyNullable.emplace_back(type.nullable ? "true" : "false");
        type.nullable = true;
    }
    auto returnType = udf.getReturnType();
    returnType.nullable = true;

    /// NOLINTNEXTLINE(concurrency-mt-unsafe)
    const auto* const python = std::getenv("NES_NUMBA_PYTHON");
    if (python == nullptr || *python == '\0')
    {
        throw QueryCompilerError(
            "Numba UDF '{}': COMPILER NUMBA needs NES_NUMBA_PYTHON, the Python executable of an environment with Numba", udf.getName());
    }
    const auto directory = std::filesystem::temp_directory_path() / fmt::format("nes-numba-{}-{}", ::getpid(), id);
    std::filesystem::create_directories(directory);
    const auto request = directory / "request.json";
    const auto output = directory / "udf.bc";
    const auto errors = directory / "errors.txt";
    const auto& source = udf.getSource();
    {
        std::ofstream file(request);
        file << fmt::format(
            R"({{"symbol": {}, "entrypoint": {}, "body": {}, "parameterNames": {}, "argumentTypes": {}, "bodyNullable": [{}], )"
            R"("returnType": {}, "importPaths": {}}})",
            jsonString(symbol),
            jsonString(source.has_value() ? std::string{} : udf.getEntrypoint()),
            jsonString(source.has_value() ? source->body : std::string{}),
            jsonList(source.has_value() ? source->parameterNames : std::vector<std::string>{}),
            jsonList(typeNames),
            fmt::join(bodyNullable, ", "),
            jsonString(magic_enum::enum_name(returnType.type)),
            jsonList(codonImportPaths(arguments.importPaths)));
    }
    const auto start = std::chrono::steady_clock::now();
    const auto command = fmt::format(
        "{} {} {} {} 2> {}",
        shellQuote(python),
        shellQuote(NES_NUMBA_COMPILE_SCRIPT),
        shellQuote(request.string()),
        shellQuote(output.string()),
        shellQuote(errors.string()));
    /// NOLINTNEXTLINE(concurrency-mt-unsafe,cert-env33-c)
    const auto status = std::system(command.c_str());
    const auto errorOutput = readFile(errors);
    if (status != 0 || !std::filesystem::exists(output))
    {
        std::filesystem::remove_all(directory);
        throw QueryCompilerError("Numba could not compile UDF '{}': {}", udf.getName(), lastLines(errorOutput, 8));
    }
    const auto bitcode = readFile(output);
    std::filesystem::remove_all(directory);
    auto compiled = finishNumbaUdf(bitcode, symbol, argumentTypes, returnType);
    fmt::print(
        stderr,
        "[PythonUDF] Compiled '{}' with Numba in {:.3f} ms ({})\n",
        symbol,
        std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - start).count(),
        lastLines(errorOutput, 1));
    return compiled;
}

}
