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

#include <SessionCommand.hpp>

#include <cstdint>
#include <filesystem>
#include <fstream>
#include <ios>
#include <optional>
#include <sstream>
#include <string>
#include <string_view>
#include <utility>

#include <Util/Strings.hpp>
#include <ErrorHandling.hpp>

namespace NES::ConnTest
{

std::optional<std::string> slurpFile(const std::filesystem::path& path)
{
    const std::ifstream input(path, std::ios::in | std::ios::binary);
    if (not input.good())
    {
        return std::nullopt;
    }
    std::ostringstream buffer;
    buffer << input.rdbuf();
    return std::move(buffer).str();
}

std::string_view verbOf(const std::string_view line)
{
    const auto end = line.find(' ');
    return line.substr(0, end);
}

std::optional<std::uint64_t> argOf(const std::string_view line)
{
    const auto separator = line.find(' ');
    if (separator == std::string_view::npos)
    {
        return std::nullopt;
    }
    const auto argument = line.substr(separator + 1);
    try
    {
        return NES::from_chars_with_exception<std::uint64_t>(argument);
    }
    catch (const Exception&)
    {
        return std::nullopt;
    }
}

std::optional<std::pair<std::uint64_t, std::uint64_t>> twoArgsOf(const std::string_view line)
{
    const auto firstSeparator = line.find(' ');
    if (firstSeparator == std::string_view::npos)
    {
        return std::nullopt;
    }
    const auto rest = line.substr(firstSeparator + 1);
    const auto secondSeparator = rest.find(' ');
    if (secondSeparator == std::string_view::npos)
    {
        return std::nullopt;
    }
    try
    {
        return std::pair{
            NES::from_chars_with_exception<std::uint64_t>(rest.substr(0, secondSeparator)),
            NES::from_chars_with_exception<std::uint64_t>(rest.substr(secondSeparator + 1))};
    }
    catch (const Exception&)
    {
        return std::nullopt;
    }
}

}
