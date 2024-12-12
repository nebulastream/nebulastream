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

#include <algorithm>
#include <cctype>
#include <iterator>
#include <optional>
#include <ranges>
#include <sstream>
#include <string>
#include <string_view>
#include <Util/Ranges.hpp>
#include <Util/Strings.hpp>
#include <ErrorHandling.hpp>

namespace NES::Util
{
template <>
std::optional<float> from_chars<float>(std::string_view input)
{
    std::string const str(trimWhiteSpaces(input));
    try
    {
        return std::stof(str);
    }
    catch (...)
    {
        return {};
    }
}

template <>
std::optional<bool> from_chars<bool>(std::string_view input)
{
    auto trimmed = trimWhiteSpaces(input);
    if (toLowerCase(trimmed) == "true" || trimmed == "1")
    {
        return true;
    }
    if (toLowerCase(trimmed) == "false" || trimmed == "0")
    {
        return false;
    }
    return {};
}

template <>
std::optional<double> from_chars<double>(std::string_view input)
{
    std::string const str(trimWhiteSpaces(input));
    try
    {
        return std::stod(str);
    }
    catch (...)
    {
        return {};
    }
}
template <>
std::optional<std::string> from_chars<std::string>(std::string_view input)
{
    return std::string(input);
}
template <>
std::optional<std::string_view> from_chars<std::string_view>(std::string_view input)
{
    return input;
}

std::string replaceAll(std::string_view origin, std::string_view search, std::string_view replace)
{
    if (search.empty())
    {
        return std::string(origin);
    }

    std::stringstream stringBuilder;
    for (auto index = origin.find(search); index != std::string_view::npos;
         origin = origin.substr(index + search.length()), index = origin.find(search))
    {
        stringBuilder << origin.substr(0, index) << replace;
    }

    stringBuilder << origin;
    return stringBuilder.str();
}

std::string replaceFirst(std::string_view origin, const std::string_view search, const std::string_view replace)
{
    if (search.empty())
    {
        return std::string(origin);
    }

    if (auto index = origin.find(search); index != std::string::npos)
    {
        std::stringstream stringBuilder;
        stringBuilder << origin.substr(0, index) << replace << origin.substr(index + search.size());
        return stringBuilder.str();
    }
    return std::string(origin);
}

namespace
{
constexpr void verifyAsciiString(std::string_view input)
{
    PRECONDITION(
        std::ranges::all_of(input, [](const auto& character) { return isascii(character) != 0; }),
        "Support for non ascii character is not implemented");
}
}

void toUpperCaseInplace(std::string& modified)
{
    verifyAsciiString(modified);
    for (char& character : modified)
    {
        character = static_cast<char>(::toupper(character));
    }
}

void toLowerCaseInplace(std::string& modified)
{
    verifyAsciiString(modified);
    for (char& character : modified)
    {
        character = static_cast<char>(::tolower(character));
    }
}

std::string toUpperCase(std::string_view input)
{
    verifyAsciiString(input);
    return input | std::views::transform(::toupper) | ranges::to<std::string>();
}

std::string toLowerCase(std::string_view input)
{
    verifyAsciiString(input);
    return input | std::views::transform(::tolower) | ranges::to<std::string>();
}


std::string_view trimWhiteSpaces(const std::string_view input)
{
    const auto start = input.find_first_not_of(" \t\n\r");
    const auto end = input.find_last_not_of(" \t\n\r");
    return (start == std::string_view::npos) ? "" : input.substr(start, end - start + 1);
}

}
