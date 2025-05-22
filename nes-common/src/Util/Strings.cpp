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
#include <concepts>
#include <cstddef>
#include <optional>
#include <ranges>
#include <sstream>
#include <string>
#include <string_view>
#include <unordered_map>

#include <Util/Ranges.hpp>
#include <Util/Strings.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>

namespace NES::Util
{
template <>
std::optional<float> from_chars<float>(const std::string_view input)
{
    const std::string str(trimWhiteSpaces(input));
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
std::optional<bool> from_chars<bool>(const std::string_view input)
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

std::string formatFloat(std::floating_point auto value)
{
    std::string formatted = fmt::format("{:.6f}", value);
    const size_t decimalPos = formatted.find('.');
    if (decimalPos == std::string_view::npos)
    {
        return formatted;
    }

    const size_t lastNonZero = formatted.find_last_not_of('0');
    if (lastNonZero == decimalPos)
    {
        return formatted.substr(0, decimalPos + 2);
    }

    return formatted.substr(0, lastNonZero + 1);
}

/// explicit instantiations
template std::string formatFloat(float);
template std::string formatFloat(double);

template <>
std::optional<double> from_chars<double>(const std::string_view input)
{
    const std::string str(trimWhiteSpaces(input));
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
std::optional<std::string> from_chars<std::string>(const std::string_view input)
{
    return std::string(input);
}

template <>
std::optional<std::string_view> from_chars<std::string_view>(std::string_view input)
{
    return input;
}

std::string replaceAll(std::string_view origin, const std::string_view search, const std::string_view replace)
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
USED_IN_DEBUG constexpr bool isAsciiString(std::string_view input)
{
    return std::ranges::all_of(input, [](const auto& character) { return isascii(character) != 0; });
}
}

void toUpperCaseInplace(std::string& modified)
{
    PRECONDITION(isAsciiString(modified), "Support for non-ascii character not implemented");
    for (char& character : modified)
    {
        character = static_cast<char>(::toupper(character));
    }
}

void toLowerCaseInplace(std::string& modified)
{
    PRECONDITION(isAsciiString(modified), "Support for non-ascii character not implemented");
    for (char& character : modified)
    {
        character = static_cast<char>(::tolower(character));
    }
}

std::string escapeSpecialCharacters(const std::string_view input)
{
    const std::unordered_map<char, std::string> specialCharacters = {
        {'\a', "\\a"},
        {'\b', "\\b"},
        {'\f', "\\f"},
        {'\n', "\\n"},
        {'\r', "\\r"},
        {'\t', "\\t"},
        {'\v', "\\v"},
    };
    std::string escapedString;
    escapedString.reserve(input.size());
    for (const auto value : input)
    {
        if (auto it = specialCharacters.find(value); it != specialCharacters.end())
        {
            escapedString += it->second;
        }
        else
        {
            escapedString += value;
        }
    }

    return escapedString;
}

std::string toUpperCase(std::string_view input)
{
    PRECONDITION(isAsciiString(input), "Support for non-ascii character not implemented");
    return input | std::views::transform(::toupper) | std::ranges::to<std::string>();
}

std::string toLowerCase(std::string_view input)
{
    PRECONDITION(isAsciiString(input), "Support for non-ascii character not implemented");
    return input | std::views::transform(::tolower) | std::ranges::to<std::string>();
}

std::string_view trimWhiteSpaces(const std::string_view input)
{
    const auto start = input.find_first_not_of(" \t\n\r");
    const auto end = input.find_last_not_of(" \t\n\r");
    return (start == std::string_view::npos) ? "" : input.substr(start, end - start + 1);
}

std::string_view trimCharacters(const std::string_view input, const char c)
{
    const auto start = input.find_first_not_of(c);
    const auto end = input.find_last_not_of(c);
    return (start == std::string_view::npos) ? "" : input.substr(start, end - start + 1);
}

std::string_view trimCharsRight(std::string_view input, char character)
{
    const std::size_t lastNotC = input.find_last_not_of(character);
    if (lastNotC == std::string_view::npos)
    {
        return std::string_view{};
    }
    input.remove_suffix(input.size() - lastNotC - 1);
    return input;
}

void removeDoubleSpaces(std::string& input)
{
    const auto newEnd = std::ranges::unique(input, [](const char lhs, const char rhs) { return (lhs == rhs) && (lhs == ' '); }).begin();
    input.erase(newEnd, input.end());
}

}
