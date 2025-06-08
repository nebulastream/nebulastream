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
#pragma once

#include <charconv>
#include <concepts>
#include <optional>
#include <ranges>
#include <string>
#include <string_view>
#include <system_error>
#include <vector>
#include <Util/Expected.hpp>
#include <magic_enum/magic_enum.hpp>

namespace NES::Util
{

/// Removes leading and trailing whitespaces from the input string_view.
/// This does not modify the underlying data!
[[nodiscard]] std::string_view trimWhiteSpaces(std::string_view input);
[[nodiscard]] std::string_view trimCharsRight(std::string_view input, char c);

/// Removes leading and trailing chars from the input string_view.
/// This does not modify the underlying data!
[[nodiscard]] std::string_view trimCharacters(std::string_view input, char c);

/// Removes all double spaces from the input string.
void removeDoubleSpaces(std::string& input);


/// From chars attempts to extract a T from string_view after removing surrounding whitespaces.
/// Note: Implementation of T = string or string_view will return the string without performing any trimming.
template <typename T>
Expected<T> from_chars(std::string_view input) = delete;

/// Default implementation falls back to std::from_chars if it is availble for T
template <typename T>
Expected<T> from_chars(std::string_view input)
requires(requires(T value) { std::from_chars<T>(input.data(), input.data() + input.size(), value); })
{
    auto trimmed = trimWhiteSpaces(input);
    T value;
    if (auto result = std::from_chars<T>(trimmed.data(), trimmed.data() + trimmed.size(), value); result.ec != std::errc())
    {
        return unexpected("Could not convert '{}' to {}: {}", input, typeid(T).name(), magic_enum::enum_name(result.ec));
    }
    return value;
}

template <>
Expected<float> from_chars(std::string_view input);
template <>
Expected<double> from_chars(std::string_view input);
template <>
Expected<std::string> from_chars(std::string_view input);
template <>
Expected<std::string_view> from_chars(std::string_view input);
template <>
Expected<bool> from_chars(std::string_view input);

/// Formats floating points similiar to flink:
/// We preserve at least 1 digit and at most 6 digits after the decimal point
/// 0.0 -> 0.0
/// 1 -> 1.0
/// 1.2342512342135 -> 1.234251
/// 1.230000 -> 1.23
std::string formatFloat(std::floating_point auto value);


/// Replaces all occurrences of `search` within `origin` with `replace`. This function will allocate a new string and not
/// modify the existing string
[[nodiscard]] std::string replaceAll(std::string_view origin, std::string_view search, std::string_view replace);

/// Replaces only the first occurrences of `search` within `origin` with `replace`. This function will allocate a new string and not
/// modify the existing string
[[nodiscard]] std::string replaceFirst(std::string_view origin, std::string_view search, std::string_view replace);


/// Upper and Lower utility methods:
/// These functions only support ASCII strings!
[[nodiscard]] std::string toUpperCase(std::string_view input);
[[nodiscard]] std::string toLowerCase(std::string_view input);

/// Modify string objects in place without requiring further allocations
void toUpperCaseInplace(std::string& modified);
void toLowerCaseInplace(std::string& modified);

/// escape characters such as '\n', e.g., for logging
std::string escapeSpecialCharacters(std::string_view input);

/// splits a string given a delimiter into multiple substrings stored in a T vector
/// the delimiter is allowed to be a string rather than a char only.
template <typename T>
std::vector<T> splitWithStringDelimiter(std::string_view inputString, std::string_view delim)
{
    return std::views::split(inputString, delim) | std::views::filter([](const auto& split) { return !split.empty(); })
        | std::views::transform([](const auto& split) { return from_chars<T>(std::string_view(split)); })
        | std::views::filter([](auto optional) { return optional.has_value(); })
        | std::views::transform([](auto optional) { return *optional; }) | std::ranges::to<std::vector>();
}

}
