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
#include <expected>
#include <optional>
#include <string>
#include <type_traits>
#include <fmt/base.h>
#include <fmt/format.h>
#include <ErrorHandling.hpp>

namespace NES
{

/// This implements a small wrapper around the `std::expected` type. We mainly use `std::strings` as the
/// return type, thus we do not need to repeat it everywhere. We also add a few convinent methods to use
/// `fmt` directly when creating an `unexpected`.
/// The implementation als adds conversions from std::optional via `toExpected(std::optional<T>, error_message...) -> Expected<T>`
/// and throwing NES exceptions via `valueOrThrow(Expected<T>, ExceptionType)`

template <typename T>
using Expected = std::expected<T, std::string>;

namespace detail
{
template <class>
struct IsExpected : std::false_type
{
};

template <class T>
struct IsExpected<std::expected<T, std::string>> : std::true_type
{
};
}

/// Test if T is an Expected type
template <class T>
static constexpr bool IsExpected_v = NES::detail::IsExpected<T>::value;

template <class T>
concept ExpectedT = IsExpected_v<T>;

template <typename... Args>
auto unexpected(fmt::format_string<Args...> fmtMsg, Args&&... args)
{
    return std::unexpected<std::string>(fmt::format(fmtMsg, std::forward<Args>(args)...));
}

/// Converts an optional<T> to an Expected<T>.
/// Flattens optional<Expected<T>> to just Expected<T>
template <typename T, typename... Args>
auto toExpected(std::optional<T> opt, fmt::format_string<Args...> fmtMsg, Args&&... args)
{
    if (!opt)
    {
        return Expected<T>(unexpected(fmtMsg, std::forward<Args>(args)...));
    }
    if constexpr (ExpectedT<T>)
    {
        return static_cast<Expected<T>>(*std::optional<T>(opt));
    }
    else
    {
        return Expected<T>(*std::optional<T>(opt));
    }
}


/// Usage: intput: Expected<uint32_t>
///        auto inputValue = valueOrThrow(input, InvalidConfiguration);
auto valueOrThrow(ExpectedT auto&& expected, Exception (*fn)(std::string))
{
    if (expected.has_value())
    {
        return std::forward<decltype(expected)>(expected).value();
    }
    throw fn(std::forward<decltype(expected)>(expected).error());
}
}
