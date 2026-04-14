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

#include <string>
#include <string_view>
#include <type_traits>
#include <Util/Logger/Formatter.hpp>
#include <Util/Strings.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

/// A string type that is always stored in uppercase.
/// Implicitly convertible to std::string / std::string_view for read access.
class UppercaseString
{
public:
    /// Compile-time validated uppercase string literal.
    /// Enables implicit conversion from uppercase literals while rejecting lowercase at compile time.
    struct UpperLiteral
    {
        std::string_view view;

        template <size_t N>
        constexpr UpperLiteral(const char (&s)[N]) : view(s, N - 1)
        {
            for (size_t i = 0; i < N - 1; ++i)
            {
                if (s[i] >= 'a' && s[i] <= 'z')
                {
                    /// In constexpr/consteval context this makes the expression ill-formed (compile error).
                    /// At runtime this is a hard assertion failure.
                    INVARIANT(false, "Literal must be uppercase");
                }
            }
        }
    };

    UppercaseString() = default;

    template <typename T>
    requires(std::is_same_v<std::decay_t<T>, std::string>)
    explicit UppercaseString(T&& input) : value(toUpperCase(std::forward<T>(input)))
    {
    }

    /// Implicit conversion from compile-time validated uppercase string literals.
    /// Enables: map.find("FILE"), UppercaseString s = "FILE"
    /// Compile error if the literal contains lowercase characters.
    UppercaseString(UpperLiteral lit) : value(lit.view) { }

    UppercaseString& operator=(std::string newValue)
    {
        value = toUpperCase(newValue);
        return *this;
    }

    [[nodiscard]] const std::string& getString() const noexcept { return value; }

    [[nodiscard]] operator const std::string&() const noexcept { return value; }

    [[nodiscard]] operator std::string_view() const noexcept { return value; }

    [[nodiscard]] const char* c_str() const noexcept { return value.c_str(); }

    [[nodiscard]] bool empty() const noexcept { return value.empty(); }

    [[nodiscard]] size_t size() const noexcept { return value.size(); }

    friend bool operator==(const UppercaseString&, const UppercaseString&) = default;
    friend auto operator<=>(const UppercaseString&, const UppercaseString&) = default;

    friend std::ostream& operator<<(std::ostream& os, const UppercaseString& s) { return os << s.value; }

private:
    std::string value;
};

}

FMT_OSTREAM(NES::UppercaseString);

template <>
struct std::hash<NES::UppercaseString>
{
    using is_transparent = void;

    size_t operator()(const NES::UppercaseString& s) const noexcept { return std::hash<std::string_view>{}(s.getString()); }

    size_t operator()(NES::UppercaseString::UpperLiteral lit) const noexcept { return std::hash<std::string_view>{}(lit.view); }
};

template <>
struct std::equal_to<NES::UppercaseString>
{
    using is_transparent = void;

    bool operator()(const NES::UppercaseString& a, const NES::UppercaseString& b) const noexcept { return a.getString() == b.getString(); }

    bool operator()(const NES::UppercaseString& a, NES::UppercaseString::UpperLiteral b) const noexcept
    {
        return std::string_view(a.getString()) == b.view;
    }

    bool operator()(NES::UppercaseString::UpperLiteral a, const NES::UppercaseString& b) const noexcept
    {
        return a.view == std::string_view(b.getString());
    }
};
