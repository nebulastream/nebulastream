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
#include <algorithm>
#include <cctype>
#include <initializer_list>
#include <ranges>
#include <string>
#include <string_view>
#include <vector>


class Identifier
{
    std::string value;
    bool caseSensitive{};

public:
    constexpr explicit Identifier() = default;
    constexpr explicit Identifier(const std::string_view value) : value(value) { }
    constexpr explicit Identifier(const std::string_view value, const bool caseSensitive) : value(value), caseSensitive(caseSensitive) { }
    constexpr Identifier(const Identifier& other) = default;
    constexpr Identifier(Identifier&& other) noexcept = default;
    constexpr Identifier& operator=(const Identifier& other) = default;
    constexpr Identifier& operator=(Identifier&& other) noexcept = default;
    constexpr ~Identifier() = default;
    [[nodiscard]] constexpr std::string getRawValue() const { return value; }
    [[nodiscard]] constexpr bool isCaseSensitive() const { return caseSensitive; }

    [[nodiscard]] constexpr std::string toString() const
    {
        std::string copy = value;
        if (!caseSensitive)
        {
            std::ranges::transform(
                copy.begin(),
                copy.end(),
                copy.begin(),
                [](const unsigned char character) -> unsigned char { return std::toupper(character); });
        }
        return copy;
    }

    friend bool operator==(const Identifier& lhs, const Identifier& rhs)
    {
        return lhs.toString() == rhs.toString();
    }
    friend bool operator!=(const Identifier& lhs, const Identifier& rhs) { return !(lhs == rhs); }
};

class IdentifierList
{
    std::vector<Identifier> identifiers;

public:
    explicit IdentifierList() = default;
    explicit IdentifierList(std::ranges::input_range auto&& identifiers) : identifiers(identifiers) { }
    IdentifierList(const std::initializer_list<Identifier> identifiers) : identifiers(identifiers) { }
    IdentifierList(const Identifier& identifier) : identifiers(std::vector{identifier}) { }
    IdentifierList(const IdentifierList& other) = default;
    IdentifierList(IdentifierList&& other) noexcept = default;
    IdentifierList& operator=(const IdentifierList& other) = default;
    IdentifierList& operator=(IdentifierList&& other) noexcept = default;
    ~IdentifierList() = default;
    [[nodiscard]] decltype(identifiers.begin()) begin() const { return identifiers.begin(); }
    [[nodiscard]] decltype(identifiers.end()) end() const { return identifiers.end(); }

    IdentifierList copyReplaceLast(const std::initializer_list<Identifier> replacements)
    {
        if (replacements.size() >= identifiers.size())
        {
            return IdentifierList{replacements};
        }
        auto newIdentifiers = std::vector{identifiers};

        const auto *iter = replacements.begin();
        for (auto i = identifiers.size() - replacements.size(); i < identifiers.size(); ++i)
        {
            newIdentifiers[i] = *iter;
            iter++;
        }
        return IdentifierList{std::move(newIdentifiers)};
    }
    friend bool operator==(const IdentifierList& lhs, const IdentifierList& rhs) { return lhs.identifiers == rhs.identifiers; }
    friend bool operator!=(const IdentifierList& lhs, const IdentifierList& rhs) { return !(lhs == rhs); }
};


static_assert(std::ranges::random_access_range<IdentifierList>);
