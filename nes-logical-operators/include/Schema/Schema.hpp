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

#include <Identifiers/Identifier.hpp>


#include <concepts>
#include <cstddef>
#include <cstdint>
#include <initializer_list>
#include <optional>
#include <ostream>
#include <ranges>
#include <span>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <Schema/Field.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>
#include <DataTypes/UnboundSchema.hpp>
#include <Util/Ranges.hpp>

namespace NES
{

class Schema
{
public:
private:
    using IdSpan = std::span<const Identifier>;
    //take const reference to make sure that the spans don't dangle
    template <std::ranges::input_range Range>
    std::pair<
        std::unordered_map<IdSpan, size_t, std::hash<IdSpan>, IdentifierList::SpanEquals>,
        std::unordered_map<
            IdSpan,
            std::vector<size_t>,
            std::hash<IdSpan>,
            IdentifierList::SpanEquals>> static initializeFields(const Range& fields) noexcept
    {
        std::unordered_map<IdSpan, size_t, std::hash<IdSpan>, IdentifierList::SpanEquals> fieldsByName{};
        std::unordered_map<IdSpan, std::vector<size_t>, std::hash<IdSpan>, IdentifierList::SpanEquals> collisions{};

        for (const std::tuple<IdentifierList, Field, size_t>& field : fields)
        {
            const IdentifierList& fullName = std::get<IdentifierList>(field);
            for (size_t i = 0; i < std::ranges::size(fullName); i++)
            {
                IdSpan idSubSpan = std::span{std::ranges::begin(fullName) + i, std::ranges::size(fullName) - i};
                if (auto existingCollisions = collisions.find(idSubSpan); existingCollisions == collisions.end())
                {
                    if (auto existingIdList = fieldsByName.find(idSubSpan); existingIdList != fieldsByName.end())
                    {
                        collisions.insert(std::pair<IdSpan, std::vector<size_t>>{idSubSpan, std::vector{std::get<size_t>(field)}});
                        fieldsByName.erase(existingIdList);
                    }
                    else
                    {
                        fieldsByName.insert(std::pair{idSubSpan, std::get<size_t>(field)});
                    }
                }
                else
                {
                    existingCollisions->second.push_back(std::get<size_t>(field));
                }
            }
        }
        return {fieldsByName, collisions};
    }

    struct Private
    {
    };

    Schema(Private, std::vector<Field> fields, std::unordered_map<IdentifierList, size_t> nameToFields) noexcept
        : fields(std::move(fields)), nameToField(std::move(nameToFields))
    {
    }

    template <std::ranges::input_range Range>
    requires(std::same_as<std::tuple<IdentifierList, Field>, std::ranges::range_value_t<Range>>)
    explicit Schema(Private, const Range& input) noexcept
    {
        auto enumerated
            = views::enumerate(input)
            | std::views::transform(
                  [](auto pair)
                  {
                      return std::tuple{std::get<IdentifierList>(std::get<1>(pair)), std::get<Field>(std::get<1>(pair)), std::get<0>(pair)};
                  });
        fields
            = enumerated | std::views::transform([](auto tuple) { return std::get<Field>(tuple); }) | std::ranges::to<std::vector<Field>>();
        auto [fieldsByName, collisions] = initializeFields(enumerated);
        nameToField = fieldsByName
            | std::views::transform([](auto pair)
                                    { return std::make_pair<IdentifierList, size_t>(IdentifierList{pair.first}, std::move(pair.second)); })
            | std::ranges::to<std::unordered_map<IdentifierList, size_t>>();
    }

public:
    /// Enum to identify the memory layout in which we want to represent the schema physically.
    struct QualifiedFieldName
    {
        explicit QualifiedFieldName(std::string streamName, std::string fieldName)
            : streamName(std::move(streamName)), fieldName(std::move(fieldName))
        {
            PRECONDITION(not this->streamName.empty(), "Cannot create a QualifiedFieldName with an empty field name");
            PRECONDITION(not this->fieldName.empty(), "Cannot create a QualifiedFieldName with an empty field name");
        }
        std::string streamName;
        std::string fieldName;
    };

    Schema() = default;
    Schema(std::initializer_list<Field> fields) noexcept;


    //template overloading on input ranges for fields and schemas which require separate handling
    //Since std::initializer_list also implements std::input_range we need to make sure we don't overload it and exclude it
    //While we are implementing them in the header,
    //we should explicitly instantiate as many of the used specializations as possible at the end of the cpp to reduce compile times
    template <std::ranges::input_range Range>
    requires(std::same_as<Field, std::ranges::range_value_t<Range>> && !std::same_as<Range, std::initializer_list<Field>>)
    explicit Schema(const Range& input) noexcept
        : Schema(
              Private{},
              input
                  | std::views::transform([](const Field& field)
                                          { return std::make_tuple(IdentifierList{field.getLastName()}, Field{field}); }))
    {
    }

    template <std::ranges::input_range Range>
    requires(std::same_as<Field, std::ranges::range_value_t<Range>> && !std::same_as<Range, std::initializer_list<Field>>)
    static std::expected<Schema, std::unordered_map<IdentifierList, std::vector<Field>>> tryCreateCollisionFree(Range input) noexcept
    {
        auto enumerated
            = views::enumerate(std::move(input))
            | std::views::transform(
                  [](const std::tuple<long, Field>& pair)
                  {
                      return std::make_tuple(IdentifierList{std::get<Field>(pair).getLastName()}, std::get<Field>(pair), std::get<0>(pair));
                  })
            | std::ranges::to<std::vector>();
        auto fields
            = enumerated | std::views::transform([](auto tuple) { return std::get<Field>(tuple); }) | std::ranges::to<std::vector<Field>>();
        auto [fieldsByName, collisions] = initializeFields(enumerated);
        if (collisions.size() > 0)
        {
            return std::unexpected{
                std::views::all(collisions)
                | std::views::transform(
                    [&enumerated](auto& pair)
                    {
                        return std::make_pair(
                            IdentifierList{pair.first},
                            pair.second | std::views::transform([&enumerated](auto index) { return std::get<Field>(enumerated.at(index)); })
                                | std::ranges::to<std::vector<Field>>());
                    })
                | std::ranges::to<std::unordered_map<IdentifierList, std::vector<Field>>>()};
        }
        auto nameToField = fieldsByName
            | std::views::transform([](auto pair) { return std::pair{IdentifierList{pair.first}, pair.second}; })
            | std::ranges::to<std::unordered_map<IdentifierList, size_t>>();
        return Schema(Private{}, fields, nameToField);
    }

    // template <std::ranges::input_range Range>
    // requires(std::same_as<Schema, std::ranges::range_value_t<Range>> && !std::same_as<Range, std::initializer_list<Schema>>)
    // static std::expected<Schema, std::unordered_map<IdentifierList, std::vector<Field>>> tryCreateCollisionFree(const Range& input) noexcept
    // {
    //     auto enumerated = views::enumerate(input | std::views::transform(&Schema::getFields) | std::views::join)
    //         | std::views::transform([](auto pair) { return std::tuple{pair.second.first, pair.second.second, pair.first}; });
    //     auto fields = enumerated | std::views::transform([](auto tuple) { return tuple.second; }) | std::ranges::to<std::vector<Field>>();
    //     auto [fieldsByName, collisions] = initializeFields(enumerated);
    //     if (collisions.size() > 0)
    //     {
    //         return collisions | std::views::transform([](auto pair) { return std::pair{IdentifierList{pair.first}, pair.second}; })
    //             | std::ranges::to<std::unordered_map<IdentifierList, std::vector<Field>>>();
    //     }
    //     auto nameToField = fieldsByName
    //         | std::views::transform([](auto pair) { return std::pair{IdentifierList{pair.first}, pair.second}; })
    //         | std::ranges::to<std::unordered_map<IdentifierList, size_t>>();
    //     return Schema(Private{}, fields, nameToField);
    // }

    ~Schema() = default;

    bool operator==(const Schema& other) const = default;
    friend std::ostream& operator<<(std::ostream& os, const Schema& schema);

    /// Replaces the type of the field

    /// @brief Returns the attribute field based on a qualified or unqualified field name.
    /// If an unqualified field name is given (e.g., `getFieldByName("fieldName")`), the function will match attribute fields with any source name.
    /// If a qualified field name is given (e.g., `getFieldByName("source$fieldName")`), the entire qualified field must match.
    /// Note that this function does not return a field with an ambiguous field name.
    [[nodiscard]] std::optional<Field> getFieldByName(const IdentifierList& fieldName) const;

    bool contains(const IdentifierList& qualifiedFieldName) const;

    std::vector<IdentifierList> getUniqueFieldNames() const&;
    [[nodiscard]] const std::vector<Field>& getFields() const;


    auto begin() const -> decltype(std::declval<const std::vector<Field>>().cbegin()) { return fields.cbegin(); }

    auto end() const -> decltype(std::declval<const std::vector<Field>>().cend()) { return fields.cend(); }

    static std::string createCollisionString(const std::unordered_map<IdentifierList, std::vector<Field>>& collisions);

    operator UnboundSchema() const;

private:
    /// Manipulating fields requires us to update the size of the schema (in bytes) and the 'nameToFieldMap', which maps names of fields to
    /// their corresponding indexes in the 'fields' vector. Thus, the below three members are private to prevent accidental manipulation.
    std::vector<Field> fields{};
    std::unordered_map<IdentifierList, size_t> nameToField{};
};

static_assert(std::ranges::range<Schema>);

}

FMT_OSTREAM(NES::Schema);
