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
#include <DataTypes/UnboundSchema.hpp>
#include <Schema/Field.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Ranges.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

class Schema
{
public:
private:
    //take const reference to make sure that the spans don't dangle
    template <std::ranges::input_range Range>
    std::pair<std::unordered_map<Identifier, size_t>, std::unordered_map<Identifier, std::vector<size_t>>> static initializeFields(
        const Range& fields) noexcept
    requires(std::same_as<Field, std::ranges::range_value_t<Range>>)
    {
        std::unordered_map<Identifier, size_t> fieldsByName{};
        std::unordered_map<Identifier, std::vector<size_t>> collisions{};
        for (const auto& [idxSigned, field] : fields | std::views::enumerate)
        {
            INVARIANT(idxSigned >= 0, "negative index");
            const auto idx = static_cast<size_t>(idxSigned);
            const auto& name = field.getLastName();
            auto collisionIter = collisions.find(name);
            if (collisionIter == collisions.end())
            {
                if (auto existingIdList = fieldsByName.find(name); existingIdList != fieldsByName.end())
                {
                    collisions.insert(std::pair{name, std::vector{existingIdList->second, idx}});
                    fieldsByName.erase(existingIdList);
                }
                else
                {
                    fieldsByName.insert(std::pair{name, idx});
                }
            }
            else
            {
                collisionIter->second.push_back(idx);
            }
        }
        if (!collisions.empty())
        {
            NES_DEBUG("Duplicate identifiers in schema: {}", fmt::join(collisions, ", "));
        }
        return std::pair{std::move(fieldsByName), std::move(collisions)};
    }

    struct Private
    {
    };

    Schema(Private, std::vector<Field> fields, std::unordered_map<IdentifierList, size_t> nameToFields) noexcept
        : fields(std::move(fields)), nameToField(std::move(nameToFields))
    {
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
    Schema(const Schema& other) = default;
    Schema(Schema&& other) noexcept = default;
    Schema& operator=(const Schema& other) = default;
    Schema& operator=(Schema&& other) noexcept = default;

    Schema(std::initializer_list<Field> fields) noexcept;
    explicit Schema(std::vector<Field> fields) noexcept;

    //template overloading on input ranges for fields and schemas which require separate handling
    //Since std::initializer_list also implements std::input_range we need to make sure we don't overload it and exclude it
    //While we are implementing them in the header,
    //we should explicitly instantiate as many of the used specializations as possible at the end of the cpp to reduce compile times
    template <std::ranges::input_range Range>
    requires(
        std::same_as<Field, std::ranges::range_value_t<Range>> && !std::same_as<Range, std::initializer_list<Field>>
        && !std::same_as<Range, std::vector<Field>> && !std::same_as<Range, Schema>)
    explicit Schema(Range&& input) noexcept : fields{input | std::ranges::to<std::vector>()}
    {
        auto [fieldsByName, collisions] = initializeFields(this->fields);
        nameToField = fieldsByName
            | std::views::transform([](auto pair)
                                    { return std::make_pair<IdentifierList, size_t>(IdentifierList{pair.first}, std::move(pair.second)); })
            | std::ranges::to<std::unordered_map<IdentifierList, size_t>>();
    }

    template <std::ranges::input_range Range>
    requires(std::same_as<Field, std::ranges::range_value_t<Range>> && !std::same_as<Range, std::initializer_list<Field>>)
    static std::expected<Schema, std::unordered_map<IdentifierList, std::vector<Field>>> tryCreateCollisionFree(Range input) noexcept
    {
        auto fields = input | std::ranges::to<std::vector>();
        auto [fieldsByName, collisions] = initializeFields(fields);
        if (collisions.size() > 0)
        {
            return std::unexpected{
                std::views::all(collisions)
                | std::views::transform(
                    [&fields](auto& pair)
                    {
                        return std::make_pair(
                            IdentifierList{pair.first},
                            pair.second | std::views::transform([&fields](auto index) { return fields.at(index); })
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
static_assert(std::same_as<std::ranges::range_value_t<Schema>, Field>);

}

FMT_OSTREAM(NES::Schema);
