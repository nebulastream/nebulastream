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
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

class Schema
{
public:
    struct Field
    {
        Field() = default;
        Field(IdentifierList name, DataType dataType);

        friend std::ostream& operator<<(std::ostream& os, const Field& field);
        bool operator==(const Field&) const = default;

        IdentifierList name;
        DataType dataType{};
    };

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
            IdentifierList::SpanEquals>> static initializeFields(Range& fields) noexcept
    {
        std::unordered_map<IdSpan, size_t, std::hash<IdSpan>, IdentifierList::SpanEquals> fieldsByName{};
        std::unordered_map<IdSpan, std::vector<size_t>, std::hash<IdSpan>, IdentifierList::SpanEquals> collisions{};

        for (std::pair<Schema::Field, size_t>& field : fields)
        {
            IdentifierList& fullName = field.first.name;
            for (size_t i = 0; i < std::ranges::size(fullName); i++)
            {
                IdSpan idSubSpan = std::span{std::ranges::begin(fullName) + i, std::ranges::size(fullName) - i};
                if (auto existingCollisions = collisions.find(idSubSpan); existingCollisions == collisions.end())
                {
                    if (auto existingIdList = fieldsByName.find(idSubSpan); existingIdList != fieldsByName.end())
                    {
                        collisions.insert(std::pair<IdSpan, std::vector<size_t>>{idSubSpan, std::vector{field.second}});
                        fieldsByName.erase(existingIdList);
                    }
                    else
                    {
                        fieldsByName.insert(std::pair{idSubSpan, field.second});
                    }
                }
                else
                {
                    existingCollisions->second.push_back(field.second);
                }
            }
        }
        return {fieldsByName, collisions};
    }

    static std::optional<IdentifierList> findCommonPrefix(const std::ranges::input_range auto& fields)
    {
        IdSpan candidate{};
        if (std::ranges::size(fields) > 0)
        {
            const Schema::Field& current = *std::ranges::begin(fields);
            candidate = std::span{std::ranges::begin(current.name), std::ranges::end(current.name)};
        }
        //A fold over the fields, trying to find the common subspan starting from 0 in the names of all fields
        //Using Spans again to avoid unnecessary copies.
        if (std::ranges::size(fields) > 1)
        {
            for (const Schema::Field& field : std::span{std::ranges::begin(fields) + 1, std::ranges::size(fields) - 1})
            {
                const auto compareUpTo = std::min(std::ranges::size(candidate), std::ranges::size(field.name));
                IdSpan previousCandidate = candidate;
                candidate = {};
                for (size_t i = 0; i < compareUpTo; ++i)
                {
                    const IdSpan candidateSubspan = std::span{std::ranges::begin(previousCandidate), compareUpTo - i};
                    const IdSpan currentSubspan = std::span{std::ranges::begin(field.name), compareUpTo - i};
                    if (IdentifierList::SpanEquals{}(candidateSubspan, currentSubspan))
                    {
                        candidate = candidateSubspan;
                        break;
                    }
                }
                if (std::ranges::empty(candidate))
                {
                    NES_DEBUG("Did not find a common prefix for schema");
                    return std::nullopt;
                }
            }
        }
        //last check to catch corner cases such as fields having an empty name
        if (std::ranges::empty(candidate))
        {
            NES_DEBUG("Did not find a common prefix for schema");
            return std::nullopt;
        }
        return IdentifierList{candidate};
    }
    struct Private
    {
    };

    template <std::ranges::input_range Range>
    requires(std::same_as<Schema::Field, std::ranges::range_value_t<Range>>)
    explicit Schema(Private, const Range& input) noexcept : fields(input | ranges::to<std::vector<Schema::Field>>())
{
    auto enumerated = std::vector<std::pair<Field, size_t>>{};
    enumerated.reserve(std::ranges::size(fields));
    for (size_t i = 0; i < std::ranges::size(fields); ++i)
    {
        enumerated.push_back({this->fields[i], i});
    }
    auto [fieldsByName, collisions] = initializeFields(enumerated);
    nameToField = fieldsByName | std::views::transform([](auto pair) { return std::pair{IdentifierList{pair.first}, pair.second}; })
        | ranges::to<std::unordered_map<IdentifierList, size_t>>();
    currentPrefix = findCommonPrefix(fields);
}

public:
    /// Enum to identify the memory layout in which we want to represent the schema physically.
    enum class MemoryLayoutType : uint8_t
    {
        ROW_LAYOUT = 0,
        COLUMNAR_LAYOUT = 1
    };
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
    Schema(std::initializer_list<Schema> fields) noexcept;


    //template overloading on input ranges for fields and schemas which require separate handling
    //Since std::initializer_list also implements std::input_range we need to make sure we don't overload it and exclude it
    //While we are implementing them in the header,
    //we should explicitly instantiate as many of the used specializations as possible at the end of the cpp to reduce compile times
    template <std::ranges::input_range Range>
    requires(std::same_as<Schema::Field, std::ranges::range_value_t<Range>> && !std::same_as<Range, std::initializer_list<Schema::Field>>)
    explicit Schema(const Range& input) noexcept : Schema(Private{}, input)
    {
    }

    template <std::ranges::input_range Range>
    requires(std::same_as<Schema, std::ranges::range_value_t<Range>> && !std::same_as<Range, std::initializer_list<Schema>>)
    explicit Schema(const Range& input) noexcept : Schema(Private{}, input | std::views::transform(&Schema::getFields) | std::views::join)
    {
    }

    explicit Schema(MemoryLayoutType memoryLayoutType);
    ~Schema() = default;

    bool operator==(const Schema& other) const = default;
    friend std::ostream& operator<<(std::ostream& os, const Schema& schema);

    Schema addField(const IdentifierList& name, const DataType& dataType);
    Schema addField(const IdentifierList& name, DataType::Type type);

    /// Replaces the type of the field
    void replaceTypeOfField(const IdentifierList& name, DataType type);

    /// @brief Returns the attribute field based on a qualified or unqualified field name.
    /// If an unqualified field name is given (e.g., `getFieldByName("fieldName")`), the function will match attribute fields with any source name.
    /// If a qualified field name is given (e.g., `getFieldByName("source$fieldName")`), the entire qualified field must match.
    /// Note that this function does not return a field with an ambiguous field name.
    [[nodiscard]] std::optional<Field> getFieldByName(const IdentifierList& fieldName) const;

    /// @Note: Raises a 'FieldNotFound' if the index is out of bounds.
    [[nodiscard]] Field getFieldAt(size_t index) const;

    bool contains(const IdentifierList& qualifiedFieldName) const;


    IdentifierList getCommonPrefix() const;

    // [[nodiscard]] IdentifierList getSourceNameQualifier() const;
    //
    // /// get the qualifier of the source without ATTRIBUTE_NAME_SEPARATOR
    // std::optional<IdentifierList> getQualifierNameForSystemGeneratedFields() const;

    /// method to get the qualifier of the source with ATTRIBUTE_NAME_SEPARATOR
    // std::string getQualifierNameForSystemGeneratedFieldsWithSeparator() const;

    [[nodiscard]] bool hasFields() const;
    [[nodiscard]] size_t getNumberOfFields() const;
    std::vector<IdentifierList> getUniqueFieldNames() const&;
    [[nodiscard]] const std::vector<Field>& getFields() const;
    void assignToFields(const Schema& otherSchema);
    void appendFieldsFromOtherSchema(const Schema& otherSchema);
    void renameField(const IdentifierList& oldFieldName, IdentifierList newFieldName);

    size_t getSizeOfSchemaInBytes() const;

    /// Public members that we get and set.
    MemoryLayoutType memoryLayoutType{MemoryLayoutType::ROW_LAYOUT};

private:
    /// Manipulating fields requires us to update the size of the schema (in bytes) and the 'nameToFieldMap', which maps names of fields to
    /// their corresponding indexes in the 'fields' vector. Thus, the below three members are private to prevent accidental manipulation.
    std::vector<Field> fields{};
    size_t sizeOfSchemaInBytes{0};
    std::unordered_map<IdentifierList, size_t> nameToField{};
    IdentifierList currentPrefix{};
};

}

FMT_OSTREAM(NES::Schema);
FMT_OSTREAM(NES::Schema::Field);
