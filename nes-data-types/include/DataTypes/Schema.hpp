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

#include <cstddef>
#include <cstdint>
#include <optional>
#include <ostream>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

struct Schema
{
    /// Enum to identify the memory layout in which we want to represent the schema physically.
    enum class MemoryLayoutType : uint8_t
    {
        ROW_LAYOUT = 0,
        COLUMNAR_LAYOUT = 1
    };
    struct Field
    {
        Field() = default;
        Field(std::string name, DataType dataType);

        friend std::ostream& operator<<(std::ostream& os, const Field& field);
        bool operator==(const Field&) const = default;

        std::string name{};
        DataType dataType{};
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

    /// schema qualifier separator
    constexpr static auto ATTRIBUTE_NAME_SEPARATOR = "$";
    Schema() = default;
    explicit Schema(MemoryLayoutType memoryLayoutType);
    ~Schema() = default;

    Schema addField(std::string name, const DataType& dataType);
    Schema addField(std::string name, DataType::Type type);

    /// Replaces the type of the field
    void replaceTypeOfField(const std::string& name, DataType type);

    [[nodiscard]] std::optional<Field> getFieldByName(const std::string& fieldName) const;

    /// @Note: Raises a 'FieldNotFound' if the index is out of bounds.
    [[nodiscard]] Field getFieldAt(size_t index) const;

    bool contains(const std::string& qualifiedFieldName) const;

    bool operator==(const Schema& other) const = default;
    friend std::ostream& operator<<(std::ostream& os, const Schema& schema);
    [[nodiscard]] std::string toString(const std::string& prefix = "", const std::string& sep = " ", const std::string& suffix = "") const;

    [[nodiscard]] std::string getSourceNameQualifier() const;

    /// get the qualifier of the source without ATTRIBUTE_NAME_SEPARATOR
    std::optional<std::string> getQualifierNameForSystemGeneratedFields() const;

    /// method to get the qualifier of the source with ATTRIBUTE_NAME_SEPARATOR
    std::string getQualifierNameForSystemGeneratedFieldsWithSeparator() const;

    [[nodiscard]] bool hasFields() const;
    [[nodiscard]] size_t getNumberOfFields() const;
    std::vector<std::string> getFieldNames() const;
    [[nodiscard]] const std::vector<Field>& getFields() const;
    void renameField(const std::string& oldFieldName, std::string_view newFieldName);
    void assignToFields(const Schema& otherSchema);
    void appendFieldsFromOtherSchema(const Schema& otherSchema);

    size_t sizeOfSchemaInBytes{0};
    MemoryLayoutType memoryLayoutType{MemoryLayoutType::ROW_LAYOUT};
    std::unordered_map<std::string, size_t> nameToField{};

private:
    /// private 'fields', because manipulating fields must change 'sizeOfSchemaInBytes' correctly
    /// e.g., copy assigning to fields must recalculate 'sizeOfSchemaInBytes'
    std::vector<Field> fields{};
};

}

FMT_OSTREAM(NES::Schema);
FMT_OSTREAM(NES::Schema::Field);
