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
#include <memory>
#include <optional>
#include <string>
#include <vector>
#include <API/AttributeField.hpp>
#include <Common/DataTypes/BasicTypes.hpp>
#include <Common/DataTypes/DataType.hpp>

namespace NES
{


class Schema
{
public:
    /// @brief Enum to identify the memory layout in which we want to represent the schema physically.
    enum class MemoryLayoutType : uint8_t
    {
        ROW_LAYOUT = 0,
        COLUMNAR_LAYOUT = 1
    };

    explicit Schema();
    explicit Schema(MemoryLayoutType layoutType);
    explicit Schema(const std::shared_ptr<Schema>& schema, MemoryLayoutType layoutType);

    /// @brief Schema qualifier separator
    constexpr static auto ATTRIBUTE_NAME_SEPARATOR = "$";

    /// @brief Factory method to create a new std::shared_ptr<Schema>.
    /// @return std::shared_ptr<Schema>
    static std::shared_ptr<Schema> create(MemoryLayoutType layoutType);
    static std::shared_ptr<Schema> create();

    /// @brief Prepends the srcName to the substring after the last occurrence of ATTRIBUTE_NAME_SEPARATOR
    /// in every field name of the schema.
    /// @param srcName
    /// @return std::shared_ptr<Schema>

    [[nodiscard]] std::shared_ptr<Schema> updateSourceName(const std::string& srcName) const;

    /// @brief Creates a copy of this schema.
    /// @note The containing AttributeFields may still reference the same objects.
    /// @return A copy of the Schema

    [[nodiscard]] std::shared_ptr<Schema> copy() const;

    /// @brief Copy all fields of otherSchema into this schema.
    /// @param otherSchema
    /// @return a copy of this schema.
    std::shared_ptr<Schema> copyFields(const std::shared_ptr<Schema>& otherSchema);

    /// @brief appends a AttributeField to the schema and returns a copy of this schema.
    /// @param attribute
    /// @return a copy of this schema.
    std::shared_ptr<Schema> addField(const std::shared_ptr<AttributeField>& attribute);

    /// @brief appends a field with a basic type to the schema and returns a copy of this schema.
    /// @param field
    /// @return a copy of this schema.
    std::shared_ptr<Schema> addField(const std::string& name, const BasicType& type);

    /// @brief appends a field with a data type to the schema and returns a copy of this schema.
    /// @param field
    /// @return a copy of this schema.
    std::shared_ptr<Schema> addField(const std::string& name, const std::shared_ptr<DataType>& data);

    /// @brief removes a AttributeField from the schema
    /// @param field
    void removeField(const std::shared_ptr<AttributeField>& field);

    /// @brief Replaces a field, which is already part of the schema.
    /// @param name of the field we want to replace
    /// @param std::shared_ptr<DataType>
    void replaceField(const std::string& name, const std::shared_ptr<DataType>& type);

    /// @brief Returns the attribute field based on a qualified or unqualified field name.
    ///
    /// @details
    /// If an unqualified field name is given (e.g., `getFieldByName("fieldName")`), the function will match attribute fields with any source name.
    /// If a qualified field name is given (e.g., `getFieldByName("source$fieldName")`), the entire qualified field must match.
    /// Note that this function does not return a field with an ambiguous field name.
    //
    /// @param fieldName: Name of the attribute field that should be returned.
    /// @return std::optional<std::shared_ptr<AttributeField>> The attribute field if found, otherwise an empty optional.
    [[nodiscard]] std::optional<std::shared_ptr<AttributeField>> getFieldByName(const std::string& fieldName) const;

    /// @brief Finds a attribute field by index in the schema
    /// @param index
    /// @return AttributeField
    /// @throws FieldNotFound if the field does not exist
    [[nodiscard]] std::shared_ptr<AttributeField> getFieldByIndex(size_t index) const;

    /// @brief Returns the number of fields in the schema.
    /// @return uint64_t
    [[nodiscard]] size_t getFieldCount() const;

    /// @brief Returns the number of bytes all fields in this schema occupy.
    /// @return uint64_t
    [[nodiscard]] uint64_t getSchemaSizeInBytes() const;

    /// @brief Checks if two Schemas are equal to each other.
    /// @param schema
    /// @return boolean
    bool operator==(const Schema& other) const;

    /// @brief Checks if the field exists in the schema
    /// @param schema
    /// @return boolean
    bool contains(const std::string& fieldName) const;

    /// @brief returns a string representation
    /// @param prefix of the string
    /// @param delimitor between each field
    /// @param suffix, for the end of the string
    /// @return schema as string
    [[nodiscard]] std::string toString(const std::string& prefix = "", const std::string& sep = " ", const std::string& suffix = "") const;

    /// @brief Method to return the source name qualifier, thus everything that is before $
    /// @return string
    [[nodiscard]] std::string getSourceNameQualifier() const;

    /// @brief method to get the qualifier of the source without $
    /// @return qualifier without $
    std::string getQualifierNameForSystemGeneratedFields() const;

    /// @brief method to get the qualifier of the source with $
    /// @return qualifier with $
    std::string getQualifierNameForSystemGeneratedFieldsWithSeparator() const;

    /// @brief Remove all fields and qualifying name
    void clear();

    /// @brief Is checks if the schema is empty (if it has no fields).
    /// @return true if empty
    bool empty() const;

    /// @brief method to get the type of the memory layout
    /// @return MemoryLayoutType
    [[nodiscard]] MemoryLayoutType getLayoutType() const;

    /// @brief method to set the memory layout
    /// @param layoutType
    void setLayoutType(MemoryLayoutType layoutType);

    /// @brief Get the field names as a vector of strings.
    /// @return std::vector<std::string> fieldNames
    std::vector<std::string> getFieldNames() const;

    auto begin() const { return std::begin(fields); }

    auto end() const { return std::end(fields); }

private:
    std::vector<std::shared_ptr<AttributeField>> fields;
    MemoryLayoutType layoutType;
};

}
