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

#include <functional>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <Identifiers/QualifiedIdentifier.hpp>
#include <Types/NESType.hpp>
#include <Util/RuntimeRegistry.hpp>

namespace NES
{

using DataTypeRegistryReturnType = DataType;

struct DataTypeRegistryArguments
{
    DataType::NULLABLE nullable{DataType::NULLABLE::IS_NULLABLE};
};

struct NumericDataTypeClassTag
{
    bool operator==(const NumericDataTypeClassTag&) const = default;
};

struct IntegerDataTypeClassTag
{
    bool operator==(const IntegerDataTypeClassTag&) const = default;
};

struct FloatDataTypeClassTag
{
    bool operator==(const FloatDataTypeClassTag&) const = default;
};

using DataTypeFn = std::function<DataTypeRegistryReturnType(DataTypeRegistryArguments)>;

/// Entries are keyed by the DataType::Type enumerator name; exposes `ScalarDatatype`,
/// `NumericDatatype`, `IntegerDatatype`, and `FloatDatatype` type classes.
class DataTypeRegistry : public RuntimeRegistry<DataTypeRegistry, QualifiedIdentifier, DataTypeFn, /*CaseSensitive*/ false>
{
public:
    static constexpr bool EXPOSE_TO_SQL_SCOPE = true;

    static DataTypeRegistry& instance();
    static NESTypeClass scalarDataTypeClass();
    static NESTypeClass numericDataTypeClass();
    static NESTypeClass integerDataTypeClass();
    static NESTypeClass floatDataTypeClass();
    static std::vector<NamedNESParametrizedTypeClass> getParametrizedTypeClasses();
};

}
