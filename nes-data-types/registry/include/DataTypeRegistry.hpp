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

#include <vector>

#include <DataTypes/DataType.hpp>
#include <DataTypes/ScalarTypes.hpp>
#include <Identifiers/QualifiedIdentifier.hpp>
#include <Types/NESType.hpp>
#include <Util/RuntimeRegistry.hpp>

namespace NES
{

/// Registry of scalar type constructors (`NESFunctor`s), including each concrete scalar type
/// (`BooleanType`, `Int32Type`, `VarSizedType`, ...) and parameterized algebraic functors (`OptionalType`, etc.).
/// Exposes `ScalarDatatype`, `NumericDatatype`, `IntegerDatatype`, and `FloatDatatype` type classes.
class DataTypeRegistry : public RuntimeRegistry<DataTypeRegistry, QualifiedIdentifier, NESFunctor, /*CaseSensitive*/ false>
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
