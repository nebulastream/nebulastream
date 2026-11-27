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

#include <DataTypeRegistry.hpp>

#include <vector>

#include <DataTypes/DataType.hpp>
#include <DataTypes/ScalarTypes.hpp>
#include <Identifiers/QualifiedIdentifier.hpp>
#include <Types/NESType.hpp>

namespace NES
{

DataTypeRegistry& DataTypeRegistry::instance()
{
    static DataTypeRegistry inst;
    return inst;
}

NESTypeClass DataTypeRegistry::scalarDataTypeClass()
{
    return NESTypeClass::forTypeTrait<ScalarTypeTrait>();
}

NESTypeClass DataTypeRegistry::numericDataTypeClass()
{
    return NESTypeClass::forTypeTrait<NumericTypeTrait>();
}

NESTypeClass DataTypeRegistry::integerDataTypeClass()
{
    return NESTypeClass::forTypeTrait<IntegerTypeTrait>();
}

NESTypeClass DataTypeRegistry::floatDataTypeClass()
{
    return NESTypeClass::forTypeTrait<FloatTypeTrait>();
}

std::vector<NamedNESParametrizedTypeClass> DataTypeRegistry::getParametrizedTypeClasses()
{
    return {
        NamedNESParametrizedTypeClass::constant(QualifiedIdentifier::parse("ScalarDatatype"), scalarDataTypeClass()),
        NamedNESParametrizedTypeClass::constant(QualifiedIdentifier::parse("NumericDatatype"), numericDataTypeClass()),
        NamedNESParametrizedTypeClass::constant(QualifiedIdentifier::parse("IntegerDatatype"), integerDataTypeClass()),
        NamedNESParametrizedTypeClass::constant(QualifiedIdentifier::parse("FloatDatatype"), floatDataTypeClass())};
}

}
