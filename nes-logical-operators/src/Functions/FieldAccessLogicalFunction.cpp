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
#include <format>
#include <memory>
#include <string>
#include <utility>
#include <API/AttributeField.hpp>
#include <API/Schema.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Nodes/Node.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Common/DataTypes/DataTypeProvider.hpp>
namespace NES
{
FieldAccessLogicalFunction::FieldAccessLogicalFunction(std::shared_ptr<DataType> stamp, std::string fieldName)
    : LogicalFunction(std::move(stamp), "FieldAccess"), fieldName(std::move(fieldName)) {};

FieldAccessLogicalFunction::FieldAccessLogicalFunction(FieldAccessLogicalFunction* other)
    : LogicalFunction(other), fieldName(other->getFieldName()) {};

std::shared_ptr<LogicalFunction> FieldAccessLogicalFunction::create(std::shared_ptr<DataType> stamp, std::string fieldName)
{
    return std::make_shared<FieldAccessLogicalFunction>(FieldAccessLogicalFunction(std::move(stamp), std::move(fieldName)));
}

std::shared_ptr<LogicalFunction> FieldAccessLogicalFunction::create(std::string fieldName)
{
    return create(DataTypeProvider::provideDataType(LogicalType::UNDEFINED), std::move(fieldName));
}

bool FieldAccessLogicalFunction::equal(const std::shared_ptr<LogicalFunction>& rhs) const
{
    if (NES::Util::instanceOf<FieldAccessLogicalFunction>(rhs))
    {
        auto otherFieldRead = NES::Util::as<FieldAccessLogicalFunction>(rhs);
        bool fieldNamesMatch = otherFieldRead->fieldName == fieldName;
        const bool stampsMatch = *otherFieldRead->stamp == *stamp;
        return fieldNamesMatch and stampsMatch;
    }
    return false;
}

std::string FieldAccessLogicalFunction::getFieldName() const
{
    return fieldName;
}

void FieldAccessLogicalFunction::updateFieldName(std::string fieldName)
{
    auto copy = *this;
    copy.fieldName = fieldName;
    return copy;
}

std::string FieldAccessLogicalFunction::toString() const
{
    return std::format("FieldAccessLogicalFunction( {} [ {} ])", fieldName, stamp->toString());
}

void FieldAccessLogicalFunction::inferStamp(const Schema& schema)
{
    /// check if the access field is defined in the schema.
    if (const auto existingField = schema.getFieldByName(fieldName))
    {
        fieldName = existingField.value()->getName();
        stamp = existingField.value()->getDataType();
        return;
    }
    throw QueryInvalid("FieldAccessFunction: the field {} is not defined in the schema {}", fieldName, schema.toString());
}

std::shared_ptr<LogicalFunction> FieldAccessLogicalFunction::deepCopy()
{
    return std::make_shared<FieldAccessLogicalFunction>(*this);
}

}
