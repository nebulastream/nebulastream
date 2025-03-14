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

#include <memory>
#include <utility>
#include <Operators/Windows/Aggregations/MinAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/WindowAggregationLogicalFunction.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Common/DataTypes/Numeric.hpp>
#include <API/Schema.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Abstract/LogicalFunction.hpp>
#include <SerializableFunction.pb.h>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES
{

MinAggregationLogicalFunction::MinAggregationLogicalFunction(std::unique_ptr<FieldAccessLogicalFunction> field)
    : WindowAggregationLogicalFunction(field->getStamp().clone(), field->getStamp().clone(), field->getStamp().clone(), std::move(field))
{
    this->aggregationType = Type::Min;
}
MinAggregationLogicalFunction::MinAggregationLogicalFunction(std::unique_ptr<LogicalFunction> field, std::unique_ptr<LogicalFunction> asField)
    : WindowAggregationLogicalFunction(field->getStamp().clone(), field->getStamp().clone(), field->getStamp().clone(), std::move(field), std::move(asField))
{
    this->aggregationType = Type::Min;
}

std::unique_ptr<WindowAggregationLogicalFunction>
MinAggregationLogicalFunction::create(std::unique_ptr<FieldAccessLogicalFunction> onField, std::unique_ptr<FieldAccessLogicalFunction> asField)
{
    return std::make_unique<MinAggregationLogicalFunction>(std::move(onField), std::move(asField));
}

std::unique_ptr<WindowAggregationLogicalFunction> MinAggregationLogicalFunction::create(std::unique_ptr<LogicalFunction> onField)
{
    if (auto function = dynamic_cast<const FieldAccessLogicalFunction*>(onField.get()))
    {
        return std::make_unique<MinAggregationLogicalFunction>(Util::unique_ptr_dynamic_cast<FieldAccessLogicalFunction>(std::move(onField)));
    }
    NES_ERROR("Query: window key has to be an FieldAccessFunction but it was a  {}", *onField);
    return nullptr;
}

std::unique_ptr<WindowAggregationLogicalFunction> MinAggregationLogicalFunction::clone()
{
    return std::make_unique<MinAggregationLogicalFunction>(onField->clone(), asField->clone());
}

void MinAggregationLogicalFunction::inferStamp(const Schema& schema)
{
    /// We first infer the stamp of the input field and set the output stamp as the same.
    onField->inferStamp(schema);
    if (dynamic_cast<Numeric*>(&onField->getStamp()) == nullptr)
    {
        NES_FATAL_ERROR("MinAggregationLogicalFunction: aggregations on non numeric fields is not supported.");
    }

    ///Set fully qualified name for the as Field
    const auto onFieldName = dynamic_cast<FieldAccessLogicalFunction*>(onField.get())->getFieldName();
    const auto asFieldName = dynamic_cast<FieldAccessLogicalFunction*>(asField.get())->getFieldName();

    const auto attributeNameResolver = onFieldName.substr(0, onFieldName.find(Schema::ATTRIBUTE_NAME_SEPARATOR) + 1);
    ///If on and as field name are different then append the attribute name resolver from on field to the as field
    if (asFieldName.find(Schema::ATTRIBUTE_NAME_SEPARATOR) == std::string::npos)
    {
        dynamic_cast<FieldAccessLogicalFunction*>(asField.get())->setFieldName(attributeNameResolver + asFieldName);
    }
    else
    {
        const auto fieldName = asFieldName.substr(asFieldName.find_last_of(Schema::ATTRIBUTE_NAME_SEPARATOR) + 1);
        dynamic_cast<FieldAccessLogicalFunction*>(asField.get())->setFieldName(attributeNameResolver + fieldName);
    }
    asField->setStamp(getFinalAggregateStamp().clone());
}

NES::SerializableAggregationFunction MinAggregationLogicalFunction::serialize() const
{
    NES::SerializableAggregationFunction serializedAggregationFunction;
    serializedAggregationFunction.set_type(NAME);

    auto *onFieldFuc = new SerializableFunction();
    onFieldFuc->CopyFrom(onField->serialize());

    auto *asFieldFuc = new SerializableFunction();
    asFieldFuc->CopyFrom(asField->serialize());

    serializedAggregationFunction.set_allocated_as_field(asFieldFuc);
    serializedAggregationFunction.set_allocated_on_field(onFieldFuc);
    return serializedAggregationFunction;
}

}
