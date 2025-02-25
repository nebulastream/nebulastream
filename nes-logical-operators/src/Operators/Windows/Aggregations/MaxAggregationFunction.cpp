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
#include <Operators/Windows/Aggregations/MaxAggregationFunction.hpp>
#include <Operators/Windows/Aggregations/WindowAggregationFunction.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Common/DataTypes/Numeric.hpp>
#include <API/Schema.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <SerializableFunction.pb.h>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES::Windowing
{

MaxAggregationFunction::MaxAggregationFunction(std::unique_ptr<FieldAccessLogicalFunction> field)
    : WindowAggregationFunction(field->getStamp().clone(), field->getStamp().clone(), field->getStamp().clone(),std::move(field))
{
    this->aggregationType = Type::Max;
}

MaxAggregationFunction::MaxAggregationFunction(std::unique_ptr<LogicalFunction> field, std::unique_ptr<LogicalFunction> asField)
    : WindowAggregationFunction(field->getStamp().clone(), field->getStamp().clone(), field->getStamp().clone(), std::move(field), std::move(asField))
{
    this->aggregationType = Type::Max;
}

std::unique_ptr<WindowAggregationFunction>
MaxAggregationFunction::create(std::unique_ptr<FieldAccessLogicalFunction> onField, std::unique_ptr<FieldAccessLogicalFunction> asField)
{
    return std::make_unique<MaxAggregationFunction>(std::move(onField), std::move(asField));
}

std::unique_ptr<WindowAggregationFunction> MaxAggregationFunction::on(std::unique_ptr<LogicalFunction> onField)
{
    return std::make_unique<MaxAggregationFunction>(Util::unique_ptr_dynamic_cast<FieldAccessLogicalFunction>(std::move(onField)));
}

void MaxAggregationFunction::inferStamp(const Schema& schema)
{
    /// We first infer the stamp of the input field and set the output stamp as the same.
    onField->inferStamp(schema);
    if (dynamic_cast<Numeric*>(&onField->getStamp()) == nullptr)
    {
        NES_FATAL_ERROR("MaxAggregationFunction: aggregations on non numeric fields is not supported.");
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

std::unique_ptr<WindowAggregationFunction> MaxAggregationFunction::clone()
{
    return std::make_unique<MaxAggregationFunction>(onField->clone(), asField->clone());
}

NES::SerializableAggregationFunction MaxAggregationFunction::serialize() const
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
