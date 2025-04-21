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
#include <Functions/LogicalFunction.hpp>
#include <SerializableFunction.pb.h>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES
{

MinAggregationLogicalFunction::MinAggregationLogicalFunction(FieldAccessLogicalFunction field)
    : WindowAggregationLogicalFunction(field.getStamp(), field.getStamp(), field.getStamp(), field)
{
}
MinAggregationLogicalFunction::MinAggregationLogicalFunction(FieldAccessLogicalFunction field, FieldAccessLogicalFunction asField)
    : WindowAggregationLogicalFunction(field.getStamp(), field.getStamp(), field.getStamp(), field, asField)
{
}

std::shared_ptr<WindowAggregationLogicalFunction>
MinAggregationLogicalFunction::create(FieldAccessLogicalFunction onField, FieldAccessLogicalFunction asField)
{
    return std::make_shared<MinAggregationLogicalFunction>(std::move(onField), std::move(asField));
}

std::shared_ptr<WindowAggregationLogicalFunction> MinAggregationLogicalFunction::create(FieldAccessLogicalFunction onField)
{
    return std::make_shared<MinAggregationLogicalFunction>(onField);
}

std::string_view MinAggregationLogicalFunction::getName() const noexcept
{
    return NAME;
}

void MinAggregationLogicalFunction::inferStamp(const Schema& schema)
{
    /// We first infer the stamp of the input field and set the output stamp as the same.
    onField = onField.withInferredStamp(schema).get<FieldAccessLogicalFunction>();
    INVARIANT(dynamic_cast<const Numeric*>(onField.getStamp().get()), "aggregations on non numeric fields is not supported.");

    ///Set fully qualified name for the as Field
    const auto onFieldName = onField.getFieldName();
    const auto asFieldName = asField.getFieldName();

    const auto attributeNameResolver = onFieldName.substr(0, onFieldName.find(Schema::ATTRIBUTE_NAME_SEPARATOR) + 1);
    ///If on and as field name are different then append the attribute name resolver from on field to the as field
    if (asFieldName.find(Schema::ATTRIBUTE_NAME_SEPARATOR) == std::string::npos)
    {
        asField = asField.withFieldName(attributeNameResolver + asFieldName).get<FieldAccessLogicalFunction>();
    }
    else
    {
        const auto fieldName = asFieldName.substr(asFieldName.find_last_of(Schema::ATTRIBUTE_NAME_SEPARATOR) + 1);
        asField = asField.withFieldName(attributeNameResolver + fieldName).get<FieldAccessLogicalFunction>();
    }
    inputStamp = onField.getStamp();
    finalAggregateStamp = onField.getStamp();
    this->asField = asField.withStamp(getFinalAggregateStamp()).get<FieldAccessLogicalFunction>();
}

NES::SerializableAggregationFunction MinAggregationLogicalFunction::serialize() const
{
    NES::SerializableAggregationFunction serializedAggregationFunction;
    serializedAggregationFunction.set_type(NAME);

    auto *onFieldFuc = new SerializableFunction();
    onFieldFuc->CopyFrom(onField.serialize());

    auto *asFieldFuc = new SerializableFunction();
    asFieldFuc->CopyFrom(asField.serialize());

    serializedAggregationFunction.set_allocated_as_field(asFieldFuc);
    serializedAggregationFunction.set_allocated_on_field(onFieldFuc);
    return serializedAggregationFunction;
}

}
