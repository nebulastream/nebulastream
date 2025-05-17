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
#include <string>
#include <string_view>
#include <utility>
#include <API/Schema.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Operators/Windows/Aggregations/MaxAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/WindowAggregationLogicalFunction.hpp>
#include <AggregationLogicalFunctionRegistry.hpp>
#include <ErrorHandling.hpp>
#include <SerializableVariantDescriptor.pb.h>
#include <Common/DataTypes/Numeric.hpp>


namespace NES
{
MaxAggregationLogicalFunction::MaxAggregationLogicalFunction(const FieldAccessLogicalFunction& field)
    : WindowAggregationLogicalFunction(field.getDataType(), field.getDataType(), field.getDataType(), field)
{
}

MaxAggregationLogicalFunction::MaxAggregationLogicalFunction(const FieldAccessLogicalFunction& field, FieldAccessLogicalFunction asField)
    : WindowAggregationLogicalFunction(field.getDataType(), field.getDataType(), field.getDataType(), field, std::move(asField))
{
}

std::shared_ptr<WindowAggregationLogicalFunction> MaxAggregationLogicalFunction::create(const FieldAccessLogicalFunction& onField)
{
    return std::make_shared<MaxAggregationLogicalFunction>(onField);
}

std::shared_ptr<WindowAggregationLogicalFunction>
MaxAggregationLogicalFunction::create(const FieldAccessLogicalFunction& onField, const FieldAccessLogicalFunction& asField)
{
    return std::make_shared<MaxAggregationLogicalFunction>(onField, asField);
}

std::string_view MaxAggregationLogicalFunction::getName() const noexcept
{
    return NAME;
}

void MaxAggregationLogicalFunction::inferStamp(const Schema& schema)
{
    /// We first infer the dataType of the input field and set the output dataType as the same.
    onField = onField.withInferredDataType(schema).get<FieldAccessLogicalFunction>();
    INVARIANT(
        dynamic_cast<const Numeric*>(onField.getDataType().get()),
        "aggregations on non numeric fields is not supported, but got {}",
        onField.getDataType()->toString());

    ///Set fully qualified name for the as Field
    auto onFieldName = onField.getFieldName();
    auto asFieldName = asField.getFieldName();

    const auto attributeNameResolver = onFieldName.substr(0, onFieldName.find(Schema::ATTRIBUTE_NAME_SEPARATOR) + 1);
    ///If on and as field name are different then append the attribute name resolver from on field to the as field
    if (asFieldName.find(Schema::ATTRIBUTE_NAME_SEPARATOR) == std::string::npos)
    {
        asField = asField.withFieldName(attributeNameResolver + asFieldName).get<FieldAccessLogicalFunction>();
    }
    else
    {
        auto fieldName = asFieldName.substr(asFieldName.find_last_of(Schema::ATTRIBUTE_NAME_SEPARATOR) + 1);
        asField = asField.withFieldName(attributeNameResolver + fieldName).get<FieldAccessLogicalFunction>();
    }
    inputStamp = onField.getDataType();
    finalAggregateStamp = onField.getDataType();
    asField = asField.withDataType(getFinalAggregateStamp()).get<FieldAccessLogicalFunction>();
}

SerializableAggregationFunction MaxAggregationLogicalFunction::serialize() const
{
    SerializableAggregationFunction serializedAggregationFunction;
    serializedAggregationFunction.set_type(NAME);

    auto onFieldFuc = SerializableFunction();
    onFieldFuc.CopyFrom(onField.serialize());

    auto asFieldFuc = SerializableFunction();
    asFieldFuc.CopyFrom(asField.serialize());

    serializedAggregationFunction.mutable_as_field()->CopyFrom(asFieldFuc);
    serializedAggregationFunction.mutable_on_field()->CopyFrom(onFieldFuc);
    return serializedAggregationFunction;
}

AggregationLogicalFunctionRegistryReturnType
AggregationLogicalFunctionGeneratedRegistrar::RegisterMaxAggregationLogicalFunction(AggregationLogicalFunctionRegistryArguments arguments)
{
    PRECONDITION(
        arguments.fields.size() == 2, "MaxAggregationLogicalFunction requires exactly two fields, but got {}", arguments.fields.size());
    return MaxAggregationLogicalFunction::create(arguments.fields[0], arguments.fields[1]);
}
}
