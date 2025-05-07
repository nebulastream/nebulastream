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
#include <API/Schema.hpp>
#include <LogicalFunctions/FieldAccessFunction.hpp>
#include <LogicalFunctions/Function.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <SerializableFunction.pb.h>
#include <Common/DataTypes/DataType.hpp>
#include <Common/DataTypes/Numeric.hpp>
#include <AggregationFunctionRegistry.hpp>
#include <LogicalOperators/Windows/Aggregations/SumAggregationFunction.hpp>

namespace NES::Logical
{
SumAggregationFunction::SumAggregationFunction(const FieldAccessFunction& field)
    : WindowAggregationFunction(field.getStamp(), field.getStamp(), field.getStamp(), std::move(field))
{
}
SumAggregationFunction::SumAggregationFunction(
    const FieldAccessFunction& field, const FieldAccessFunction& asField)
    : WindowAggregationFunction(field.getStamp(), field.getStamp(), field.getStamp(), std::move(field), std::move(asField))
{
}

std::string_view SumAggregationFunction::getName() const noexcept
{
    return NAME;
}

std::shared_ptr<WindowAggregationFunction>
SumAggregationFunction::create(const FieldAccessFunction& onField, const FieldAccessFunction& asField)
{
    return std::make_shared<SumAggregationFunction>(onField, asField);
}

std::shared_ptr<WindowAggregationFunction> SumAggregationFunction::create(Function onField)
{
    return std::make_shared<SumAggregationFunction>(onField.get<FieldAccessFunction>());
}

void SumAggregationFunction::inferStamp(const Schema& schema)
{
    /// We first infer the stamp of the input field and set the output stamp as the same.
    onField = onField.withInferredStamp(schema).get<FieldAccessFunction>();
    INVARIANT(dynamic_cast<const Numeric*>(onField.getStamp().get()), "aggregations on non numeric fields is not supported, but got {}", onField.getStamp()->toString());

    ///Set fully qualified name for the as Field
    const auto onFieldName = onField.getFieldName();
    const auto asFieldName = asField.getFieldName();

    const auto attributeNameResolver = onFieldName.substr(0, onFieldName.find(Schema::ATTRIBUTE_NAME_SEPARATOR) + 1);
    ///If on and as field name are different then append the attribute name resolver from on field to the as field
    if (asFieldName.find(Schema::ATTRIBUTE_NAME_SEPARATOR) == std::string::npos)
    {
        asField = asField.withFieldName(attributeNameResolver + asFieldName).get<FieldAccessFunction>();
    }
    else
    {
        const auto fieldName = asFieldName.substr(asFieldName.find_last_of(Schema::ATTRIBUTE_NAME_SEPARATOR) + 1);
        asField = asField.withFieldName(attributeNameResolver + fieldName).get<FieldAccessFunction>();
    }
    inputStamp = onField.getStamp();
    finalAggregateStamp = onField.getStamp();
    asField = asField.withStamp(finalAggregateStamp).get<FieldAccessFunction>();
}

NES::SerializableAggregationFunction SumAggregationFunction::serialize() const
{
    NES::SerializableAggregationFunction serializedAggregationFunction;
    serializedAggregationFunction.set_type(NAME);

    auto onFieldFuc = SerializableFunction();
    onFieldFuc.CopyFrom(onField.serialize());

    auto asFieldFuc = SerializableFunction();
    asFieldFuc.CopyFrom(asField.serialize());

    serializedAggregationFunction.mutable_as_field()->CopyFrom(asFieldFuc);
    serializedAggregationFunction.mutable_on_field()->CopyFrom(onFieldFuc);
    return serializedAggregationFunction;
}

AggregationFunctionRegistryReturnType AggregationFunctionGeneratedRegistrar::RegisterSumAggregationFunction(AggregationFunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.fields.size() == 2, "SumAggregationFunction requires exactly two fields, but got {}", arguments.fields.size());
    return SumAggregationFunction::create(arguments.fields[0], arguments.fields[1]);
}
}
