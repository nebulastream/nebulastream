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
#include <ErrorHandling.hpp>
#include <LogicalOperators/Windows/Aggregations/MinAggregationFunction.hpp>

namespace NES::Logical
{
MinAggregationFunction::MinAggregationFunction(FieldAccessFunction field)
    : WindowAggregationFunction(field.getStamp(), field.getStamp(), field.getStamp(), field)
{
}
MinAggregationFunction::MinAggregationFunction(FieldAccessFunction field, FieldAccessFunction asField)
    : WindowAggregationFunction(field.getStamp(), field.getStamp(), field.getStamp(), field, asField)
{
}

std::shared_ptr<WindowAggregationFunction>
MinAggregationFunction::create(FieldAccessFunction onField, FieldAccessFunction asField)
{
    return std::make_shared<MinAggregationFunction>(std::move(onField), std::move(asField));
}

std::shared_ptr<WindowAggregationFunction> MinAggregationFunction::create(FieldAccessFunction onField)
{
    return std::make_shared<MinAggregationFunction>(onField);
}

std::string_view MinAggregationFunction::getName() const noexcept
{
    return NAME;
}

void MinAggregationFunction::inferStamp(const Schema& schema)
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
    this->asField = asField.withStamp(getFinalAggregateStamp()).get<FieldAccessFunction>();
}

NES::SerializableAggregationFunction MinAggregationFunction::serialize() const
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

AggregationFunctionRegistryReturnType AggregationFunctionGeneratedRegistrar::RegisterMinAggregationFunction(AggregationFunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.fields.size() == 2, "MinAggregationFunction requires exactly two fields, but got {}", arguments.fields.size());
    return MinAggregationFunction::create(arguments.fields[0], arguments.fields[1]);
}
}
