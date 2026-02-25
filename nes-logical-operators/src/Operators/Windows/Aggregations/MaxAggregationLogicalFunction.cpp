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

#include <Operators/Windows/Aggregations/MaxAggregationLogicalFunction.hpp>

#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <DataTypes/Schema.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Operators/Windows/Aggregations/WindowAggregationLogicalFunction.hpp>
#include <Util/Reflection.hpp>
#include <AggregationLogicalFunctionRegistry.hpp>
#include <ErrorHandling.hpp>

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

std::string_view MaxAggregationLogicalFunction::getName() const noexcept
{
    return NAME;
}

void MaxAggregationLogicalFunction::inferStamp(const Schema& schema)
{
    /// We first infer the dataType of the input field and set the output dataType as the same.
    this->setOnField(this->getOnField().withInferredDataType(schema).getAs<FieldAccessLogicalFunction>().get());
    if (not this->getOnField().getDataType().isNumeric())
    {
        throw CannotDeserialize("aggregations on non numeric fields is not supported, but got {}", this->getOnField().getDataType());
    }

    ///Set fully qualified name for the as Field
    auto onFieldName = this->getOnField().getFieldName();
    auto asFieldName = this->getAsField().getFieldName();

    const auto attributeNameResolver = onFieldName.substr(0, onFieldName.find(Schema::ATTRIBUTE_NAME_SEPARATOR) + 1);
    ///If on and as field name are different then append the attribute name resolver from on field to the as field
    if (asFieldName.find(Schema::ATTRIBUTE_NAME_SEPARATOR) == std::string::npos)
    {
        this->setAsField(this->getAsField().withFieldName(attributeNameResolver + asFieldName));
    }
    else
    {
        auto fieldName = asFieldName.substr(asFieldName.find_last_of(Schema::ATTRIBUTE_NAME_SEPARATOR) + 1);
        this->setAsField(this->getAsField().withFieldName(attributeNameResolver + fieldName));
    }
    this->setInputStamp(this->getOnField().getDataType());
    this->setFinalAggregateStamp(this->getOnField().getDataType());
    this->setAsField(this->getAsField().withDataType(getFinalAggregateStamp()));
}

Reflected MaxAggregationLogicalFunction::reflect() const
{
    return NES::reflect(this);
}

Reflected Reflector<MaxAggregationLogicalFunction>::operator()(const MaxAggregationLogicalFunction& function) const
{
    return reflect(detail::ReflectedMaxAggregationLogicalFunction{.onField = function.getOnField(), .asField = function.getAsField()});
}

MaxAggregationLogicalFunction
Unreflector<MaxAggregationLogicalFunction>::operator()(const Reflected& reflected, const ReflectionContext& context) const
{
    auto [onField, asField] = context.unreflect<detail::ReflectedMaxAggregationLogicalFunction>(reflected);
    return MaxAggregationLogicalFunction{onField, asField};
}

AggregationLogicalFunctionRegistryReturnType
AggregationLogicalFunctionGeneratedRegistrar::RegisterMaxAggregationLogicalFunction(AggregationLogicalFunctionRegistryArguments arguments)
{
    if (!arguments.reflected.isEmpty())
    {
        return std::make_shared<MaxAggregationLogicalFunction>(
            ReflectionContext{}.unreflect<MaxAggregationLogicalFunction>(arguments.reflected));
    }
    if (arguments.fields.size() != 2)
    {
        throw CannotDeserialize("MaxAggregationLogicalFunction requires exactly two fields, but got {}", arguments.fields.size());
    }
    return std::make_shared<MaxAggregationLogicalFunction>(arguments.fields[0], arguments.fields[1]);
}
}
