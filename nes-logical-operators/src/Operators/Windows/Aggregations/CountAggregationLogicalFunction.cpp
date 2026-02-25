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

#include <Operators/Windows/Aggregations/CountAggregationLogicalFunction.hpp>

#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <DataTypes/Schema.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Operators/Windows/Aggregations/WindowAggregationLogicalFunction.hpp>
#include <Util/Reflection.hpp>
#include <AggregationLogicalFunctionRegistry.hpp>
#include <ErrorHandling.hpp>

namespace NES
{
CountAggregationLogicalFunction::CountAggregationLogicalFunction(const FieldAccessLogicalFunction& field)
    : WindowAggregationLogicalFunction(
          DataTypeProvider::provideDataType(inputAggregateStampType),
          DataTypeProvider::provideDataType(partialAggregateStampType),
          DataTypeProvider::provideDataType(finalAggregateStampType),
          field)
{
}

CountAggregationLogicalFunction::CountAggregationLogicalFunction(FieldAccessLogicalFunction field, FieldAccessLogicalFunction asField)
    : WindowAggregationLogicalFunction(
          DataTypeProvider::provideDataType(inputAggregateStampType),
          DataTypeProvider::provideDataType(partialAggregateStampType),
          DataTypeProvider::provideDataType(finalAggregateStampType),
          std::move(field),
          std::move(asField))
{
}

std::string_view CountAggregationLogicalFunction::getName() const noexcept
{
    return NAME;
}

Reflected CountAggregationLogicalFunction::reflect() const
{
    return NES::reflect(this);
}

void CountAggregationLogicalFunction::inferStamp(const Schema& schema)
{
    if (const auto sourceNameQualifier = schema.getSourceNameQualifier())
    {
        const auto attributeNameResolver = sourceNameQualifier.value() + std::string(Schema::ATTRIBUTE_NAME_SEPARATOR);
        const auto asFieldName = this->getAsField().getFieldName();

        ///If on and as field name are different then append the attribute name resolver from on field to the as field
        if (asFieldName.find(Schema::ATTRIBUTE_NAME_SEPARATOR) == std::string::npos)
        {
            this->setAsField(this->getAsField().withFieldName(attributeNameResolver + asFieldName));
        }
        else
        {
            const auto fieldName = asFieldName.substr(asFieldName.find_last_of(Schema::ATTRIBUTE_NAME_SEPARATOR) + 1);
            this->setAsField(this->getAsField().withFieldName(attributeNameResolver + fieldName));
        }

        /// a count aggregation is always on an uint 64
        this->setOnField(this->getOnField().withDataType(DataTypeProvider::provideDataType(DataType::Type::UINT64)));
        this->setAsField(this->getAsField().withDataType(DataTypeProvider::provideDataType(DataType::Type::UINT64)));
    }
    else
    {
        throw CannotInferSchema("Schema lacked source name qualifier: {}", schema);
    }
}

Reflected Reflector<CountAggregationLogicalFunction>::operator()(const CountAggregationLogicalFunction& function) const
{
    return reflect(detail::ReflectedCountAggregationLogicalFunction{.onField = function.getOnField(), .asField = function.getAsField()});
}

CountAggregationLogicalFunction
Unreflector<CountAggregationLogicalFunction>::operator()(const Reflected& reflected, const ReflectionContext& context) const
{
    auto [onField, asField] = context.unreflect<detail::ReflectedCountAggregationLogicalFunction>(reflected);

    if (!onField.has_value() || !asField.has_value())
    {
        throw CannotDeserialize("CountAggregationLogicalFunction is missing onField/asField function");
    }

    return {onField.value(), asField.value()};
}

AggregationLogicalFunctionRegistryReturnType
AggregationLogicalFunctionGeneratedRegistrar::RegisterCountAggregationLogicalFunction(AggregationLogicalFunctionRegistryArguments arguments)
{
    if (!arguments.reflected.isEmpty())
    {
        return std::make_shared<CountAggregationLogicalFunction>(
            ReflectionContext{}.unreflect<CountAggregationLogicalFunction>(arguments.reflected));
    }

    if (arguments.fields.size() != 2)
    {
        throw CannotDeserialize("CountAggregationLogicalFunction requires exactly two fields, but got {}", arguments.fields.size());
    }
    return std::make_shared<CountAggregationLogicalFunction>(arguments.fields[0], arguments.fields[1]);
}
}
