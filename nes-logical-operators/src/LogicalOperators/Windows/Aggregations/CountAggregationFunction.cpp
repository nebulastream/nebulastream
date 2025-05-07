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
#include <string_view>
#include <utility>
#include <API/Schema.hpp>
#include <LogicalFunctions/FieldAccessFunction.hpp>
#include <LogicalFunctions/Function.hpp>
#include <Util/Common.hpp>
#include <ErrorHandling.hpp>
#include <SerializableFunction.pb.h>
#include <Common/DataTypes/DataType.hpp>
#include <Common/DataTypes/DataTypeProvider.hpp>
#include <AggregationFunctionRegistry.hpp>
#include <LogicalOperators/Windows/Aggregations/CountAggregationFunction.hpp>

namespace NES::Logical
{
CountAggregationFunction::CountAggregationFunction(FieldAccessFunction field)
    : WindowAggregationFunction(
          DataTypeProvider::provideDataType(LogicalType::UINT64),
          DataTypeProvider::provideDataType(LogicalType::UINT64),
          DataTypeProvider::provideDataType(LogicalType::UINT64),
          field)
{
}
CountAggregationFunction::CountAggregationFunction(FieldAccessFunction field, FieldAccessFunction asField)
    : WindowAggregationFunction(
          DataTypeProvider::provideDataType(LogicalType::UINT64),
          DataTypeProvider::provideDataType(LogicalType::UINT64),
          DataTypeProvider::provideDataType(LogicalType::UINT64),
          field,
          asField)
{
}

std::shared_ptr<WindowAggregationFunction>
CountAggregationFunction::create(FieldAccessFunction onField, FieldAccessFunction asField)
{
    return std::make_shared<CountAggregationFunction>(onField, asField);
}

std::shared_ptr<WindowAggregationFunction> CountAggregationFunction::create(FieldAccessFunction onField)
{
    return std::make_shared<CountAggregationFunction>(onField);
}

std::string_view CountAggregationFunction::getName() const noexcept
{
    return NAME;
}

void CountAggregationFunction::inferStamp(const Schema& schema)
{
    const auto attributeNameResolver = schema.getSourceNameQualifier() + Schema::ATTRIBUTE_NAME_SEPARATOR;
    const auto asFieldName = asField.getFieldName();

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

    /// a count aggregation is always on an uint 64
    this->onField = onField.withStamp(DataTypeProvider::provideDataType(LogicalType::UINT64)).get<FieldAccessFunction>();
    this->asField = asField.withStamp(DataTypeProvider::provideDataType(LogicalType::UINT64)).get<FieldAccessFunction>();
}

NES::SerializableAggregationFunction CountAggregationFunction::serialize() const
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

AggregationFunctionRegistryReturnType
AggregationFunctionGeneratedRegistrar::RegisterCountAggregationFunction(AggregationFunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.fields.size() == 2, "CountAggregationFunction requires exactly two fields, but got {}", arguments.fields.size());
    return CountAggregationFunction::create(arguments.fields[0], arguments.fields[1]);
}
}
