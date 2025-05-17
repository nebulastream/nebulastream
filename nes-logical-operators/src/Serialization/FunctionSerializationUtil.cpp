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
#include <vector>
#include <Configurations/Descriptor.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Operators/Windows/Aggregations/WindowAggregationLogicalFunction.hpp>
#include <Serialization/DataTypeSerializationUtil.hpp>
#include <Serialization/FunctionSerializationUtil.hpp>
#include <AggregationLogicalFunctionRegistry.hpp>
#include <ErrorHandling.hpp>
#include <LogicalFunctionRegistry.hpp>
#include <SerializableVariantDescriptor.pb.h>

namespace NES::FunctionSerializationUtil
{

LogicalFunction deserializeFunction(const SerializableFunction& serializedFunction)
{
    const auto& functionType = serializedFunction.function_type();

    std::vector<LogicalFunction> deserializedChildren;
    for (const auto& child : serializedFunction.children())
    {
        deserializedChildren.emplace_back(deserializeFunction(child));
    }

    auto dataType = DataTypeSerializationUtil::deserializeDataType(serializedFunction.data_type());

    NES::Configurations::DescriptorConfig::Config functionDescriptorConfig{};
    for (const auto& [key, value] : serializedFunction.config())
    {
        functionDescriptorConfig[key] = Configurations::protoToDescriptorConfigType(value);
    }

    auto argument = LogicalFunctionRegistryArguments(functionDescriptorConfig, deserializedChildren, dataType);

    if (auto function = LogicalFunctionRegistry::instance().create(functionType, argument))
    {
        return function.value();
    }
    throw CannotDeserialize("Logical Function: {}", serializedFunction.DebugString());
}

std::shared_ptr<WindowAggregationLogicalFunction>
deserializeWindowAggregationFunction(const SerializableAggregationFunction& serializedFunction)
{
    const auto& type = serializedFunction.type();
    auto onField = deserializeFunction(serializedFunction.on_field());
    auto asField = deserializeFunction(serializedFunction.as_field());

    if (auto fieldAccess = onField.tryGet<FieldAccessLogicalFunction>())
    {
        if (auto asFieldAccess = asField.tryGet<FieldAccessLogicalFunction>())
        {
            AggregationLogicalFunctionRegistryArguments args;
            args.fields = {fieldAccess.value(), asFieldAccess.value()};

            if (auto function = AggregationLogicalFunctionRegistry::instance().create(type, args))
            {
                return function.value();
            }
        }
    }
    throw UnknownLogicalOperator();
}

}
