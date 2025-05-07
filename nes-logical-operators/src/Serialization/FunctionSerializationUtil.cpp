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

#include <AggregationFunctionRegistry.hpp>
#include <memory>
#include <vector>
#include <Configurations/Descriptor.hpp>
#include <LogicalFunctions/Function.hpp>
#include <Serialization/FunctionSerializationUtil.hpp>
#include <ErrorHandling.hpp>
#include <FunctionRegistry.hpp>
#include <SerializableFunction.pb.h>
#include <LogicalOperators/Windows/Aggregations/WindowAggregationFunction.hpp>
#include <Serialization/DataTypeSerializationUtil.hpp>

namespace NES::FunctionSerializationUtil
{

Logical::Function deserializeFunction(const SerializableFunction& serializedFunction)
{
    const auto& functionType = serializedFunction.functiontype();

    std::vector<Logical::Function> deserializedChildren;
    for (const auto& child : serializedFunction.children())
    {
        deserializedChildren.emplace_back(deserializeFunction(child));
    }

    auto stamp = DataTypeSerializationUtil::deserializeDataType(serializedFunction.stamp());

    NES::Configurations::DescriptorConfig::Config functionDescriptorConfig{};
    for (const auto& [key, value] : serializedFunction.config())
    {
        functionDescriptorConfig[key] = Configurations::protoToDescriptorConfigType(value);
    }

    auto argument = Logical::FunctionRegistryArguments(functionDescriptorConfig, deserializedChildren, stamp);

    if (auto function
        = Logical::FunctionRegistry::instance().create(functionType, argument))
    {
        return function.value();
    }
    throw CannotDeserialize("Logical Function: {}", serializedFunction.DebugString());
}

std::shared_ptr<Logical::WindowAggregationFunction> deserializeWindowAggregationFunction(
    const SerializableAggregationFunction& serializedFunction) {
    const auto& type = serializedFunction.type();
    auto onField = deserializeFunction(serializedFunction.on_field());
    auto asField = deserializeFunction(serializedFunction.as_field());

    if (auto fieldAccess = onField.tryGet<Logical::FieldAccessFunction>()) {
        if (auto asFieldAccess = asField.tryGet<Logical::FieldAccessFunction>()) {
            Logical::AggregationFunctionRegistryArguments args;
            args.fields = {fieldAccess.value(), asFieldAccess.value()};
            
            if (auto function = Logical::AggregationFunctionRegistry::instance().create(type, args)) {
                return function.value();
            }
        }
    }
    throw UnknownLogicalOperator();
}

}
