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

#include <Serialization/FunctionSerializationUtil.hpp>
#include <memory>
#include <vector>
#include <Functions/LogicalFunction.hpp>
#include <ErrorHandling.hpp>
#include <SerializableFunction.pb.h>
#include <LogicalFunctionRegistry.hpp>
#include <LogicalFunctionRegistry.hpp>
#include <LogicalFunctionRegistry.hpp>
#include <Configurations/Descriptor.hpp>

namespace NES::FunctionSerializationUtil
{

LogicalFunction deserializeFunction(const SerializableFunction& serializedFunction)
{
    const auto& functionType = serializedFunction.functiontype();

    std::vector<LogicalFunction> deserializedChildren;
    for (const auto& child : serializedFunction.children()) {
        deserializedChildren.emplace_back(deserializeFunction(child));
    }

    NES::Configurations::DescriptorConfig::Config functionDescriptorConfig{};
    for (const auto& [key, value] : serializedFunction.config())
    {
        functionDescriptorConfig[key] = Configurations::protoToDescriptorConfigType(value);
    }

    if (auto function = LogicalFunctionRegistry::instance().create(functionType, LogicalFunctionRegistryArguments(functionDescriptorConfig)))
    {
        return function.value().withChildren(deserializedChildren);
    }
    throw CannotDeserialize("Logical Function: {}", serializedFunction.DebugString());
}

}
