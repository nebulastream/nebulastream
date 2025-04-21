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
#include <utility>
#include <Abstract/LogicalFunction.hpp>
#include <ErrorHandling.hpp>
#include <SerializableFunction.pb.h>
#include <LogicalFunctionRegistry.hpp>
#include <LogicalFunctionRegistry.hpp>
#include <LogicalFunctionRegistry.hpp>
#include <Configurations/Descriptor.hpp>

namespace NES
{

LogicalFunction deserializeUnaryFunction(const SerializableFunction& serializedFunction)
{
    const auto& functionType = serializedFunction.functiontype();

    const auto& unaryFunction = serializedFunction.unaryfunction();
    auto child = FunctionSerializationUtil::deserializeFunction(unaryFunction.child());

    NES::Configurations::DescriptorConfig::Config functionDescriptorConfig{};
    for (const auto& [key, value] : serializedFunction.config())
    {
        functionDescriptorConfig[key] = Configurations::protoToDescriptorConfigType(value);
    }

    if (auto function = UnaryLogicalFunctionRegistry::instance().create(functionType, UnaryLogicalFunctionRegistryArguments(functionDescriptorConfig)))
    {
        function.value()->setChild(std::move(child));
        return std::move(function.value());
    }
    throw CannotDeserialize("Binary Logical Function: {}", serializedFunction.DebugString());
}

LogicalFunction deserializeBinaryFunction(const SerializableFunction& serializedFunction)
{
    const auto& functionType = serializedFunction.functiontype();

    const auto& unaryFunction = serializedFunction.binaryfunction();
    auto leftChild = FunctionSerializationUtil::deserializeFunction(unaryFunction.leftchild());
    auto rightChild = FunctionSerializationUtil::deserializeFunction(unaryFunction.rightchild());

    NES::Configurations::DescriptorConfig::Config functionDescriptorConfig{};
    for (const auto& [key, value] : serializedFunction.config())
    {
        functionDescriptorConfig[key] = Configurations::protoToDescriptorConfigType(value);
    }

    if (auto function
        = BinaryLogicalFunctionRegistry::instance().create(functionType, BinaryLogicalFunctionRegistryArguments(functionDescriptorConfig)))
    {
        function.value()->setLeftChild(std::move(leftChild));
        function.value()->setRightChild(std::move(rightChild));
        return std::move(function.value());
    }
    throw CannotDeserialize("Binary Logical Function: {}", serializedFunction.DebugString());
}

LogicalFunction FunctionSerializationUtil::deserializeFunction(const SerializableFunction& serializedFunction)
{
    if (serializedFunction.has_unaryfunction())
    {
        return deserializeUnaryFunction(serializedFunction);
    }
    else if (serializedFunction.has_binaryfunction())
    {
        return deserializeBinaryFunction(serializedFunction);
    }
    else if (serializedFunction.has_naryfunction())
    {
        throw UnsupportedOperation("Currently no support for N-ary functions");
    }
    throw CannotDeserialize("Logical Function: {}", serializedFunction.DebugString());
}

}
