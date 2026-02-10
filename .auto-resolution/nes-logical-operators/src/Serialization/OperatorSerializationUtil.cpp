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

#include <Serialization/OperatorSerializationUtil.hpp>

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include <Configurations/Descriptor.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <Util/Reflection.hpp>
#include <ErrorHandling.hpp>
#include <LogicalOperatorRegistry.hpp>

namespace NES
{

ReflectedOperator OperatorSerializationUtil::serializeOperator(const TypedLogicalOperator<>& op)
{
    ReflectedOperator reflectedOperator;
    reflectedOperator.operatorId = op.getId().getRawValue();
    reflectedOperator.type = op.getName();
    reflectedOperator.inputSchemas = op.getInputSchemas();
    reflectedOperator.outputSchema = op.getOutputSchema();
    reflectedOperator.traitSet = op.getTraitSet();

    for (const auto& child : op.getChildren())
    {
        reflectedOperator.childrenIds.emplace_back(child.getId().getRawValue());
    }
    reflectedOperator.config = op->reflect();

    return reflectedOperator;
}

LogicalOperator OperatorSerializationUtil::deserializeOperator(const ReflectedOperator& serialized)
{
    const std::optional<LogicalOperator> result = [&] -> std::optional<LogicalOperator>
    {
        if (serialized.type == "Source")
        {
            return unreflect<SourceDescriptorLogicalOperator>(serialized.config);
        }

        if (serialized.type == "Sink")
        {
            return unreflect<SinkLogicalOperator>(serialized.config);
        }

        auto registryArgument
            = LogicalOperatorRegistryArguments{.inputSchemas = {}, .outputSchema = Schema(), .reflected = serialized.config};

        auto logicalOperatorOpt = LogicalOperatorRegistry::instance().create(serialized.type, registryArgument);

        if (!logicalOperatorOpt.has_value())
        {
            return std::nullopt;
        }

        auto logicalOperator = logicalOperatorOpt.value();

        logicalOperator = logicalOperator.withInferredSchema(serialized.inputSchemas);

        return std::make_optional(logicalOperator);
    }();

    if (result.has_value())
    {
        return result.value()->withTraitSet(serialized.traitSet);
    }

    throw CannotDeserialize("could not de-serialize operator {} of type {}", serialized.operatorId, serialized.type);
}
}
