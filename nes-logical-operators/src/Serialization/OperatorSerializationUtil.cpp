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

#include <algorithm>
#include <cctype>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <variant>
#include <vector>

#include <Configurations/Descriptor.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <Serialization/SchemaSerializationUtil.hpp>
#include <Serialization/SerializedData.hpp>
#include <Serialization/TraitSetSerializationUtil.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <Sources/LogicalSource.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Traits/TraitSet.hpp>
#include <Util/Reflection.hpp>
#include <ErrorHandling.hpp>
#include <LogicalOperatorRegistry.hpp>
#include <SerializableOperator.pb.h>

namespace NES
{


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
            = LogicalOperatorRegistryArguments{.inputSchemas = {}, .outputSchema = Schema(), .config = {}, .reflected = serialized.config};

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
        return result.value();
    }

    throw CannotDeserialize("could not de-serialize this serialized operator {} of type ", serialized.operatorId, serialized.type);
}
}
