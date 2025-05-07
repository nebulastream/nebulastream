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

#pragma once

#include <memory>
#include <LogicalOperators/Operator.hpp>

#include <LogicalOperators/Sinks/SinkOperator.hpp>
#include <SerializableOperator.pb.h>
#include <LogicalOperators/Windows/Aggregations/WindowAggregationFunction.hpp>
#include <Sources/SourceDescriptor.hpp>

namespace NES::Logical
{


/// The OperatorSerializationUtil offers functionality to serialize and deserialize logical operator trees to a corresponding protobuffer object.
class OperatorSerializationUtil
{
public:
    /// Deserializes the input SerializableOperator only
    /// Note: This method will not deserialize its children
    static Operator deserializeOperator(SerializableOperator serializedOperator);
    static std::unique_ptr<Sources::SourceDescriptor>
    deserializeSourceDescriptor(const SerializableSourceDescriptor& sourceDescriptor);
    static std::unique_ptr<Sinks::SinkDescriptor>
    deserializeSinkDescriptor(const SerializableSinkDescriptor& serializableSinkDescriptor);
    static std::shared_ptr<WindowAggregationFunction> deserializeWindowAggregationFunction(
        const SerializableAggregationFunction& serializedFunction);
};
}
