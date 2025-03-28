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

#ifndef INCLUDED_FROM_REGISTRY_LOGICAL_OPERATOR
#    error "This file should not be included directly! " \
"Include instead include <Operators/LogicalOperatorRegistry.hpp>"
#endif


namespace NES::LogicalOperatorGeneratedRegistrar
{
std::unique_ptr<LogicalOperator> selectionLogicalOperator();
std::unique_ptr<LogicalOperator> mapLogicalOperator();
std::unique_ptr<LogicalOperator> projectionLogicalOperator();
std::unique_ptr<LogicalOperator> joinLogicalOperator();
std::unique_ptr<LogicalOperator> aggregationLogicalOperator();
std::unique_ptr<LogicalOperator> unionLogicalOperator();

std::unique_ptr<LogicalOperator> deserializeSelectionLogicalOperator(const SerializableOperator& serializableOperator);
std::unique_ptr<LogicalOperator> deserializeMapLogicalOperator(const SerializableOperator& serializableOperator);
std::unique_ptr<LogicalOperator> deserializeProjectionLogicalOperator(const SerializableOperator& serializableOperator);
std::unique_ptr<LogicalOperator> deserializeJoinLogicalOperator(const SerializableOperator& serializableOperator);
std::unique_ptr<LogicalOperator> deserializeAggregationLogicalOperator(const SerializableOperator& serializableOperator);
std::unique_ptr<LogicalOperator> deserializeUnionLogicalOperator(const SerializableOperator& serializableOperator);
}

namespace NES
{
template <>
inline void Registrar<RegistryLogicalOperator, RegistrySignatureLogicalOperator>::registerAll([[maybe_unused]] Registry<Registrar>& registry)
{
    using namespace NES::LogicalOperatorGeneratedRegistrar;
    registry.registerPlugin("Selection", selectionLogicalOperator);
    registry.registerPlugin("Map", mapLogicalOperator);
    registry.registerPlugin("Projection", projectionLogicalOperator);
    registry.registerPlugin("Join", joinLogicalOperator);
    registry.registerPlugin("Aggregation", aggregationLogicalOperator);
    registry.registerPlugin("Union", unionLogicalOperator);
}

template <>
inline void Registrar<RegistryDeserializeLogicalOperator, RegistrySignatureDeserializeLogicalOperator>::registerAll([[maybe_unused]] Registry<Registrar>& registry)
{
    using namespace NES::LogicalOperatorGeneratedRegistrar;
    registry.registerPlugin("Selection", deserializeSelectionLogicalOperator);
    registry.registerPlugin("Map", deserializeMapLogicalOperator);
    registry.registerPlugin("Projection", deserializeProjectionLogicalOperator);
    registry.registerPlugin("Join", deserializeJoinLogicalOperator);
    registry.registerPlugin("Aggregation", deserializeAggregationLogicalOperator);
    registry.registerPlugin("Union", deserializeUnionLogicalOperator);
}
}
