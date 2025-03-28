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

#ifndef INCLUDED_FROM_REGISTRY_REWRITE_RULE
#    error "This file should not be included directly! " \
"Include instead include <Operators/LogicalOperatorRegistry.hpp>"
#endif


namespace NES::LogicalOperatorGeneratedRegistrar
{
std::unique_ptr<LogicalOperator> RegisterSelectionLogicalOperator();
std::unique_ptr<LogicalOperator> RegisterMapLogicalOperator();
std::unique_ptr<LogicalOperator> RegisterProjectionLogicalOperator();
std::unique_ptr<LogicalOperator> RegisterJoinLogicalOperator();
std::unique_ptr<LogicalOperator> RegisterAggregationLogicalOperator();
}

namespace NES
{
template <>
inline void Registrar<LogicalOperatorRegistry, RewriteRuleRegistrySignature>::registerAll([[maybe_unused]] Registry<Registrar>& registry)
{
    using namespace NES::LogicalOperatorGeneratedRegistrar;
    registry.registerPlugin("Selection", RegisterSelectionLogicalOperator);
    registry.registerPlugin("Map", RegisterMapLogicalOperator);
    registry.registerPlugin("Projection", RegisterProjectionLogicalOperator);
    registry.registerPlugin("Join", RegisterJoinLogicalOperator);
    registry.registerPlugin("Aggregation", RegisterAggregationLogicalOperator);
}
}
