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

#include <LegacyOptimizer/OperatorPlacement.hpp>

#include <optional>

#include <Operators/LogicalOperator.hpp>
#include <Traits/PlacementTrait.hpp>
#include <Traits/TraitSet.hpp>
#include <ErrorHandling.hpp>
#include <NetworkTopology.hpp>

namespace NES
{

Topology::NodeId getPlacementFor(const LogicalOperator& op)
{
    auto placement = getTrait<PlacementTrait>(op.getTraitSet())
                         .transform([](const auto& placementTrait) { return Topology::NodeId(placementTrait->onNode); });
    if (!placement.has_value())
    {
        throw PlacementFailure("Operator does not have a placement trait");
    }
    return placement.value();
}

}
