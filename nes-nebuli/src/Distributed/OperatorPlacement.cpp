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

#include <Distributed/OperatorPlacement.hpp>

#include <optional>

#include <Distributed/NetworkTopology.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Traits/PlacementTrait.hpp>
#include <Traits/Trait.hpp>

namespace NES
{

OperatorPlacer::OperatorPlacer(const TopologyGraph& topology, WorkerCatalog& nodeCatalog) : topology{topology}, nodeCatalog{nodeCatalog}
{
}

std::optional<TopologyGraph::NodeId> getPlacementFor(const LogicalOperator& op)
{
    return getTrait<PlacementTrait>(op.getTraitSet())
        .transform([](const PlacementTrait& placement) -> TopologyGraph::NodeId { return placement.onNode; });
}

LogicalOperator placeOperatorOnNode(const LogicalOperator& op, const TopologyGraph::NodeId& nodeId)
{
    return addAdditionalTraits(op, {PlacementTrait{nodeId}});
}

}
