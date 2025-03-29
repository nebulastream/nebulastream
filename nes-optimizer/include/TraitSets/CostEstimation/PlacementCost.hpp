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

#include <fmt/core.h>
#include "../CardEstimator.hpp"
#include "../TraitSet.hpp"
#include "../Traits/QueryForSubtree.hpp"

namespace NES
{

class PlacementCost
{
    CostFunction<TraitSet<QueryForSubtree>, double> cardEstimator;

    double apply(TraitSet<ChildNodes, QueryForSubtree, Placement> node)
    {
        double movement = 0f;
        for (TraitSet<ChildNodes, QueryForSubtree, Placement> child : node->get<ChildNodes>()->getChildren())
        {
            if (child->get<Placement>() != node->get<Placement>())
            {
                /// As they are not on the same node whe need some movement.
                movement += cardEstimator->apply(child);
            }
        }
        return movement;
    }

    constexpr RewriteRule<TraitSet<Trait...>, TraitSet<Trait...>> derive() { return optimizer::subset; }

    constexpr RewriteRule<TraitSet<ChildNodes, QueryAtNode, Placement>, TraitSet<ChildNodes, QueryForSubtree, Placement>> derive()
    {
        return optimizer::collectQueriesAtNode;
    }
}

}
