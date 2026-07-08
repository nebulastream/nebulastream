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

#include <Placement/FaultTolerancePlacement.hpp>

#include <Operators/FaultTolerance/SNDeduplicationLogicalOperator.hpp>
#include "Operators/Sources/SourceDescriptorLogicalOperator.hpp"

namespace NES
{

#include <uuid/uuid.h>

static std::string generate_uuid()
{
    uuid_t bin;
    uuid_generate_random(bin);
    char str[37];
    uuid_unparse_lower(bin, str);
    return std::string(str);
}

static LogicalOperator dfs(LogicalOperator& root)
{
    // TODO: check that there are no problems with running this after semantic analysis
    if (auto source = root.tryGetAs<SourceDescriptorLogicalOperator>())
    {
        if (source.value()->getSourceDescriptor().getSourceType() == Identifier::parse("Network").asCanonicalString())
        {
            // TODO set up better file paths?
            return SNDeduplicationLogicalOperator(WeakLogicalOperator{}, generate_uuid())
                .withChildren({root})
                .withTraitSet(root.getTraitSet());
        }
        return root;
    }
    return root.withChildren(
        root.getChildren() | std::views::transform([&](LogicalOperator& child) -> LogicalOperator { return dfs(child); })
        | std::ranges::to<std::vector>());
}

DistributedLogicalPlan FTPlacer::apply(DistributedLogicalPlan& distributedPlan)
{
    std::unordered_map<Host, std::vector<LogicalPlan>> updatedPlans;
    for (auto [host, localPlans] : distributedPlan)
    {
        updatedPlans.insert({host, std::vector<LogicalPlan>()});
        for (auto& localPlan : localPlans)
        {
            updatedPlans[host].push_back(localPlan.withRootOperators(
                localPlan.getRootOperators() | std::views::transform([&](LogicalOperator& child) -> LogicalOperator { return dfs(child); })
                | std::ranges::to<std::vector>()));
        }
    }

    return DistributedLogicalPlan{std::move(updatedPlans), std::move(distributedPlan.getGlobalPlan())};
}
}
