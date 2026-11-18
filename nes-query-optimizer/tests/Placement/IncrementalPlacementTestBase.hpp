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

#include <optional>
#include <string>
#include <Identifiers/Identifiers.hpp>
#include <Operators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <Placement/BottomUpPlacement.hpp>
#include <Placement/IncrementalPlacementUtils.hpp>
#include <Plans/LogicalPlan.hpp>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>
#include <DistributedTestCluster.hpp>
#include <ErrorHandling.hpp>
#include <GlobalExecutionPlan.hpp>
#include <OperatorGraph.hpp>
#include <QueryId.hpp>
#include <SharedQueryPlan.hpp>

namespace NES
{

/// Topology of the mobility scenario of ISQP: leafs connect to one of two base stations, which connect to the cloud.
///
///   leaf1 ---> base1 ---> cloud
///   leaf2 --/            /
///   leaf3 --/  base2 ---/
///
/// Leafs have no capacity, base stations have two slots each, the cloud is unlimited.
class IncrementalPlacementTestBase : public Testing::BaseUnitTest
{
public:
    void SetUp() override
    {
        BaseUnitTest::SetUp();
        cluster.addWorker("cloud:1", {});
        cluster.addWorker("base1:1", {"cloud:1"}, 2);
        cluster.addWorker("base2:1", {"cloud:1"}, 2);
        for (const auto* leaf : {"leaf1:1", "leaf2:1", "leaf3:1"})
        {
            cluster.addWorker(leaf, {"base1:1"}, 0);
        }
        cluster.addSource("src1", {"a", "b"}, "leaf1:1");
        cluster.addSource("src2", {"a", "b"}, "leaf2:1");
        cluster.addSource("src3", {"a", "b"}, "leaf3:1");
        cluster.addSink("snk", {"a", "b"}, "cloud:1");
    }

    /// Places the query with the initial placement, registers it and occupies the slots of its operators.
    void deploy(const std::string& sql)
    {
        const auto plan = cluster.optimize(sql);
        globalExecutionPlan.addQuery(queryId, plan);
        sharedQueryPlan.emplace(queryId, plan.getGlobalPlan(), getPlacement(plan.getGlobalPlan()));
        const OperatorGraph graph(plan.getGlobalPlan());
        for (const auto& operatorId : graph.getAllOperators())
        {
            cluster.workerCatalog->occupySlots(
                *sharedQueryPlan->getPinnedWorker(operatorId), getOperatorCapacityDemand(graph.getOperator(operatorId)));
        }
    }

    /// Moves a leaf from base1 to base2 and marks the operators to re-place, like the ISQP request does for a removed link.
    PinnedOperators moveLeaf(const std::string& leaf)
    {
        EXPECT_TRUE(cluster.workerCatalog->removeLink(Host(leaf), Host("base1:1")));
        cluster.workerCatalog->addLink(Host(leaf), Host("base2:1"));
        auto pinned = findUpstreamAndDownstreamPinnedOperators(
            *sharedQueryPlan, globalExecutionPlan, Host(leaf), Host("base1:1"), cluster.workerCatalog->getTopology());
        sharedQueryPlan->performReOperatorPlacement(pinned.upstreamOperators, pinned.downstreamOperators);
        return pinned;
    }

    [[nodiscard]] OperatorId sourceOn(const std::string& worker) const
    {
        for (const auto& source : getOperatorByType<SourceDescriptorLogicalOperator>(sharedQueryPlan->getPlan()))
        {
            if (source->getSourceDescriptor().getHost() == Host(worker))
            {
                return source.getId();
            }
        }
        throw TestException("No source on {}", worker);
    }

    template <typename T>
    [[nodiscard]] OperatorId single() const
    {
        const auto operators = getOperatorByType<T>(sharedQueryPlan->getPlan());
        if (operators.size() != 1)
        {
            throw TestException("Expected a single operator, found {}", operators.size());
        }
        return operators.front().getId();
    }

    [[nodiscard]] DecomposedQueryId planOn(const std::string& worker) const
    {
        const auto plans = globalExecutionPlan.getDecomposedQueryPlans(queryId, Host(worker));
        if (plans.size() != 1)
        {
            throw TestException("Expected a single plan on {}, found {}", worker, plans.size());
        }
        return plans.front().id;
    }

    DistributedTestCluster cluster;
    GlobalExecutionPlan globalExecutionPlan;
    std::optional<SharedQueryPlan> sharedQueryPlan;
    const DistributedQueryId queryId{"q"};
};

}
