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

#include <string>
#include <Operators/LogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <CollectionDomain.hpp>
#include <ErrorHandling.hpp>
#include <StatisticTuple.hpp>

namespace NES
{

/// Forward-declared to keep this interface header decoupled from the concrete statement definition.
/// Including RequestStatisticBuildStatement.hpp would pull in:
///   RequestStatisticBuildStatement.hpp --> CollectionDomain.hpp --> Metric.hpp --> StatisticTuple.hpp
/// This header only needs the type for a const reference parameter.
struct RequestStatisticBuildStatement;

/// Abstract interface for generating statistic collection queries.
/// Allows swapping the generator (e.g., for testing with a mock, or a future cost-based generator).
class StatisticQueryGenerator
{
public:
    virtual ~StatisticQueryGenerator() = default;

    /// Generates a LogicalPlan that builds a statistic and writes it to the StatisticStore.
    /// The statisticId is provided by the StatisticInterface and uniquely identifies this statistic.
    [[nodiscard]] virtual LogicalPlan generateQuery(
        const RequestStatisticBuildStatement& request, StatisticTuple::StatisticId statisticId, const std::string& interfaceAddress) const
        = 0;

    /// Builds the "build branch" sub-plan for a WorkloadDomain statistic: a chain rooted at the
    /// gRPC sink stacked on top of `spliceLeaf` (the data query's source operator). The caller
    /// then merges the returned plan's roots into the data query's plan so the optimizer's
    /// LogicalSourceExpansionRule produces a single shared Union(SourceDescriptors). Default impl
    /// throws NotImplemented — generators that don't support WorkloadDomain can leave it that way.
    [[nodiscard]] virtual LogicalPlan generateWorkloadBranch(
        const WorkloadDomain& domain,
        const RequestStatisticBuildStatement& request,
        StatisticTuple::StatisticId statisticId,
        const std::string& interfaceAddress,
        const LogicalOperator& spliceLeaf) const
    {
        (void)domain;
        (void)request;
        (void)statisticId;
        (void)interfaceAddress;
        (void)spliceLeaf;
        throw NotImplemented("This StatisticQueryGenerator does not support WorkloadDomain build-branch generation");
    }

};

}
