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
#include <Plans/LogicalPlan.hpp>
#include <StatisticTuple.hpp>
#include <StatisticQueryGenerator.hpp>

namespace NES
{

struct RequestStatisticBuildStatement;

/// Builds the query that collects a statistic.
///
/// Only the DataDomain / scalar slice is implemented. WorkloadDomain and InfrastructureDomain keep their arm in
/// the dispatch and throw NotImplemented, as does every metric other than Average -- the synopsis statistics
/// (equi-width histogram, count-min sketch, reservoir sample) they would map to are not part of this port.
///
/// The generated plan is deliberately thin:
///
///   Source -> [watermark] -> WindowedAggregation(ScalarStatistic Avg) -> GrpcSink
///
/// The StatisticStoreWriter does not appear: it is fused into the aggregation's lowering rule. No schema is built
/// by hand anywhere -- the source schema comes from the catalog and the sink infers its own -- which keeps this
/// file clear of the bound/unbound field machinery entirely.
class DefaultStatisticQueryGenerator final : public StatisticQueryGenerator
{
public:
    [[nodiscard]] LogicalPlan generateQuery(
        const RequestStatisticBuildStatement& request,
        StatisticTuple::StatisticId statisticId,
        const std::string& interfaceAddress) const override;
};

}
