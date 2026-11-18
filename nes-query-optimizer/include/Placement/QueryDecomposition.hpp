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
#include <Identifiers/Identifiers.hpp>
#include <Identifiers/NESStrongType.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Sinks/SinkCatalog.hpp>
#include <Sources/SourceCatalog.hpp>
#include <Util/Pointers.hpp>
#include <DistributedLogicalPlan.hpp>
#include <WorkerCatalog.hpp>

namespace NES
{

struct QueryOptimizerNetworkConfiguration;
using ChannelId = NESStrongUUIDType<struct ChannelId_>;

/// The network source and sink of a network channel. The sink has the upstream operator as its child.
struct NetworkOperators
{
    LogicalOperator source;
    LogicalOperator sink;
};

class QueryDecomposer
{
    SharedPtr<const WorkerCatalog> workerCatalog;
    SharedPtr<const SourceCatalog> sourceCatalog;
    SharedPtr<const SinkCatalog> sinkCatalog;

public:
    QueryDecomposer(
        SharedPtr<const WorkerCatalog> workerCatalog,
        SharedPtr<const SourceCatalog> sourceCatalog,
        SharedPtr<const SinkCatalog> sinkCatalog);

    DistributedLogicalPlan decompose(const LogicalPlan& placedPlan, const QueryOptimizerNetworkConfiguration& networkConfiguration) const;

    /// Creates the network sink on `upstreamWorker` (with `upstreamOperator` as its child) and the network source on `downstreamWorker` of
    /// the channel `channelId`. The incremental placement of ISQP uses this to connect re-placed operators.
    [[nodiscard]] NetworkOperators createNetworkOperators(
        const std::string& channelId,
        const LogicalOperator& upstreamOperator,
        const Host& upstreamWorker,
        const Host& downstreamWorker,
        const QueryOptimizerNetworkConfiguration& networkConfiguration) const;
};

}
