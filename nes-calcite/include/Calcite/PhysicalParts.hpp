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

#include <functional>
#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>
#include <Identifiers/Identifiers.hpp>
#include <Schema/Schema.hpp>
#include <Sinks/SinkCatalog.hpp>
#include <Sources/SourceCatalog.hpp>
#include <ModelCatalog.hpp>
#include <PipelinedQueryPlan.hpp>
#include <QueryExecutionConfiguration.hpp>
#include <QueryId.hpp>

namespace substrait
{
class Plan;
}

/// The parts of a placed physical plan (PhysicalOperator payloads, see NesPhysical.java): the coordinator resolves each
/// worker's part from its catalogs into a self-contained WorkerPart, and the worker builds its pipelines from that alone.
namespace NES::Calcite
{

/// Binds the sink clause of a physical plan, given the query's output schema: a sink of the catalog, or an anonymous sink
/// of that schema.
using BindSink = std::function<SinkDescriptor(const Schema<UnqualifiedUnboundField, Ordered>&)>;

/// What resolving the parts of a placed physical plan needs besides the plan: the catalogs (for the sources and the network
/// sources and sinks), the sink clause, each worker's data address for its network channels, and the models.
struct PhysicalPlanContext
{
    const SourceCatalog& sourceCatalog;
    const SinkCatalog& sinkCatalog;
    BindSink bindSink;
    std::unordered_map<Host, std::string> dataAddresses;
    /// For model inference: the models, and the catalog name of each name the plan uses (systest's unprefixed names).
    const ModelCatalog* models = nullptr;
    std::unordered_map<std::string, std::string> catalogNames;
};

/// The rows that the sink of a placed physical plan receives.
Schema<UnqualifiedUnboundField, Ordered> sinkInputSchema(const substrait::Plan& plan);

/// The part of the root `relation` for the worker that runs it, as a serialized WorkerPart: the relation with the plan's
/// extensions, and the source and sink descriptors and models it reads, resolved from the catalogs of `context`.
std::string resolveWorkerPart(const substrait::Plan& plan, int relation, const PhysicalPlanContext& context, const Host& worker);

/// Builds the pipelines of a serialized WorkerPart on the worker that runs it, with the worker's configuration, e.g. its
/// buffer size.
std::shared_ptr<PipelinedQueryPlan> toPipelines(std::string_view workerPart, QueryId queryId, const QueryExecutionConfiguration& configuration);

}
