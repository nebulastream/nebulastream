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
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <Identifiers/Identifier.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Sinks/SinkCatalog.hpp>
#include <Sources/SourceCatalog.hpp>
#include <PhysicalQuery.hpp>
#include <WorkerCatalog.hpp>
#include <PipelinedQueryPlan.hpp>
#include <QueryExecutionConfiguration.hpp>

namespace substrait
{
class Plan;
}

/// Plans queries with Apache Calcite through the nes-calcite-planner service (see nes-calcite-planner/README.md).
namespace NES::Calcite
{

/// Address of the planner service: $NES_CALCITE_PLANNER, or localhost:50061.
std::string plannerAddress();

/// $NES_CALCITE_PHYSICAL=1: plan down to physical operators (planPhysical) instead of a logical plan for our optimizer.
bool physicalPlans();

/// $NES_CALCITE_FINITE_INPUTS=1: all sources are finite, so joins and aggregations without a window get one window over the
/// whole input that fires at the end of the stream. Off by default; without it Calcite rejects them.
bool finiteInputs();

/// Splits "<query> INTO <sink>" into the query and the sink clause. The query is sent to Calcite, the sink stays ours: a
/// sink name or an anonymous sink such as File('out.csv' AS "SINK".FILE_PATH, ...), in our SQL dialect.
std::pair<std::string, std::string> splitSink(std::string_view statement);

/// Plans a single SELECT in Calcite's SQL dialect against the schemas of all logical sources in `catalog`.
/// Returns the same kind of unbound plan the ANTLR parser produces, writing into `sinkClause` (see splitSink).
/// Throws InvalidQuerySyntax if Calcite rejects the query and UnsupportedQuery if the service is unreachable or the plan
/// contains something we cannot convert.
/// sourceNames: if not empty, only these catalog sources are offered to Calcite, under the given names
/// (catalog name -> name in the SQL); e.g. systest offers each test file's sources under their unprefixed names.
LogicalPlan plan(
    std::string_view sql,
    std::string_view sinkClause,
    const SourceCatalog& catalog,
    const std::string& address,
    bool finiteInputs = false,
    const std::unordered_map<std::string, std::string>& sourceNames = {});

/// Binds the sink clause of a physical plan, given the query's output schema: a sink of the catalog, or an anonymous sink
/// of that schema.
using BindSink = std::function<SinkDescriptor(const Schema<UnqualifiedUnboundField, Ordered>&)>;

/// What building a placed physical plan needs besides the plan: the catalogs (for the network sources and sinks), the sink
/// clause, and each worker's data address, for its network channels.
struct PhysicalPlanContext
{
    const SourceCatalog& sourceCatalog;
    const SinkCatalog& sinkCatalog;
    BindSink bindSink;
    std::unordered_map<Host, std::string> dataAddresses;
};

/// Plans a single SELECT with Calcite down to its pipelines and places them on the workers of `workerCatalog`; we neither
/// optimize, place nor pipeline it, each worker only compiles its part (see PhysicalQuery). The sink clause is bound with
/// `sinkCatalog`; both catalogs must outlive the workers' start of the query. Errors as for plan().
PlacedPhysicalQuery planPhysical(
    std::string_view sql,
    std::string_view sinkClause,
    const SourceCatalog& sourceCatalog,
    const SinkCatalog& sinkCatalog,
    const WorkerCatalog& workerCatalog,
    const std::string& address,
    bool finiteInputs = false,
    const std::unordered_map<std::string, std::string>& sourceNames = {});

/// Builds the pipelines of the parts of a placed physical plan (PhysicalOperator payloads) that run on `worker`, with the
/// worker's configuration, e.g. its buffer size.
std::shared_ptr<PipelinedQueryPlan> toPipelines(
    const substrait::Plan& plan,
    const PhysicalPlanContext& context,
    const Host& worker,
    QueryId queryId,
    const QueryExecutionConfiguration& configuration);

/// Converts a Substrait plan from the planner service into an unbound logical plan without a sink.
/// finiteInputs: constant rows (VALUES) may be derived from the sources, see the planner's finite_inputs. They use a source
/// the plan reads, or fallbackSource if it reads none. catalogNames maps source names in the plan to catalog names.
LogicalPlan toLogicalPlan(
    const substrait::Plan& plan,
    bool finiteInputs = false,
    std::optional<std::string> fallbackSource = std::nullopt,
    std::unordered_map<std::string, std::string> catalogNames = {});

}
