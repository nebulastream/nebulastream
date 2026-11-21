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

#include <Calcite/QueryOptimizerConfiguration.hpp>
#include <Configurations/Util.hpp>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>
#include <Identifiers/Identifier.hpp>
#include <Schema/Schema.hpp>
#include <Sinks/SinkCatalog.hpp>
#include <Sources/SourceCatalog.hpp>
#include <PhysicalQuery.hpp>
#include <Calcite/PhysicalParts.hpp>
#include <WorkerCatalog.hpp>
#include <ModelCatalog.hpp>
#include <PipelinedQueryPlan.hpp>
#include <QueryExecutionConfiguration.hpp>

namespace substrait
{
class Plan;
}

/// Plans queries with Apache Calcite: the nes-calcite-planner, compiled into a native library (see nes-calcite-planner/README.md).
namespace NES::Calcite
{

/// For inspecting the planner: with $NES_CALCITE_DUMP_REQUESTS set to a directory, the requests to the planner are written
/// there, each as `<label>.pb` (a serialized PlanRequest), with the label this thread set last (e.g. systest's query id).
/// nes-calcite-planner/docs/tools/generate_systest_steps.sh shows the planner's steps for them.
void setRequestLabel(std::string label);

/// Splits "<query> INTO <sink>" into the query and the sink clause. The query is sent to Calcite, the sink stays ours: a
/// sink name or an anonymous sink such as File('out.csv' AS "SINK".FILE_PATH, ...), in our SQL dialect.
std::pair<std::string, std::string> splitSink(std::string_view statement);

/// A step of the planner (PlanStep): its name, who does it (origin) and the plan after it. With detailed steps, also the
/// plan as a Graphviz graph (DOT) and, for a step that applies rules, the plan after each rule that changed it.
struct PlanStep
{
    struct Rule
    {
        std::string rule;
        /// One of our rules, not one of Calcite's.
        bool ours;
        std::string plan;
        std::string dot;
    };

    std::string name;
    std::string origin;
    std::string plan;
    std::string dot;
    std::vector<Rule> rules;
};

/// A query that Calcite planned down to its pipelines and placed on workers (planPhysical).
struct PlannedPhysicalQuery
{
    /// The part of each worker.
    PlacedPhysicalQuery parts;
    /// The rows that the sink writes: its own schema if it has one (a named sink's, or an anonymous sink's that gives one),
    /// else the query's result.
    Schema<UnqualifiedUnboundField, Ordered> sinkInputSchema;
    /// The sink's type, e.g. File or Checksum.
    std::string sinkType;
    /// The placed pipelines as the planner describes them, per worker (for EXPLAIN).
    std::string explain;
    /// The planner's steps (for EXPLAIN VERBOSE, and with detailed steps for the HTML of nes-calcite-planner/docs/tools).
    std::vector<PlanStep> steps;
};

/// Plans a single SELECT with Calcite down to its pipelines and places them on the workers of `workerCatalog`; we neither
/// optimize, place nor pipeline it, each worker builds and compiles its part (see PhysicalQuery, WorkerPart). The sink
/// clause is bound with `sinkCatalog`. A named sink's schema must have the fields of
/// the query's result (CannotInferSchema otherwise). nestedLoopJoins: every window join is a nested-loop join (as our
/// optimizer's join_strategy NESTED_LOOP_JOIN).
/// sourceNames: if not empty, only these catalog sources are offered to Calcite, under the given names (catalog name ->
/// name in the SQL); e.g. systest offers each test file's sources under their unprefixed names.
/// Throws InvalidQuerySyntax if Calcite rejects the query, UnsupportedQuery if the query uses something we cannot
/// run, and PlacementFailure if it cannot be placed on the workers.
PlannedPhysicalQuery planPhysical(
    std::string_view sql,
    std::string_view sinkClause,
    const SourceCatalog& sourceCatalog,
    const SinkCatalog& sinkCatalog,
    const WorkerCatalog& workerCatalog,
    const std::unordered_map<std::string, std::string>& sourceNames = {},
    const ModelCatalog* models = nullptr,
    const QueryOptimizerConfiguration& optimizer = defaultConfiguration<QueryOptimizerConfiguration>(),
    bool detailedSteps = false);

/// Plans a query statement, "SELECT ... INTO <sink>", as planPhysical does, with the optimizer configuration (e.g. its finite_inputs).
PlannedPhysicalQuery planStatement(
    std::string_view statement,
    const SourceCatalog& sourceCatalog,
    const SinkCatalog& sinkCatalog,
    const WorkerCatalog& workerCatalog,
    const ModelCatalog* models,
    const QueryOptimizerConfiguration& optimizer,
    bool detailedSteps = false);


}
