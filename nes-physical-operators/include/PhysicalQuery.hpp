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
#include <unordered_map>
#include <Identifiers/Identifiers.hpp>
#include <QueryId.hpp>

namespace NES
{
struct PipelinedQueryPlan;
struct QueryExecutionConfiguration;

/// A query planned down to its pipelines, e.g. by the Calcite frontend: the worker neither optimizes, lowers nor
/// pipelines it, it only compiles the pipelines. They are built with the worker's configuration (e.g. its buffer size).
struct PhysicalQuery
{
    QueryId queryId;
    std::function<std::shared_ptr<PipelinedQueryPlan>(QueryId, const QueryExecutionConfiguration&)> pipelines;
};

/// A physical query placed on workers: the part each worker runs.
using PlacedPhysicalQuery = std::unordered_map<Host, PhysicalQuery>;

}
