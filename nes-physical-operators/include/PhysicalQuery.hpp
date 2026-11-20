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
#include <unordered_map>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <QueryId.hpp>

namespace NES
{

/// A query planned down to its pipelines by the Calcite frontend: the worker neither optimizes, lowers nor pipelines it.
/// It builds the pipelines from the part, with its own configuration (e.g. its buffer size), and compiles them.
struct PhysicalQuery
{
    QueryId queryId;
    /// The worker's part of the plan, a serialized WorkerPart (see Calcite/PhysicalParts.hpp): self-contained, so that it
    /// can be sent to a remote worker.
    std::string part;
};

/// A physical query placed on workers: the parts each worker runs, each a query of its own there. A worker's engine runs one
/// sink per query, and a worker can hold several parts, e.g. one per channel to another worker.
using PlacedPhysicalQuery = std::unordered_map<Host, std::vector<PhysicalQuery>>;

}
