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

#include <cstddef>
#include <string>
#include <variant>
#include <vector>
#include <Configurations/ConfigLiteral.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>

namespace NES
{

namespace CapacityKind
{
struct Unlimited
{
};

struct Limited
{
    size_t value;
};
}

using Capacity = std::variant<CapacityKind::Unlimited, CapacityKind::Limited>;

struct WorkerCatalogEntry
{
    Host host; /// gRPC management endpoint, used as primary worker identity
    std::string dataAddress; /// Data-plane address for network sources/sinks (set via --data_address)
    Capacity maxOperators;
    std::vector<Host> downstream;
    /// Worker config literals from the topology file. They are merged with the CLI literals
    /// (CLI wins) and resolved against SingleNodeWorkerConfiguration's declared schema when the
    /// worker is instantiated.
    Schema<LiteralConfigValue, Ordered> config;
};

}
