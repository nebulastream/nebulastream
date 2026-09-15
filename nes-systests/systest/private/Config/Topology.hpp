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

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include <Configurations/ConfigLiteral.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>

namespace NES
{

/// One worker of a topology file.
struct TopologyWorker
{
    /// The address the coordinator sends commands to, which is also the name of the worker in placements.
    Host host;
    /// The address the worker receives data from other workers on.
    std::string dataAddress;
    /// How many operators the worker takes. Not set means no limit.
    std::optional<uint64_t> maxOperators;
    /// The workers this one can send data to.
    std::vector<Host> downstream;
    /// Worker settings from the `config` block of the file.
    /// The settings of the command line and then of a test file override them.
    /// A worker in another process is already running, so they do not reach it.
    Schema<LiteralConfigValue, Ordered> config;
};

/// The content of a topology file: the workers, and the workers a source or a sink may be placed on.
/// The first entry of each list is the host of a source or sink that sets none.
struct ClusterConfiguration
{
    std::vector<TopologyWorker> workers;
    std::vector<Host> allowSourcePlacement;
    std::vector<Host> allowSinkPlacement;
};

}
