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
#include <unordered_map>
#include <vector>

#include <Identifiers/Identifiers.hpp>

namespace NES
{

/// One worker a topology declares, in the terms `CREATE WORKER` takes.
/// The config is flattened to the dotted keys a worker matches its options by.
struct TopologyWorker
{
    Host host;
    std::string dataAddress;
    std::optional<uint64_t> maxOperators;
    std::vector<Host> downstream;
    std::unordered_map<std::string, std::string> config;
};

/// The topology a run places its sources and sinks on.
/// Empty workers mean the invocation gave no topology, and the run uses the one worker the coordinator starts for itself.
struct ClusterConfiguration
{
    std::vector<TopologyWorker> workers;
    std::vector<Host> allowSourcePlacement;
    std::vector<Host> allowSinkPlacement;
};

}
