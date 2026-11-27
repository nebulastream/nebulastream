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

#include <optional>

#include <Config/Topology.hpp>
#include <Identifiers/Identifiers.hpp>

namespace NES::Testing
{

/// The smallest topology a run accepts, for tests that build their configuration in code.
inline ClusterConfiguration singleWorkerTopology()
{
    const Host host{"localhost:8080"};
    return ClusterConfiguration{
        .workers
        = {TopologyWorker{.host = host, .dataAddress = "localhost:9090", .maxOperators = std::nullopt, .downstream = {}, .config = {}}},
        .allowSourcePlacement = {host},
        .allowSinkPlacement = {host}};
}

}
