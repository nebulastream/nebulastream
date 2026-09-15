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
#include <string_view>

#include <Runner/Topology.hpp>

namespace NES
{

/// Returns the `CREATE WORKER` option for the address the worker at this index serves data on.
/// The coordinator keys a worker by its data address and by its host address separately, so each worker needs a distinct pair.
std::string dataAddressOption(size_t index);

/// Returns the host address of the worker at this index.
/// The coordinator supplies the first worker's address and the rest count up from it, so a run configuring no worker registers only that one.
std::string workerHostAddress(std::string_view first, size_t index);

/// Builds the `CREATE WORKER` statement that registers a worker at an address with the given options.
std::string registerWorkerStatement(std::string_view hostAddress, std::string_view options);

/// Builds the `CREATE WORKER` statement for one worker a topology declares, carrying its data address, its capacity,
/// its downstream links and whatever else the topology configured it with.
std::string registerTopologyWorkerStatement(const TopologyWorker& worker);

}
