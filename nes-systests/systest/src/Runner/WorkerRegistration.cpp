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

#include <Runner/WorkerRegistration.hpp>

#include <cstddef>
#include <string>
#include <string_view>
#include <vector>

#include <fmt/format.h>

#include <Rewriter/Constants.hpp>
#include <Runner/Topology.hpp>

namespace NES
{
namespace
{

/// The option keys a `CREATE WORKER` recognizes for the worker itself.
/// Anything else it is given configures the worker rather than describing it in the topology.
constexpr std::string_view dataAddressKey = "DATA";
constexpr std::string_view capacityKey = "CAPACITY";
constexpr std::string_view downstreamKey = "DOWNSTREAM";

}

std::string dataAddressOption(const size_t index)
{
    return fmt::format("{} AS {}", Sql::stringLiteral(fmt::format("localhost:{}", 9090 + index)), dataAddressKey);
}

/// Returns the host address of the worker at this index.
/// The coordinator supplies the first worker's address and the rest count up from it, so a run configuring no worker registers only that one.
std::string workerHostAddress(const std::string_view first, const size_t index)
{
    if (index == 0)
    {
        return std::string{first};
    }
    return fmt::format("localhost:{}", 8080 + index);
}

/// Builds the `CREATE WORKER` statement that registers a worker at an address with the given options.
std::string registerWorkerStatement(const std::string_view hostAddress, const std::string_view options)
{
    return fmt::format("CREATE WORKER {} SET ({})", Sql::stringLiteral(hostAddress), options);
}

std::string registerTopologyWorkerStatement(const TopologyWorker& worker)
{
    std::vector<std::string> options;
    if (not worker.dataAddress.empty())
    {
        options.push_back(fmt::format("{} AS {}", Sql::stringLiteral(worker.dataAddress), dataAddressKey));
    }
    if (worker.maxOperators.has_value())
    {
        options.push_back(fmt::format("{} AS {}", *worker.maxOperators, capacityKey));
    }
    for (const auto& downstream : worker.downstream)
    {
        options.push_back(fmt::format("{} AS {}", Sql::stringLiteral(downstream.view()), downstreamKey));
    }
    for (const auto& [key, value] : worker.config)
    {
        options.push_back(fmt::format("{} AS {}", Sql::stringLiteral(value), key));
    }
    return registerWorkerStatement(worker.host.view(), Sql::optionList(options));
}

}
