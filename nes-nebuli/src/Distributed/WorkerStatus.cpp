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
#include <Distributed/WorkerStatus.hpp>

#include <chrono>
#include <cstdint>
#include <string>
#include <string_view>

#include <fmt/chrono.h>
#include <fmt/format.h>

#include <SingleNodeWorkerRPCService.pb.h>

namespace NES::Distributed
{

std::string formatTimestamp(const uint64_t timestampMs)
{
    auto timePoint = std::chrono::system_clock::time_point(std::chrono::milliseconds(timestampMs));
    return fmt::format("{:%Y-%m-%d %H:%M:%S}.{:03d}", timePoint, timestampMs % 1000);
}

void prettyPrintWorkerStatus(std::string_view grpcAddress, const WorkerStatusResponse& response)
{
    /// Print header
    fmt::print("=== Worker Status Report ===\n");
    fmt::print("Node: {}\n", grpcAddress);
    fmt::print("Report time: {}\n\n", formatTimestamp(response.afterunixtimestampinms()));

    /// Print active queries
    fmt::print("Active Queries: {}\n", response.activequeries_size());
    if (response.activequeries_size() > 0)
    {
        fmt::print("{:<12} {:<30}\n", "Query ID", "Started At");
        fmt::print("{:<12} {:<30}\n", "--------", "----------");

        for (const auto& query : response.activequeries())
        {
            fmt::print("{:<12} {:<30}\n", query.queryid(), formatTimestamp(query.startedunixtimestampinms()));
        }
        fmt::print("\n");
    }

    /// Print terminated queries
    fmt::print("Terminated Queries: {}\n", response.terminatedqueries_size());
    if (response.terminatedqueries_size() > 0)
    {
        fmt::print("{:<12} {:<30} {:<30} {:<30}\n", "Query ID", "Started At", "Terminated At", "Error");
        fmt::print("{:<12} {:<30} {:<30} {:<30}\n", "--------", "----------", "-------------", "-----");

        for (const auto& query : response.terminatedqueries())
        {
            std::string errorMsg = query.has_error() ? fmt::format("Error: {}", query.error().message()) : "None";

            fmt::print(
                "{:<12} {:<30} {:<30} {:<30}\n",
                query.queryid(),
                formatTimestamp(query.startedunixtimestampinms()),
                formatTimestamp(query.terminatedunixtimestampinms()),
                errorMsg);
        }
        fmt::print("\n");
    }

    fmt::print("=== End of Report ===\n");
}
}