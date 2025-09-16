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

#include <WorkerStatus.hpp>

#include <chrono>
#include <cstddef>
#include <optional>
#include <ranges>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <cpptrace/basic.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>
#include <SingleNodeWorkerRPCService.pb.h>

namespace NES
{

void serializeWorkerStatus(const WorkerStatus& status, WorkerStatusResponse* response)
{
    for (const auto& activeQuery : status.activeQueries)
    {
        auto* activeQueryGRPC = response->add_activequeries();
        activeQueryGRPC->set_queryid(activeQuery.queryId.getRawValue());
        activeQueryGRPC->set_startedunixtimestampinms(
            std::chrono::duration_cast<std::chrono::milliseconds>(activeQuery.started.time_since_epoch()).count());
    }

    for (const auto& terminatedQuery : status.terminatedQueries)
    {
        auto* terminatedQueryGRPC = response->add_terminatedqueries();
        terminatedQueryGRPC->set_queryid(terminatedQuery.queryId.getRawValue());
        terminatedQueryGRPC->set_startedunixtimestampinms(
            std::chrono::duration_cast<std::chrono::milliseconds>(terminatedQuery.started.time_since_epoch()).count());
        terminatedQueryGRPC->set_terminatedunixtimestampinms(
            std::chrono::duration_cast<std::chrono::milliseconds>(terminatedQuery.terminated.time_since_epoch()).count());
        if (terminatedQuery.error)
        {
            const auto& exception = terminatedQuery.error.value();
            auto* errorGRPC = terminatedQueryGRPC->mutable_error();
            errorGRPC->set_message(exception.what());
            errorGRPC->set_stacktrace(exception.trace().to_string());
            errorGRPC->set_code(exception.code());
            errorGRPC->set_location(fmt::format(
                "{}:{}",
                exception.where().transform([](const auto& where) { return where.filename; }).value_or("unknown"),
                exception.where().transform([](const auto& where) { return where.line.value_or(-1); }).value_or(-1)));
        }
    }
    response->set_afterunixtimestampinms(std::chrono::duration_cast<std::chrono::milliseconds>(status.after.time_since_epoch()).count());
    response->set_untilunixtimestampinms(std::chrono::duration_cast<std::chrono::milliseconds>(status.until.time_since_epoch()).count());
}

WorkerStatus deserializeWorkerStatus(const WorkerStatusResponse* response)
{
    auto fromMillis = [](size_t unixTimestampInMillis)
    { return std::chrono::system_clock::time_point{std::chrono::milliseconds{unixTimestampInMillis}}; };
    return {
        .after = fromMillis(response->afterunixtimestampinms()),
        .until = fromMillis(response->untilunixtimestampinms()),
        .activeQueries = response->activequeries()
            | std::views::transform(
                             [&](const auto& activeQuery)
                             {
                                 return WorkerStatus::ActiveQuery{
                                     .queryId = QueryId(activeQuery.queryid()),
                                     .started = fromMillis(activeQuery.startedunixtimestampinms())};
                             })
            | std::ranges::to<std::vector>(),
        .terminatedQueries
        = response->terminatedqueries()
            | std::views::transform(
                [&](const auto& terminatedQuery)
                {
                    return WorkerStatus::TerminatedQuery{
                        .queryId = QueryId(terminatedQuery.queryid()),
                        .started = fromMillis(terminatedQuery.startedunixtimestampinms()),
                        .terminated = fromMillis(terminatedQuery.terminatedunixtimestampinms()),
                        .error = terminatedQuery.has_error()
                            ? std::make_optional(Exception(terminatedQuery.error().message(), terminatedQuery.error().code()))
                            : std::nullopt};
                })
            | std::ranges::to<std::vector>()};
}

}
