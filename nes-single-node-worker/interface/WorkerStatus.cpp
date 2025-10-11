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
        auto* activeQueryGRPC = response->add_active_queries();
        activeQueryGRPC->set_query_id(activeQuery.queryId.getRawValue());
        if (activeQuery.started)
        {
            activeQueryGRPC->set_started_unix_timestamp_in_milli_seconds(
                std::chrono::duration_cast<std::chrono::milliseconds>(activeQuery.started->time_since_epoch()).count());
        }
    }

    for (const auto& terminatedQuery : status.terminatedQueries)
    {
        auto* terminatedQueryGRPC = response->add_terminated_queries();
        terminatedQueryGRPC->set_query_id(terminatedQuery.queryId.getRawValue());
        if (terminatedQuery.started)
        {
            terminatedQueryGRPC->set_started_unix_timestamp_in_milli_seconds(
                std::chrono::duration_cast<std::chrono::milliseconds>(terminatedQuery.started->time_since_epoch()).count());
        }
        terminatedQueryGRPC->set_terminated_unix_timestamp_in_milli_seconds(
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
    response->set_after_unix_timestamp_in_milli_seconds(
        std::chrono::duration_cast<std::chrono::milliseconds>(status.after.time_since_epoch()).count());
    response->set_until_unix_timestamp_in_milli_seconds(
        std::chrono::duration_cast<std::chrono::milliseconds>(status.until.time_since_epoch()).count());
}

WorkerStatus deserializeWorkerStatus(const WorkerStatusResponse* response)
{
    auto fromMillis = [](size_t unixTimestampInMillis)
    { return std::chrono::system_clock::time_point{std::chrono::milliseconds{unixTimestampInMillis}}; };
    return {
        .after = fromMillis(response->after_unix_timestamp_in_milli_seconds()),
        .until = fromMillis(response->until_unix_timestamp_in_milli_seconds()),
        .activeQueries = response->active_queries()
            | std::views::transform(
                             [&](const auto& activeQuery)
                             {
                                 return WorkerStatus::ActiveQuery{
                                     .queryId = LocalQueryId(activeQuery.query_id()),
                                     .started = activeQuery.has_started_unix_timestamp_in_milli_seconds()
                                         ? std::make_optional(fromMillis(activeQuery.started_unix_timestamp_in_milli_seconds()))
                                         : std::nullopt};
                             })
            | std::ranges::to<std::vector>(),
        .terminatedQueries
        = response->terminated_queries()
            | std::views::transform(
                [&](const auto& terminatedQuery)
                {
                    return WorkerStatus::TerminatedQuery{
                        .queryId = LocalQueryId(terminatedQuery.query_id()),
                        .started = terminatedQuery.has_started_unix_timestamp_in_milli_seconds()
                            ? std::make_optional(fromMillis(terminatedQuery.started_unix_timestamp_in_milli_seconds()))
                            : std::nullopt,
                        .terminated = fromMillis(terminatedQuery.terminated_unix_timestamp_in_milli_seconds()),
                        .error = terminatedQuery.has_error()
                            ? std::make_optional(Exception(terminatedQuery.error().message(), terminatedQuery.error().code()))
                            : std::nullopt};
                })
            | std::ranges::to<std::vector>()};
}
}
