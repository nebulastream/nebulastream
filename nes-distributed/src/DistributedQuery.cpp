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

#include <DistributedQuery.hpp>

#include <algorithm>
#include <initializer_list>
#include <optional>
#include <ostream>
#include <ranges>
#include <sstream>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <fmt/format.h>
#include <fmt/ostream.h>
#include <fmt/ranges.h>
#include <magic_enum/magic_enum.hpp>
#include <ErrorHandling.hpp>
#include <QueryId.hpp>
#include <QueryStatus.hpp>

NES::DistributedException::DistributedException(std::unordered_map<Host, std::vector<Exception>> errors) : errors(std::move(errors))
{
    std::stringstream builder;
    builder << "Multiple Exceptions:";
    for (const auto& [grpc, localExceptions] : this->errors)
    {
        for (const auto& exception : localExceptions)
        {
            builder << fmt::format("\n\t{}: {}({})", grpc, exception.what(), exception.code());
        }
    }
    errorMessage = builder.str();
}

NES::DistributedQueryStatus NES::DistributedQueryStatusSnapshot::getGlobalQueryStatus() const
{
    auto any = [&](std::initializer_list<QueryStatus> state)
    {
        return std::ranges::any_of(
            localStatusSnapshots | std::views::values,
            [&](const std::unordered_map<QueryId, std::expected<LocalQueryStatusSnapshot, Exception>>& localMap)
            {
                return std::ranges::any_of(
                    localMap | std::views::values,
                    [&](const std::expected<LocalQueryStatusSnapshot, Exception>& local)
                    { return local.has_value() && std::ranges::contains(state, local.value().state); });
            });
    };
    auto all = [this](std::initializer_list<QueryStatus> state)
    {
        return std::ranges::all_of(
            localStatusSnapshots | std::views::values,
            [&](const std::unordered_map<QueryId, std::expected<LocalQueryStatusSnapshot, Exception>>& localMap)
            {
                return std::ranges::all_of(
                    localMap | std::views::values,
                    [&](const std::expected<LocalQueryStatusSnapshot, Exception>& local)
                    { return local.has_value() && std::ranges::contains(state, local.value().state); });
            });
    };
    auto hasConnectionException = std::ranges::any_of(
        localStatusSnapshots | std::views::values,
        [](const auto& perWorker)
        { return std::ranges::any_of(perWorker | std::views::values, [](const auto& expected) { return !expected.has_value(); }); });

    if (any({QueryStatus::Failed}))
    {
        return DistributedQueryStatus::Failed;
    }

    if (hasConnectionException)
    {
        return DistributedQueryStatus::Unreachable;
    }

    if (all({QueryStatus::Stopped}))
    {
        return DistributedQueryStatus::Stopped;
    }

    if (any({QueryStatus::Stopped}))
    {
        return DistributedQueryStatus::PartiallyStopped;
    }

    if (all({QueryStatus::Registered}))
    {
        return DistributedQueryStatus::Registered;
    }

    if (all({QueryStatus::Stopped, QueryStatus::Running, QueryStatus::Started, QueryStatus::Registered}))
    {
        return DistributedQueryStatus::Running;
    }

    INVARIANT(false, "Unexpected combination of local query states");
    std::unreachable();
}

std::unordered_map<NES::Host, std::vector<NES::Exception>> NES::DistributedQueryStatusSnapshot::getExceptions() const
{
    std::unordered_map<Host, std::vector<Exception>> exceptions;
    for (const auto& [grpc, localStatusMap] : localStatusSnapshots)
    {
        for (const auto& [localQueryId, result] : localStatusMap)
        {
            if (result.has_value() && result->metrics.error.has_value())
            {
                /// NOLINTNEXTLINE(bugprone-unchecked-optional-access) guarded by has_value() above
                exceptions[grpc].emplace_back(result->metrics.error.value());
            }
            else if (!result.has_value())
            {
                exceptions[grpc].emplace_back(result.error());
            }
        }
    }
    return exceptions;
}

std::optional<NES::DistributedException> NES::DistributedQueryStatusSnapshot::coalesceException() const
{
    auto exceptions = getExceptions();
    if (exceptions.empty())
    {
        return std::nullopt;
    }
    return DistributedException(exceptions);
}

NES::DistributedQueryMetrics NES::DistributedQueryStatusSnapshot::coalesceQueryMetrics() const
{
    auto all = [&](auto predicate)
    {
        return std::ranges::all_of(
            localStatusSnapshots | std::views::values,
            [&](const std::unordered_map<QueryId, std::expected<LocalQueryStatusSnapshot, Exception>>& localMap)
            {
                return std::ranges::all_of(
                    localMap | std::views::values,
                    [&](const std::expected<LocalQueryStatusSnapshot, Exception>& local)
                    { return local.has_value() && predicate(local.value()); });
            });
    };
    auto get = [&](auto projection)
    {
        return localStatusSnapshots | std::views::values
            | std::views::transform([](const auto& localMap) { return localMap | std::views::values; }) | std::views::join
            | std::views::transform([&](const std::expected<LocalQueryStatusSnapshot, Exception>& local)
                                    { return projection(local.value()); });
    };

    DistributedQueryMetrics metrics;
    if (all([](const LocalQueryStatusSnapshot& local) { return local.metrics.start.has_value(); }))
    {
        metrics.start = std::ranges::min(get([](const LocalQueryStatusSnapshot& local) { return local.metrics.start.value(); }));
    }

    if (all([](const LocalQueryStatusSnapshot& local) { return local.metrics.running.has_value(); }))
    {
        metrics.running = std::ranges::max(get([](const LocalQueryStatusSnapshot& local) { return local.metrics.running.value(); }));
    }

    if (all([](const LocalQueryStatusSnapshot& local) { return local.metrics.stop.has_value(); }))
    {
        metrics.stop = std::ranges::max(get([](const LocalQueryStatusSnapshot& local) { return local.metrics.stop.value(); }));
    }

    metrics.error = coalesceException();
    return metrics;
}

NES::DistributedQuery::DistributedQuery(std::unordered_map<Host, std::vector<QueryId>> localQueries) : localQueries(std::move(localQueries))
{
}

std::ostream& NES::operator<<(std::ostream& os, const DistributedQuery& query)
{
    std::vector<std::string> entries;
    for (const auto& [grpc, ids] : query.localQueries)
    {
        for (const auto& id : ids)
        {
            entries.push_back(fmt::format("{}@{}", id, grpc));
        }
    }
    fmt::print(os, "Query [{}]", fmt::join(entries, ", "));
    return os;
}

std::ostream& NES::operator<<(std::ostream& ostream, const DistributedQueryStatus& status)
{
    return ostream << magic_enum::enum_name(status);
}
