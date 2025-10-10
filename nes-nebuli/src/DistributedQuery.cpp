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

#include <algorithm>
#include <initializer_list>
#include <optional>
#include <sstream>
#include <vector>
#include <Listeners/QueryLog.hpp>
#include <DistributedQuery.hpp>
#include <ErrorHandling.hpp>

NES::DistributedQueryId NES::getNextDistributedQueryId()
{
    static std::atomic_uint64_t id = DistributedQueryId::INITIAL;
    return DistributedQueryId(id++);
}

NES::DistributedQueryState NES::DistributedQueryStatus::getGlobalQueryState() const
{
    auto any = [&](std::initializer_list<QueryState> state)
    {
        return std::ranges::any_of(
            localStatusSnapshots | std::views::values,
            [&](const std::vector<std::expected<LocalQueryStatus, Exception>>& local)
            {
                return std::ranges::any_of(
                    local,
                    [&](const std::expected<LocalQueryStatus, Exception>& local)
                    { return local.has_value() && std::ranges::contains(state, local.value().state); });
            });
    };
    auto all = [&](std::initializer_list<QueryState> state)
    {
        return std::ranges::all_of(
            localStatusSnapshots | std::views::values,
            [&](const std::vector<std::expected<LocalQueryStatus, Exception>>& local)
            {
                return std::ranges::all_of(
                    local,
                    [&](const std::expected<LocalQueryStatus, Exception>& local)
                    { return local.has_value() && std::ranges::contains(state, local.value().state); });
            });
    };

    if (any({QueryState::Failed}))
    {
        return DistributedQueryState::Failed;
    }

    if (all({QueryState::Stopped}))
    {
        return DistributedQueryState::Stopped;
    }

    if (any({QueryState::Stopped}))
    {
        return DistributedQueryState::PartiallyStopped;
    }

    if (all({QueryState::Registered}))
    {
        return DistributedQueryState::Registered;
    }

    if (all({QueryState::Stopped, QueryState::Running, QueryState::Started, QueryState::Registered}))
    {
        return DistributedQueryState::Running;
    }

    INVARIANT(false, "unreachable. i think");
}

std::unordered_map<NES::GrpcAddr, std::vector<NES::Exception>> NES::DistributedQueryStatus::getExceptions() const
{
    std::unordered_map<GrpcAddr, std::vector<Exception>> exceptions;
    for (const auto& [grpc, localStatusResults] : localStatusSnapshots)
    {
        for (const auto& result : localStatusResults)
        {
            if (result.has_value() && result->metrics.error)
            {
                exceptions[grpc].emplace_back(result.value().metrics.error.value());
            }
            else if (!result.has_value())
            {
                exceptions[grpc].emplace_back(result.error());
            }
        }
    }
    return exceptions;
}

std::optional<NES::Exception> NES::DistributedQueryStatus::coalesceException() const
{
    auto exceptions = getExceptions();
    if (exceptions.empty())
    {
        return std::nullopt;
    }

    std::stringstream builder;
    for (const auto& [grpc, localExceptions] : exceptions)
    {
        for (const auto& exception : localExceptions)
        {
            builder << fmt::format("\n\t{}: {}({})", grpc, exception.what(), exception.code());
        }
    }

    return DistributedFailure(builder.str());
}

NES::QueryMetrics NES::DistributedQueryStatus::coalesceQueryMetrics() const
{
    auto all = [&](auto predicate)
    {
        return std::ranges::all_of(
            localStatusSnapshots | std::views::values,
            [&](const std::vector<std::expected<LocalQueryStatus, Exception>>& local)
            {
                return std::ranges::all_of(
                    local,
                    [&](const std::expected<LocalQueryStatus, Exception>& local) { return local.has_value() && predicate(local.value()); });
            });
    };
    auto get = [&](auto projection)
    {
        return localStatusSnapshots | std::views::values | std::views::join
            | std::views::transform([&](const std::expected<LocalQueryStatus, Exception>& local) { return projection(local.value()); });
    };

    QueryMetrics metrics;
    if (all([](const LocalQueryStatus& local) { return local.metrics.start.has_value(); }))
    {
        metrics.start = std::ranges::min(get([](const LocalQueryStatus& local) { return local.metrics.start.value(); }));
    }

    if (all([](const LocalQueryStatus& local) { return local.metrics.running.has_value(); }))
    {
        metrics.running = std::ranges::max(get([](const LocalQueryStatus& local) { return local.metrics.running.value(); }));
    }

    if (all([](const LocalQueryStatus& local) { return local.metrics.stop.has_value(); }))
    {
        metrics.stop = std::ranges::max(get([](const LocalQueryStatus& local) { return local.metrics.stop.value(); }));
    }

    metrics.error = coalesceException();
    return metrics;
}

NES::DistributedQuery::DistributedQuery(std::unordered_map<GrpcAddr, std::vector<LocalQueryId>> localQueries) : localQueries(std::move(localQueries))
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

std::ostream& NES::operator<<(std::ostream& ostream, const DistributedQueryState& status)
{
    return ostream << magic_enum::enum_name(status);
}
