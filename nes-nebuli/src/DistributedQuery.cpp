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

NES::DistributedQueryId NES::getNextDistributedQueryId()
{
    static std::atomic_uint64_t id = DistributedQueryId::INITIAL;
    return DistributedQueryId(id++);
}

NES::QueryState NES::DistributedQueryStatus::getGlobalQueryState() const
{
    /// Query if considered failed if any local query failed
    if (std::ranges::any_of(
            localStatusSnapshots | std::views::values, [](const LocalQueryStatus& local) { return local.state == QueryState::Failed; }))
    {
        return QueryState::Failed;
    }
    /// Query is not failed, stopped if all local queries have stopped
    if (std::ranges::all_of(
            localStatusSnapshots | std::views::values, [](const LocalQueryStatus& local) { return local.state == QueryState::Stopped; }))
    {
        return QueryState::Stopped;
    }
    /// Query is neither failed nor stopped. Running, if all local queries are running
    if (std::ranges::all_of(
            localStatusSnapshots | std::views::values, [](const LocalQueryStatus& local) { return local.state == QueryState::Running; }))
    {
        return QueryState::Running;
    }
    /// Query is neither failed nor stopped, nor running. Started, if all local queries have started
    if (std::ranges::all_of(
            localStatusSnapshots | std::views::values, [](const LocalQueryStatus& local) { return local.state == QueryState::Started; }))
    {
        return QueryState::Started;
    }
    /// Some local queries might be stopped, running, or started, but at least one local query has not been started
    return QueryState::Registered;
}

std::unordered_map<NES::GrpcAddr, NES::Exception> NES::DistributedQueryStatus::getExceptions() const
{
    std::unordered_map<GrpcAddr, Exception> exceptions;
    for (const auto& [grpc, localStatus] : localStatusSnapshots)
    {
        if (auto err = localStatus.metrics.error)
        {
            exceptions.emplace(grpc, *err);
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

    return DistributedFailure(fmt::format(
        "\n\t{}",
        fmt::join(
            exceptions
                | std::views::transform(
                    [](const auto& exception)
                    { return fmt::format("{}: {}({})", exception.first, exception.second.what(), exception.second.code()); }),
            "\n\t")));
}

NES::QueryMetrics NES::DistributedQueryStatus::coalesceQueryMetrics() const
{
    QueryMetrics metrics;
    if (std::ranges::all_of(
            localStatusSnapshots | std::views::values, [](const LocalQueryStatus& local) { return local.metrics.start.has_value(); }))
    {
        for (const auto& localStatus : localStatusSnapshots | std::views::values)
        {
            if (!metrics.start.has_value())
            {
                metrics.start = localStatus.metrics.start.value();
            }

            if (*metrics.start > localStatus.metrics.start.value())
            {
                metrics.start = localStatus.metrics.start.value();
            }
        }
    }

    if (std::ranges::all_of(
            localStatusSnapshots | std::views::values, [](const LocalQueryStatus& local) { return local.metrics.running.has_value(); }))
    {
        for (const auto& localStatus : localStatusSnapshots | std::views::values)
        {
            if (!metrics.running.has_value())
            {
                metrics.running = localStatus.metrics.running.value();
            }

            if (*metrics.running > localStatus.metrics.running.value())
            {
                metrics.running = localStatus.metrics.running.value();
            }
        }
    }

    if (std::ranges::all_of(
            localStatusSnapshots | std::views::values, [](const LocalQueryStatus& local) { return local.metrics.stop.has_value(); }))
    {
        for (const auto& localStatus : localStatusSnapshots | std::views::values)
        {
            if (!metrics.stop.has_value())
            {
                metrics.stop = localStatus.metrics.stop.value();
            }

            if (*metrics.stop < localStatus.metrics.stop.value())
            {
                metrics.stop = localStatus.metrics.stop.value();
            }
        }
    }

    metrics.error = coalesceException();
    return metrics;
}

NES::DistributedQuery::DistributedQuery(std::vector<LocalQuery> localQueries) : localQueries(std::move(localQueries))
{
}

std::ostream& NES::operator<<(std::ostream& os, const DistributedQuery& query)
{
    fmt::print(
        os,
        "Query [{}]",
        fmt::join(
            query.localQueries
                | std::views::transform([](const auto& localQuery) { return fmt::format("{}@{}", localQuery.id, localQuery.grpcAddr); }),
            ", "));
    return os;
}
