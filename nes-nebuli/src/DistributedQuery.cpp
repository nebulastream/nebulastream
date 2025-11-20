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
#include <array>
#include <ctime>
#include <initializer_list>
#include <optional>
#include <ostream>
#include <random>
#include <ranges>
#include <sstream>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Listeners/QueryLog.hpp>
#include <Runtime/Execution/QueryStatus.hpp>
#include <fmt/format.h>
#include <fmt/ostream.h>
#include <fmt/ranges.h>
#include <magic_enum/magic_enum.hpp>
#include <ErrorHandling.hpp>
#include <WorkerConfig.hpp>

namespace
{
constexpr std::array HORSE_BREEDS = std::to_array<std::string_view>(
    {"arabian",    "thoroughbred", "quarter",  "morgan",       "appaloosa", "paint",     "mustang",   "andalusian", "friesian",
     "clydesdale", "percheron",    "belgian",  "shire",        "shetland",  "welsh",     "connemara", "hanoverian", "warmblood",
     "lipizzaner", "palomino",     "buckskin", "standardbred", "tennessee", "icelandic", "haflinger"});

constexpr std::array ATTRIBUTES = std::to_array<std::string_view>(
    {"swift",    "brave",    "noble",   "wild",        "gallant",   "proud",   "mighty",  "gentle",   "fierce",   "graceful",
     "spirited", "majestic", "elegant", "strong",      "agile",     "loyal",   "bold",    "free",     "dashing",  "swift",
     "valiant",  "fearless", "regal",   "magnificent", "brilliant", "radiant", "blazing", "charging", "prancing", "soaring"});

/// Generate a unique Docker-style name
std::string generateHorseName()
{
    static std::mt19937 rng(static_cast<unsigned>(std::time(nullptr)));
    std::uniform_int_distribution<> randomBreed(0, HORSE_BREEDS.size() - 1);
    std::uniform_int_distribution<> randomAttribute(0, ATTRIBUTES.size() - 1);

    std::string_view attribute = ATTRIBUTES.at(randomAttribute(rng));
    std::string_view breed = HORSE_BREEDS.at(randomBreed(rng));

    return fmt::format("{}_{}", attribute, breed);
}

}

NES::DistributedQueryId NES::getNextDistributedQueryId()
{
    return NES::DistributedQueryId(generateHorseName());
}

NES::DistributedQueryState NES::DistributedQueryStatus::getGlobalQueryState() const
{
    auto any = [&](std::initializer_list<QueryState> state)
    {
        return std::ranges::any_of(
            localStatusSnapshots | std::views::values,
            [&](const std::unordered_map<LocalQueryId, std::expected<LocalQueryStatus, Exception>>& localMap)
            {
                return std::ranges::any_of(
                    localMap | std::views::values,
                    [&](const std::expected<LocalQueryStatus, Exception>& local)
                    { return local.has_value() && std::ranges::contains(state, local.value().state); });
            });
    };
    auto all = [&](std::initializer_list<QueryState> state)
    {
        return std::ranges::all_of(
            localStatusSnapshots | std::views::values,
            [&](const std::unordered_map<LocalQueryId, std::expected<LocalQueryStatus, Exception>>& localMap)
            {
                return std::ranges::all_of(
                    localMap | std::views::values,
                    [&](const std::expected<LocalQueryStatus, Exception>& local)
                    { return local.has_value() && std::ranges::contains(state, local.value().state); });
            });
    };
    auto hasConnectionException = std::ranges::any_of(
        localStatusSnapshots | std::views::values,
        [](const auto& perWorker)
        { return std::ranges::any_of(perWorker | std::views::values, [](const auto& expected) { return !expected.has_value(); }); });

    if (any({QueryState::Failed}))
    {
        return DistributedQueryState::Failed;
    }

    if (hasConnectionException)
    {
        return DistributedQueryState::Unreachable;
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
    std::unreachable();
}

std::unordered_map<NES::GrpcAddr, std::vector<NES::Exception>> NES::DistributedQueryStatus::getExceptions() const
{
    std::unordered_map<GrpcAddr, std::vector<Exception>> exceptions;
    for (const auto& [grpc, localStatusMap] : localStatusSnapshots)
    {
        for (const auto& [localQueryId, result] : localStatusMap)
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
            [&](const std::unordered_map<LocalQueryId, std::expected<LocalQueryStatus, Exception>>& localMap)
            {
                return std::ranges::all_of(
                    localMap | std::views::values,
                    [&](const std::expected<LocalQueryStatus, Exception>& local) { return local.has_value() && predicate(local.value()); });
            });
    };
    auto get = [&](auto projection)
    {
        return localStatusSnapshots | std::views::values
            | std::views::transform([](const auto& localMap) { return localMap | std::views::values; }) | std::views::join
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

NES::DistributedQuery::DistributedQuery(std::unordered_map<GrpcAddr, std::vector<LocalQueryId>> localQueries)
    : localQueries(std::move(localQueries))
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
