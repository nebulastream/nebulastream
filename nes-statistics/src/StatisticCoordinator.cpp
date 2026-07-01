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

#include <StatisticCoordinator.hpp>

#include <array>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <csignal>
#include <cstddef>
#include <cstdint>
#include <future>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include <fcntl.h>
#include <poll.h>
#include <sys/stat.h>
#include <unistd.h>

#include <Identifiers/Identifiers.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Util/Files.hpp>
#include <Util/Logger/Logger.hpp>
#include <WindowTypes/Measures/TimeMeasure.hpp>
#include <ConditionTrigger.hpp>
#include <ErrorHandling.hpp>
#include <RequestStatisticStatement.hpp>
#include <Statistic.hpp>
#include <StatisticQueryGenerator.hpp>
#include <StatisticRegistry.hpp>

namespace NES
{

StatisticCoordinator::StatisticCoordinator(
    std::unique_ptr<StatisticQueryGenerator> queryGenerator, SubmitQueryFn submitQuery, StopQueryFn stopQuery)
    : queryGenerator(std::move(queryGenerator)), submitQuery(std::move(submitQuery)), stopQuery(std::move(stopQuery))
{
}

StatisticCoordinator::~StatisticCoordinator()
{
    stopResultReader();
}

std::expected<CollectStatisticResult, Exception> StatisticCoordinator::collectNewStatistic(const RequestStatisticBuildStatement& statement)
{
    const StatisticRegistry::Key key{
        .metric = statement.metric, .collectionDomain = statement.domain, .windowSize = Windowing::TimeMeasure{statement.windowSizeMs}};

    if (const auto existing = registry.find(key))
    {
        if (statement.conditionTrigger.has_value())
        {
            registry.addTrigger(key, statement.conditionTrigger.value());
        }
        return CollectStatisticResult{.queryId = existing->queryId, .statisticId = existing->statisticId, .alreadyExisted = true};
    }

    const auto statisticId = Statistic::StatisticId{nextStatisticId.fetch_add(1)};
    auto plan = queryGenerator->generateBuildQuery(statisticId.getRawValue(), statement.windowSizeMs, fifoPath);

    return submitQuery(std::move(plan))
        .transform(
            [this, &key, statisticId, &statement](auto queryId)
            {
                std::vector<ConditionTrigger> triggers;
                if (statement.conditionTrigger.has_value())
                {
                    triggers.emplace_back(*statement.conditionTrigger);
                }
                registry.registerStatistic(key, queryId, statisticId, std::move(triggers));
                return CollectStatisticResult{.queryId = queryId, .statisticId = statisticId, .alreadyExisted = false};
            });
}

std::expected<CollectStatisticResult, Exception> StatisticCoordinator::watchStatistic(const RequestStatisticBuildStatement& statement)
{
    const StatisticRegistry::Key key{
        .metric = statement.metric, .collectionDomain = statement.domain, .windowSize = Windowing::TimeMeasure{statement.windowSizeMs}};

    if (const auto existing = registry.find(key))
    {
        /// A watch subsumes a build, but a build cannot serve as a watch (it never reads the store back). Turning a
        /// running build into a watch would mean redeploying it with a different query shape — not supported yet.
        if (existing->kind == StatisticQueryKind::Build)
        {
            throw NotImplemented("upgrading a running statistic build query to a watch query is not implemented");
        }
        if (statement.conditionTrigger.has_value())
        {
            registry.addTrigger(key, statement.conditionTrigger.value());
        }
        return CollectStatisticResult{.queryId = existing->queryId, .statisticId = existing->statisticId, .alreadyExisted = true};
    }

    const auto statisticId = Statistic::StatisticId{nextStatisticId.fetch_add(1)};
    auto plan = queryGenerator->generateWatchQuery(statisticId.getRawValue(), statement.windowSizeMs, fifoPath);

    return submitQuery(std::move(plan))
        .transform(
            [this, &key, statisticId, &statement](auto queryId)
            {
                std::vector<ConditionTrigger> triggers;
                if (statement.conditionTrigger.has_value())
                {
                    triggers.emplace_back(*statement.conditionTrigger);
                }
                registry.registerStatistic(key, queryId, statisticId, std::move(triggers), StatisticQueryKind::Watch);
                return CollectStatisticResult{.queryId = queryId, .statisticId = statisticId, .alreadyExisted = false};
            });
}

bool StatisticCoordinator::addConditionTrigger(const StatisticRegistry::Key& key, ConditionTrigger trigger)
{
    return registry.addTrigger(key, std::move(trigger));
}

bool StatisticCoordinator::deregisterStatistic(const StatisticRegistry::Key& key)
{
    return registry.deregisterStatistic(key);
}

bool StatisticCoordinator::stopStatistic(const StatisticRegistry::Key& key)
{
    const auto entry = registry.find(key);
    if (not entry.has_value())
    {
        return false;
    }
    if (stopQuery)
    {
        if (auto stopped = stopQuery(entry->queryId); not stopped.has_value())
        {
            NES_WARNING(
                "StatisticCoordinator::stopStatistic: failed to stop build query {}: {}", entry->queryId, stopped.error().what());
        }
    }
    registry.deregisterStatistic(key);
    return true;
}

std::string StatisticCoordinator::startResultReader()
{
    /// A data-plane FileSink may still be writing when we close/unlink the FIFO on shutdown; ignore SIGPIPE so that
    /// write fails with EPIPE instead of terminating the whole process.
    std::signal(SIGPIPE, SIG_IGN);

    /// Unique per coordinator instance (not just per process) so multiple coordinators in one process do not collide.
    static std::atomic<uint64_t> fifoCounter{0};
    fifoPath = "/tmp/nes_statistic_coordinator_" + std::to_string(::getpid()) + "_" + std::to_string(fifoCounter.fetch_add(1)) + ".fifo";
    ::unlink(fifoPath.c_str());
    if (::mkfifo(fifoPath.c_str(), 0600) != 0)
    {
        throw CannotOpenSink("StatisticCoordinator: mkfifo failed for {}: {}", fifoPath, getErrorMessageFromERRNO());
    }
    readerRunning.store(true);
    readerThread = std::thread([this] { readerLoop(); });
    NES_INFO("StatisticCoordinator result FIFO listening at {}", fifoPath);
    return fifoPath;
}

void StatisticCoordinator::stopResultReader()
{
    readerRunning.store(false);
    if (readerThread.joinable())
    {
        readerThread.join();
    }
    if (not fifoPath.empty())
    {
        ::unlink(fifoPath.c_str());
        fifoPath.clear();
    }
}

void StatisticCoordinator::readerLoop()
{
    /// O_NONBLOCK so the open succeeds immediately (no writer required yet) and the loop stays responsive to the stop flag.
    const int fd = ::open(fifoPath.c_str(), O_RDONLY | O_NONBLOCK);
    if (fd < 0)
    {
        NES_ERROR("StatisticCoordinator: cannot open FIFO {}: {}", fifoPath, getErrorMessageFromERRNO());
        return;
    }

    const auto reportCsvLine = [this](const std::string& line)
    {
        if (line.empty())
        {
            return;
        }
        const auto c1 = line.find(',');
        const auto c2 = line.find(',', c1 + 1);
        const auto c3 = line.find(',', c2 + 1);
        if (c1 == std::string::npos || c2 == std::string::npos || c3 == std::string::npos)
        {
            return;
        }
        try
        {
            const auto statisticId = std::stoull(line.substr(0, c1));
            const auto startTs = std::stoull(line.substr(c1 + 1, c2 - c1 - 1));
            const auto endTs = std::stoull(line.substr(c2 + 1, c3 - c2 - 1));
            const auto value = std::stod(line.substr(c3 + 1));
            onStatisticReport(
                Statistic::StatisticId{statisticId}, Windowing::TimeMeasure{startTs}, Windowing::TimeMeasure{endTs}, value);
        }
        catch (...)
        {
            /// Skip malformed lines (e.g. a stray schema header).
        }
    };

    std::string buffer;
    std::array<char, 4096> chunk{};
    pollfd pfd{.fd = fd, .events = POLLIN, .revents = 0};
    while (readerRunning.load())
    {
        const int pollResult = ::poll(&pfd, 1, 200);
        if (pollResult < 0)
        {
            if (errno == EINTR)
            {
                continue;
            }
            break;
        }
        if (pollResult == 0)
        {
            continue;
        }
        if ((pfd.revents & POLLIN) != 0)
        {
            const auto bytesRead = ::read(fd, chunk.data(), chunk.size());
            if (bytesRead > 0)
            {
                buffer.append(chunk.data(), static_cast<size_t>(bytesRead));
                for (auto newline = buffer.find('\n'); newline != std::string::npos; newline = buffer.find('\n'))
                {
                    reportCsvLine(buffer.substr(0, newline));
                    buffer.erase(0, newline + 1);
                }
            }
            /// bytesRead == 0 (no writer attached yet) or < 0 (EAGAIN): just keep polling.
        }
    }
    ::close(fd);
}

std::optional<double>
StatisticCoordinator::getStatistics(const StatisticRegistry::Key& key, Windowing::TimeMeasure startTs, Windowing::TimeMeasure endTs)
{
    const auto entry = registry.find(key);
    if (not entry.has_value())
    {
        throw QueryNotFound("StatisticCoordinator::getStatistics: key not found in registry");
    }
    const auto statisticId = entry->statisticId;

    std::promise<double> promise;
    auto future = promise.get_future();
    pendingProbes.wlock()->insert_or_assign(statisticId, PendingProbe{.promise = std::move(promise)});

    auto plan = queryGenerator->generateProbeQuery(statisticId.getRawValue(), startTs.getTime(), endTs.getTime(), fifoPath);
    const auto submitResult = submitQuery(std::move(plan));
    if (not submitResult.has_value())
    {
        pendingProbes.wlock()->erase(statisticId);
        NES_WARNING("StatisticCoordinator::getStatistics: probe submit failed: {}", submitResult.error().what());
        return std::nullopt;
    }
    const auto probeQueryId = submitResult.value();

    constexpr auto timeout = std::chrono::seconds{30};
    std::optional<double> result;
    if (future.wait_for(timeout) == std::future_status::ready)
    {
        result = future.get();
    }
    else
    {
        NES_WARNING("StatisticCoordinator::getStatistics: timeout waiting for probe result of statisticId={}", statisticId);
    }
    pendingProbes.wlock()->erase(statisticId);

    /// The probe is a one-shot query; stop it now that we have the result so it does not linger.
    if (stopQuery)
    {
        if (auto stopped = stopQuery(probeQueryId); not stopped.has_value())
        {
            NES_WARNING("StatisticCoordinator::getStatistics: failed to stop probe query {}: {}", probeQueryId, stopped.error().what());
        }
    }
    return result;
}

void StatisticCoordinator::onStatisticReport(
    const Statistic::StatisticId statisticId, const Windowing::TimeMeasure startTs, const Windowing::TimeMeasure endTs, const double value)
{
    /// Is this a response to a pending probe?
    {
        auto probes = pendingProbes.wlock();
        if (auto it = probes->find(statisticId); it != probes->end())
        {
            it->second.promise.set_value(value);
            probes->erase(it);
            return;
        }
    }

    /// Otherwise fire any condition triggers registered for this statisticId.
    registry.forEachEntry(
        [&](const auto&, const StatisticRegistry::Entry& entry)
        {
            if (entry.statisticId == statisticId)
            {
                for (const auto& trigger : entry.triggers)
                {
                    trigger.callback(statisticId, startTs, endTs, value);
                }
            }
        });
}

}
