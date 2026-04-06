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

#include <SystemSocketReader.hpp>

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <filesystem>
#include <functional>
#include <sstream>
#include <string>
#include <thread>

#include <csignal>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

#include <Util/Logger/Logger.hpp>
#include <fmt/ranges.h>
#include <ErrorHandling.hpp>

namespace NES::Systest
{

std::string SystemSocketReader::statusSocketPathForHost(const Host& host)
{
    auto sanitized = host.getRawValue();
    std::replace(sanitized.begin(), sanitized.end(), ':', '_');
    return "/tmp/nes_query_status_" + sanitized + ".sock";
}

std::string SystemSocketReader::compilationSocketPathForHost(const Host& host)
{
    auto sanitized = host.getRawValue();
    std::replace(sanitized.begin(), sanitized.end(), ':', '_');
    return "/tmp/nes_pipeline_compilation_times_" + sanitized + ".sock";
}

std::string SystemSocketReader::ingestionSocketPathForHost(const Host& host)
{
    auto sanitized = host.getRawValue();
    std::replace(sanitized.begin(), sanitized.end(), ':', '_');
    return "/tmp/nes_buffer_ingestion_" + sanitized + ".sock";
}

void SystemSocketReader::waitForSockets(const std::vector<Host>& workerHosts, std::chrono::milliseconds timeout)
{
    std::vector<std::string> paths;
    paths.reserve(workerHosts.size() * 3);
    for (const auto& host : workerHosts)
    {
        paths.push_back(statusSocketPathForHost(host));
        paths.push_back(compilationSocketPathForHost(host));
        paths.push_back(ingestionSocketPathForHost(host));
    }

    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline)
    {
        bool allExist = true;
        for (const auto& path : paths)
        {
            if (!std::filesystem::exists(path))
            {
                allExist = false;
                break;
            }
        }
        if (allExist)
        {
            NES_DEBUG("All system query sockets are ready");
            return;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    std::vector<std::string> missing;
    for (const auto& path : paths)
    {
        if (!std::filesystem::exists(path))
        {
            missing.push_back(path);
        }
    }
    throw TestException("Timed out waiting for system query sockets: {}", fmt::join(missing, ", "));
}

SystemSocketReader::SystemSocketReader(const std::vector<Host>& workerHosts)
{
    // Ignore SIGPIPE globally — socket disconnections are handled via error codes, not signals.
    std::signal(SIGPIPE, SIG_IGN);

    for (const auto& host : workerHosts)
    {
        threads.emplace_back(&SystemSocketReader::statusReaderThread, this, statusSocketPathForHost(host));
        threads.emplace_back(&SystemSocketReader::compilationReaderThread, this, compilationSocketPathForHost(host));
        threads.emplace_back(&SystemSocketReader::ingestionReaderThread, this, ingestionSocketPathForHost(host));
    }
}

SystemSocketReader::~SystemSocketReader()
{
    stop();
}

void SystemSocketReader::stop()
{
    running.store(false);
    cv.notify_all();
    for (auto& t : threads)
    {
        if (t.joinable())
        {
            t.join();
        }
    }
    threads.clear();
}

namespace
{
/// Connect to a unix socket with a 1s read timeout.  Returns fd or -1.
int connectToSocket(const std::string& socketPath, [[maybe_unused]] std::atomic<bool>& running)
{
    const int fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (fd < 0)
    {
        NES_WARNING("SystemSocketReader: could not create socket: {}", std::strerror(errno));
        return -1;
    }

    sockaddr_un addr{};
    addr.sun_family = AF_UNIX;
    std::strncpy(addr.sun_path, socketPath.c_str(), sizeof(addr.sun_path) - 1);

    if (connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0)
    {
        NES_WARNING("SystemSocketReader: could not connect to {}: {}", socketPath, std::strerror(errno));
        close(fd);
        return -1;
    }

    timeval tv{};
    tv.tv_sec = 1;
    tv.tv_usec = 0;
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

    NES_DEBUG("SystemSocketReader: connected to {}", socketPath);
    return fd;
}

/// Read lines from a socket, calling lineHandler for each non-header line.
void readSocketLines(int fd, std::atomic<bool>& running, const std::string& socketPath,
                     const std::function<void(const std::string&)>& lineHandler)
{
    std::string buffer;
    bool headerSkipped = false;
    char readBuf[4096];

    while (running.load())
    {
        const ssize_t n = recv(fd, readBuf, sizeof(readBuf), 0);
        if (n < 0)
        {
            if (errno == EAGAIN || errno == EWOULDBLOCK)
            {
                continue;
            }
            NES_DEBUG("SystemSocketReader: read error on {}: {}", socketPath, std::strerror(errno));
            break;
        }
        if (n == 0)
        {
            NES_DEBUG("SystemSocketReader: socket {} closed by server", socketPath);
            break;
        }

        buffer.append(readBuf, static_cast<size_t>(n));

        size_t pos;
        while ((pos = buffer.find('\n')) != std::string::npos)
        {
            auto line = buffer.substr(0, pos);
            buffer.erase(0, pos + 1);

            if (!line.empty() && line.back() == '\r')
            {
                line.pop_back();
            }
            if (line.empty())
            {
                continue;
            }
            if (!headerSkipped)
            {
                headerSkipped = true;
                continue;
            }

            lineHandler(line);
        }
    }

    close(fd);
}
}

void SystemSocketReader::statusReaderThread(std::string socketPath)
{
    const int fd = connectToSocket(socketPath, running);
    if (fd < 0)
    {
        return;
    }

    readSocketLines(fd, running, socketPath, [this](const std::string& line)
    {
        // CSV: timestamp_ms,query_id,status
        std::istringstream iss(line);
        std::string tsStr, queryId, status;
        if (!std::getline(iss, tsStr, ',') || !std::getline(iss, queryId, ',') || !std::getline(iss, status, ','))
        {
            return;
        }

        uint64_t ts = 0;
        try { ts = std::stoull(tsStr); }
        catch (...) { return; }

        if (status == "stopped" || status == "failed")
        {
            std::lock_guard lock(statusMutex);
            terminalEvents.push(QueryStatusEvent{ts, std::move(queryId), std::move(status)});
            cv.notify_all();
        }
    });
}

void SystemSocketReader::compilationReaderThread(std::string socketPath)
{
    const int fd = connectToSocket(socketPath, running);
    if (fd < 0)
    {
        return;
    }

    readSocketLines(fd, running, socketPath, [this](const std::string& line)
    {
        // CSV: timestamp_ms,query_id,pipeline_id,compile_time_ns
        std::istringstream iss(line);
        std::string tsStr, queryId, pipelineIdStr, compileTimeStr;
        if (!std::getline(iss, tsStr, ',') || !std::getline(iss, queryId, ',')
            || !std::getline(iss, pipelineIdStr, ',') || !std::getline(iss, compileTimeStr, ','))
        {
            return;
        }

        uint64_t compileTimeNs = 0;
        try { compileTimeNs = std::stoull(compileTimeStr); }
        catch (...) { return; }

        std::lock_guard lock(compilationMutex);
        auto& stats = compilationStatsPerQuery[queryId];
        stats.totalCompileTimeNs += compileTimeNs;
        stats.pipelineCount++;
    });
}

std::vector<QueryStatusEvent> SystemSocketReader::waitForTerminalEvents()
{
    std::unique_lock lock(statusMutex);
    cv.wait(lock, [this] { return !terminalEvents.empty() || !running.load(); });

    std::vector<QueryStatusEvent> result;
    while (!terminalEvents.empty())
    {
        result.push_back(std::move(terminalEvents.front()));
        terminalEvents.pop();
    }
    return result;
}

CompilationStats SystemSocketReader::getCompilationStats(const std::string& localQueryId) const
{
    std::lock_guard lock(compilationMutex);
    auto it = compilationStatsPerQuery.find(localQueryId);
    if (it != compilationStatsPerQuery.end())
    {
        return it->second;
    }
    return {};
}

void SystemSocketReader::ingestionReaderThread(std::string socketPath)
{
    const int fd = connectToSocket(socketPath, running);
    if (fd < 0)
    {
        return;
    }

    readSocketLines(fd, running, socketPath, [this](const std::string& line)
    {
        // CSV: query_id,start,end,total_tuples,ingestion_count
        std::istringstream iss(line);
        std::string queryId, startStr, endStr, totalStr, countStr;
        if (!std::getline(iss, queryId, ',') || !std::getline(iss, startStr, ',')
            || !std::getline(iss, endStr, ',') || !std::getline(iss, totalStr, ',')
            || !std::getline(iss, countStr, ','))
        {
            return;
        }

        uint64_t totalTuples = 0, ingestionCount = 0;
        try
        {
            totalTuples = std::stoull(totalStr);
            ingestionCount = std::stoull(countStr);
        }
        catch (...) { return; }

        std::lock_guard lock(ingestionMutex);
        auto& stats = ingestionStatsPerQuery[queryId];
        stats.totalTuples += totalTuples;
        stats.totalBuffers += ingestionCount;
        stats.windowCount++;
    });
}

IngestionStats SystemSocketReader::getIngestionStats(const std::string& localQueryId) const
{
    std::lock_guard lock(ingestionMutex);
    auto it = ingestionStatsPerQuery.find(localQueryId);
    if (it != ingestionStatsPerQuery.end())
    {
        return it->second;
    }
    return {};
}

}
