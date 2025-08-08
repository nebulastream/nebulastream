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

#include <SliceStore/FileBackedTimeBasedSliceStore.hpp>
#include <SliceStore/Logger/AsyncLogger.hpp>
#include <magic_enum/magic_enum.hpp>

namespace NES
{

AsyncLogger::AsyncLogger(const std::string& path)
    : loggingThread([this](const std::stop_token& stopToken) { processLogs(stopToken); }), file(path, std::ios::out | std::ios::app)
//, stopLogging(false)
//, nextQueue(0)
{
    /*logQueues.resize(fileNames.size());
    queueMutexes = std::vector<std::mutex>(fileNames.size());
    cvs = std::vector<std::condition_variable>(fileNames.size());
    for (size_t i = 0; i < fileNames.size(); ++i)
    {
        loggingThreads.emplace_back(&AsyncLogger::processLogs, this, fileNames[i], i);
    }*/
    //loggingThread = std::thread(&AsyncLogger::processLogs, this, fileNames[0], 0);

    if (not file.is_open())
    {
        throw std::runtime_error("Failed to open log file: " + path);
    }
}

void AsyncLogger::log(const LoggingParams& params)
{
    /*const size_t queueIndex = nextQueue++ % loggingThreads.size();
    {
        const std::lock_guard lock(queueMutexes[queueIndex]);
        logQueues[queueIndex].push(message);
    }
    cvs[queueIndex].notify_one();*/

    const std::lock_guard lock(queueMutex);
    logQueue.push(params);
}

void AsyncLogger::processLogs(const std::stop_token& token)
{
    /*while (not stopLogging or not logQueues[queueIndex].empty())
    {
        std::string message;

        std::unique_lock lock(queueMutexes[queueIndex]);
        cvs[queueIndex].wait(lock, [this, queueIndex] { return stopLogging or not logQueues[queueIndex].empty(); });
        if (not logQueues[queueIndex].empty())
        {
            message = std::move(logQueues[queueIndex].front());
            logQueues[queueIndex].pop();
        }
        lock.unlock();

        if (not message.empty())
        {
            logFile << message << '\n';
        }
    }*/
    while (not token.stop_requested())
    {
        std::string message;

        const std::lock_guard lock(queueMutex);
        while (not logQueue.empty())
        {
            const auto [timestamp, operation, status, sliceEnd] = std::move(logQueue.front());
            logQueue.pop();
            message = fmt::format(
                "{:%Y-%m-%d %H:%M:%S} Executed operation {} with status {} on slice {}",
                timestamp,
                magic_enum::enum_name<FileOperation>(operation),
                magic_enum::enum_name<OperationStatus>(status),
                sliceEnd);
            file << message << '\n';
        }
    }
}
}
