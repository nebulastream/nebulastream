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

#include <SliceStore/Logger/AsyncLogger.hpp>

namespace NES
{

AsyncLogger::AsyncLogger(const std::vector<std::string>& fileNames) : stopLogging(false), nextQueue(0)
{
    logQueues.resize(fileNames.size());
    queueMutexes = std::vector<std::mutex>(fileNames.size());
    cvs = std::vector<std::condition_variable>(fileNames.size());
    for (size_t i = 0; i < fileNames.size(); ++i)
    {
        loggingThreads.emplace_back(&AsyncLogger::processLogs, this, fileNames[i], i);
    }
}

AsyncLogger::~AsyncLogger()
{
    stopLogging = true;
    for (auto& cv : cvs)
    {
        cv.notify_all();
    }
    for (auto& thread : loggingThreads)
    {
        if (thread.joinable())
        {
            thread.join();
        }
    }
}

void AsyncLogger::log(const std::string& message)
{
    const size_t queueIndex = nextQueue++ % loggingThreads.size();
    {
        const std::lock_guard lock(queueMutexes[queueIndex]);
        logQueues[queueIndex].push(message);
    }
    cvs[queueIndex].notify_one();
}

void AsyncLogger::processLogs(const std::string& fileName, const size_t queueIndex)
{
    std::ofstream logFile(fileName, std::ios::out | std::ios::app);
    if (not logFile.is_open())
    {
        throw std::runtime_error("Failed to open log file: " + fileName);
    }

    while (not stopLogging or not logQueues[queueIndex].empty())
    {
        std::string message;
        {
            std::unique_lock lock(queueMutexes[queueIndex]);
            cvs[queueIndex].wait(lock, [this, queueIndex] { return stopLogging or not logQueues[queueIndex].empty(); });

            if (not logQueues[queueIndex].empty())
            {
                message = std::move(logQueues[queueIndex].front());
                logQueues[queueIndex].pop();
            }
        }

        if (not message.empty())
        {
            logFile << message << '\n';
        }
    }
}

}
