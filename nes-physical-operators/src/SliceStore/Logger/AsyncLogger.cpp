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

AsyncLogger::AsyncLogger(const std::string& fileName) : stopLogging(false)
{
    logFile.open(fileName, std::ios::out | std::ios::app);
    loggingThread = std::thread(&AsyncLogger::processLogs, this);
}

AsyncLogger::~AsyncLogger()
{
    stopLogging = true;
    cv.notify_all();
    if (loggingThread.joinable())
    {
        loggingThread.join();
    }
    if (logFile.is_open())
    {
        logFile.close();
    }
}

void AsyncLogger::log(const std::string& message)
{
    {
        const std::lock_guard lock(queueMutex);
        logQueue.push(message);
    }
    cv.notify_one();
}

void AsyncLogger::processLogs()
{
    while (not stopLogging or not logQueue.empty())
    {
        std::unique_lock lock(queueMutex);
        cv.wait(lock, [this] { return stopLogging or not logQueue.empty(); });

        while (not logQueue.empty())
        {
            logFile << logQueue.front() << '\n';
            logQueue.pop();
        }
    }
}

}
