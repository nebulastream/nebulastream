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

#pragma once

#include <condition_variable>
#include <fstream>
#include <mutex>
#include <queue>
#include <thread>

namespace NES
{

class AsyncLogger
{
public:
    explicit AsyncLogger(const std::vector<std::string>& fileNames);
    ~AsyncLogger();

    void log(const std::string& message);

private:
    void processLogs(const std::string& fileName, size_t queueIndex);

    std::vector<std::queue<std::string>> logQueues;
    std::vector<std::mutex> queueMutexes;
    std::vector<std::condition_variable> cvs;
    std::vector<std::thread> loggingThreads;
    std::atomic<bool> stopLogging;
    std::atomic<size_t> nextQueue;
};

}
