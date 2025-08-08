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
#include <SliceStore/Slice.hpp>

namespace NES
{

enum class FileOperation : uint8_t;
enum class OperationStatus : uint8_t;

class AsyncLogger
{
public:
    struct LoggingParams
    {
        const std::chrono::system_clock::time_point timestamp;
        const FileOperation operation;
        const OperationStatus status;
        const SliceEnd sliceEnd;
    };

    explicit AsyncLogger(const std::string& path);
    //~AsyncLogger();

    void log(const LoggingParams& params);

private:
    void processLogs(const std::stop_token& token);

    /*std::vector<std::queue<std::string>> logQueues;
    std::vector<std::mutex> queueMutexes;
    std::vector<std::condition_variable> cvs;
    std::vector<std::thread> loggingThreads;*/
    std::queue<LoggingParams> logQueue;
    std::mutex queueMutex;
    //std::condition_variable cv;
    //std::thread loggingThread;
    std::jthread loggingThread;
    //std::atomic<bool> stopLogging;
    //std::atomic<size_t> nextQueue;
    std::ofstream file;
};

}
