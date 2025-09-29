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

#include <thread>
#include <SliceStore/Slice.hpp>

namespace NES
{

enum class FileOperation : uint8_t;
enum class OperationStatus : uint8_t;

class AsyncLogger
{
public:
    struct LatencyParams
    {
        LatencyParams(
            const WorkerThreadId threadId,
            const FileOperation operation,
            const std::chrono::system_clock::time_point start,
            const std::chrono::system_clock::time_point end)
            : threadId(threadId), operation(operation), start(start), end(end)
        {
        }
        LatencyParams() = default;

        WorkerThreadId threadId = WorkerThreadId(WorkerThreadId::INVALID);
        FileOperation operation = static_cast<FileOperation>(0);
        std::chrono::system_clock::time_point start = std::chrono::system_clock::now();
        std::chrono::system_clock::time_point end = std::chrono::system_clock::now();
    };

    struct SliceAccessParams
    {
        SliceAccessParams(
            const std::chrono::system_clock::time_point timestamp,
            const WorkerThreadId threadId,
            const FileOperation operation,
            const OperationStatus status,
            const SliceEnd sliceEnd,
            const bool prediction)
            : timestamp(timestamp), threadId(threadId), operation(operation), status(status), sliceEnd(sliceEnd), prediction(prediction)
        {
        }
        SliceAccessParams() = default;

        std::chrono::system_clock::time_point timestamp = std::chrono::system_clock::now();
        WorkerThreadId threadId = WorkerThreadId(WorkerThreadId::INVALID);
        FileOperation operation = static_cast<FileOperation>(0);
        OperationStatus status = static_cast<OperationStatus>(0);
        SliceEnd sliceEnd = SliceEnd(SliceEnd::INVALID_VALUE);
        bool prediction = false;
    };

    using LoggingParams = std::variant<LatencyParams, SliceAccessParams>;

    explicit AsyncLogger(const std::vector<std::string>& paths);
    ~AsyncLogger();

    void log(LoggingParams params);

private:
    void processLogs(const std::string& path);

    folly::MPMCQueue<LoggingParams> queue{100000};
    std::vector<std::thread> threads;
    std::atomic<bool> running;
};

}
