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

AsyncLogger::AsyncLogger(const std::vector<std::string>& paths) : running(true)
{
    for (const auto& path : paths)
    {
        threads.emplace_back(&AsyncLogger::processLogs, this, path);
    }
}

AsyncLogger::~AsyncLogger()
{
    running = false;
    for (auto& thread : threads)
    {
        if (thread.joinable())
        {
            thread.join();
        }
    }
}

void AsyncLogger::log(LoggingParams params)
{
    while (not queue.writeIfNotFull(params))
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
}

void AsyncLogger::processLogs(const std::string& path)
{
    std::ofstream file;
    file.open(path, std::ios::out | std::ios::app);
    if (not file.is_open())
    {
        throw std::runtime_error("Failed to open log file: " + path);
    }

    LoggingParams params{LatencyParams()};
    while (running)
    {
        if (queue.readIfNotEmpty(params)) [[likely]]
        {
            if (std::holds_alternative<LatencyParams>(params))
            {
                const auto latencyParams = std::get<LatencyParams>(params);
                file << fmt::format(
                    "Thread {} executed operation {} starting {:%Y-%m-%d %H:%M:%S} and ending {:%Y-%m-%d %H:%M:%S}\n",
                    latencyParams.threadId,
                    magic_enum::enum_name<FileOperation>(latencyParams.operation),
                    latencyParams.start,
                    latencyParams.end);
            }
            else
            {
                const auto sliceAccessParams = std::get<SliceAccessParams>(params);
                file << fmt::format(
                    "{:%Y-%m-%d %H:%M:%S} Thread {} executed operation {} with status {} on slice {} {} prediction\n",
                    sliceAccessParams.timestamp,
                    sliceAccessParams.threadId,
                    magic_enum::enum_name<FileOperation>(sliceAccessParams.operation),
                    magic_enum::enum_name<OperationStatus>(sliceAccessParams.status),
                    sliceAccessParams.sliceEnd,
                    sliceAccessParams.prediction ? "with" : "without");
            }
        }
        else
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
    }
    file.flush();
}
}
