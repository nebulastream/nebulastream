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
    : file(path, std::ios::out | std::ios::app), thread(&AsyncLogger::processLogs, this, std::stop_token{})
{
    if (not file.is_open())
    {
        throw std::runtime_error("Failed to open log file: " + path);
    }
}

AsyncLogger::~AsyncLogger()
{
    thread.request_stop();
    thread.join();
}

void AsyncLogger::log(LoggingParams params)
{
    if (not queue.writeIfNotFull(std::move(params)))
    {
        //throw std::runtime_error("Log queue is full, dropping log entry\n");
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
}

void AsyncLogger::processLogs(const std::stop_token& token)
{
    LoggingParams params;
    while (not token.stop_requested())
    {
        if (queue.read(params))
        {
            file << fmt::format(
                "{:%Y-%m-%d %H:%M:%S} Executed operation {} with status {} on slice {}",
                params.timestamp,
                magic_enum::enum_name<FileOperation>(params.operation),
                magic_enum::enum_name<OperationStatus>(params.status),
                params.sliceEnd);
            file.flush();
        }
        else
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
    }
}
}
