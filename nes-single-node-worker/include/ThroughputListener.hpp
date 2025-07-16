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

#include <filesystem>
#include <fstream>
#include <thread>
#include <Runtime/BufferManager.hpp>
#include <folly/MPMCQueue.h>
#include <QueryEngineStatisticListener.hpp>

namespace NES
{

class ThroughputListener : public QueryEngineStatisticListener
{
public:
    struct CallBackParams
    {
        const QueryId queryId;
        const Timestamp windowStart;
        const Timestamp windowEnd;
        const double throughputInBytesPerSec;
        const double throughputInTuplesPerSec;
        const size_t memoryConsumption;
    };

    void onEvent(Event event) override;
    explicit ThroughputListener(
        const std::filesystem::path& path,
        Timestamp::Underlying timeIntervalInMilliSeconds,
        const std::function<void(std::ofstream&, const CallBackParams&)>& callBack);

    void setBufferManager(std::shared_ptr<Memory::BufferManager> bufferManager);

private:
    std::ofstream file;
    folly::MPMCQueue<Event> events{100};
    const Timestamp::Underlying timeIntervalInMilliSeconds;

    /// BufferManager is needed to record memory consumption
    std::shared_ptr<Memory::BufferManager> bufferManager;

    /// We need to store the callback, as it might go out of scope
    std::function<void(std::ofstream&, const CallBackParams&)> callBack;
    std::jthread calculateThread;
};

}
