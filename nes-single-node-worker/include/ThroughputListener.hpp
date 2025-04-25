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
#include <folly/MPMCQueue.h>
#include <QueryEngineStatisticListener.hpp>


namespace NES::Runtime
{

class ThroughputListener : public QueryEngineStatisticListener
{
    std::ofstream file;
    folly::MPMCQueue<Event> events{100};
    const Timestamp::Underlying timeIntervalInMilliSeconds; // todo change this to std::milliseconds from std::chrono
    const std::function<void(const QueryId&, const Timestamp&, const Timestamp&, double)>& callBack;
    std::jthread calculateThread;


public:
    void onEvent(Event event) override;
    explicit ThroughputListener(
        const Timestamp::Underlying timeIntervalInMilliSeconds, const std::function<void(const QueryId&, const Timestamp&, const Timestamp&, double)>& callBack);
};

}
