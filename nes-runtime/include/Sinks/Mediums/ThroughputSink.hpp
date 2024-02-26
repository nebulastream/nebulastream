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

#ifndef NES_RUNTIME_INCLUDE_SINKS_MEDIUMS_THROUGHPUTSINK_HPP_
#define NES_RUNTIME_INCLUDE_SINKS_MEDIUMS_THROUGHPUTSINK_HPP_
#include "SinkMedium.hpp"

#include <atomic>
#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <fmt/core.h>

namespace NES {
class ThroughputSink : public SinkMedium {
    static std::mutex mtx;
    static std::map<std::string, std::shared_ptr<std::atomic<size_t>>> counter;

    std::shared_ptr<std::atomic<size_t>> sharedCounter;
    size_t localCounter;
    size_t reportingThreshhold;
    std::string counterName;
    std::chrono::time_point<std::chrono::system_clock> time = std::chrono::high_resolution_clock::now();

  public:
    explicit ThroughputSink(std::string counterName,
                            size_t reportingThreshhold,
                            SinkFormatPtr sinkFormat,
                            Runtime::NodeEnginePtr nodeEngine,
                            uint32_t numOfProducers,
                            SharedQueryId sharedQueryId,
                            DecomposedQueryPlanId decomposedQueryPlanId);

    void setup() override;

    void shutdown() override {}
    bool writeData(Runtime::TupleBuffer& inputBuffer, Runtime::WorkerContext&) override;
    size_t getCurrentCounter() const { return sharedCounter->load(); }

    std::string toString() const override { return fmt::format("THROUGHPUT_SINK({})", counterName); }
    SinkMediumTypes getSinkMediumType() override { return SinkMediumTypes::MONITORING_SINK; };
};
}// namespace NES
#endif//NES_RUNTIME_INCLUDE_SINKS_MEDIUMS_THROUGHPUTSINK_HPP_
