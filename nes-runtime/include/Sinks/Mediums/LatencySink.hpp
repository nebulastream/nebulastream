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

#ifndef NES_RUNTIME_INCLUDE_SINKS_MEDIUMS_LATENCY_HPP_
#define NES_RUNTIME_INCLUDE_SINKS_MEDIUMS_LATENCY_HPP_
#include <Sinks/Mediums/SinkMedium.hpp>

#include <atomic>
#include <cstdint>
#include <fmt/core.h>
#include <map>
#include <memory>
#include <mutex>
#include <string>

namespace NES {
class LatencySink : public SinkMedium {
    std::chrono::milliseconds reportingThreshhold;

  public:
    explicit LatencySink(size_t reportingThreshholdInMs,
                         SinkFormatPtr sinkFormat,
                         Runtime::NodeEnginePtr nodeEngine,
                         uint32_t numOfProducers,
                         SharedQueryId sharedQueryId,
                         DecomposedQueryPlanId decomposedQueryPlanId);

    void setup() override;

    void shutdown() override {}
    bool writeData(Runtime::TupleBuffer& inputBuffer, Runtime::WorkerContext&) override;

    std::string toString() const override { return fmt::format("LATENCY_SINK({})", this->reportingThreshhold.count()); }
    SinkMediumTypes getSinkMediumType() override { return SinkMediumTypes::MONITORING_SINK; };
};
}// namespace NES
#endif//NES_RUNTIME_INCLUDE_SINKS_MEDIUMS_LATENCY_HPP_
