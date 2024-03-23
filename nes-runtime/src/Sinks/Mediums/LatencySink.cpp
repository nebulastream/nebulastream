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

#include <Sinks/Formats/SinkFormat.hpp>
#include <Sinks/Mediums/LatencySink.hpp>

NES::LatencySink::LatencySink(size_t reportingThreshholdInMs,
                              SinkFormatPtr sinkFormat,
                              Runtime::NodeEnginePtr nodeEngine,
                              uint32_t numOfProducers,
                              SharedQueryId sharedQueryId,
                              DecomposedQueryPlanId decomposedQueryPlanId)
    : SinkMedium(std::move(sinkFormat), std::move(nodeEngine), numOfProducers, sharedQueryId, decomposedQueryPlanId),
      reportingThreshhold(std::chrono::milliseconds(reportingThreshholdInMs)) {}

void NES::LatencySink::setup() {}

bool NES::LatencySink::writeData(Runtime::TupleBuffer& inputBuffer, Runtime::WorkerContext&) {
    NES_ASSERT(inputBuffer.getNumberOfTuples() == 1, "Expected a single tuple with the timestamp");
    auto epoch = std::chrono::time_point<std::chrono::high_resolution_clock>();
    auto since_epoch = std::chrono::microseconds(*reinterpret_cast<uint64_t*>(inputBuffer.getBuffer()));
    auto timestamp = epoch + since_epoch;
    auto diff = std::chrono::high_resolution_clock::now() - timestamp;
    if (diff > reportingThreshhold) {
        NES_WARNING("Latency: {}", std::chrono::duration_cast<std::chrono::milliseconds>(diff).count());
    }

    return true;
}