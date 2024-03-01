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

#include <Sinks/Mediums/ThroughputSink.hpp>

std::mutex NES::ThroughputSink::mtx = std::mutex();
std::map<std::string, std::shared_ptr<std::atomic<size_t>>> NES::ThroughputSink::counter = {};

NES::ThroughputSink::ThroughputSink(std::string counterName,
                                    size_t reportingThreshhold,
                                    SinkFormatPtr sinkFormat,
                                    Runtime::NodeEnginePtr nodeEngine,
                                    uint32_t numOfProducers,
                                    SharedQueryId sharedQueryId,
                                    DecomposedQueryPlanId decomposedQueryPlanId)
    : SinkMedium(std::move(sinkFormat), std::move(nodeEngine), numOfProducers, sharedQueryId, decomposedQueryPlanId),
      reportingThreshhold(reportingThreshhold), counterName(std::move(counterName)) {}

void NES::ThroughputSink::setup() {
    std::unique_lock lock(mtx);
    if (!counter.contains(counterName)) {
        size_t initial_value = 0;
        counter.emplace(counterName, std::make_shared<std::atomic<size_t>>(initial_value));
    }
    sharedCounter = counter[counterName];
    localCounter = sharedCounter->load();
}

bool NES::ThroughputSink::writeData(Runtime::TupleBuffer& inputBuffer, Runtime::WorkerContext&) {
    const size_t numberOfTuples = inputBuffer.getNumberOfTuples();
    if (const auto currentCounter = sharedCounter->fetch_add(numberOfTuples);
        currentCounter % reportingThreshhold > ((currentCounter + numberOfTuples) % reportingThreshhold)) {
        auto numberOfTuplesReceived = (currentCounter + numberOfTuples) - localCounter;
        const auto currentTime = std::chrono::high_resolution_clock::now();
        const auto timeDiff = std::chrono::duration_cast<std::chrono::nanoseconds>(currentTime - time);
        double durationInSeconds = timeDiff.count() * 1.0e-9;

        NES_INFO("Throughput for {}: {} tps", counterName, static_cast<float>(numberOfTuplesReceived) / durationInSeconds);
        NES_DEBUG("Throughput for {}: Tuples {} in {}s", counterName, numberOfTuplesReceived, durationInSeconds);
        localCounter = currentCounter + numberOfTuples;
        time = currentTime;
    }

    return true;
}