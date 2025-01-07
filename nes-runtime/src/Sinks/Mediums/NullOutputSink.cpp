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

#include <Runtime/QueryManager.hpp>
#include <Runtime/WorkerContext.hpp>
#include <Sinks/Mediums/MultiOriginWatermarkProcessor.hpp>
#include <Sinks/Mediums/NullOutputSink.hpp>
#include <Util/Logger/Logger.hpp>
#include <sstream>
#include <string>

namespace NES {
NullOutputSink::NullOutputSink(Runtime::NodeEnginePtr nodeEngine,
                               uint32_t numOfProducers,
                               SharedQueryId sharedQueryId,
                               DecomposedQueryId decomposedQueryId,
                               DecomposedQueryPlanVersion decomposedQueryVersion,
                               FaultToleranceType faultToleranceType,
                               uint64_t numberOfOrigins)
    : SinkMedium(nullptr,
                 std::move(nodeEngine),
                 numOfProducers,
                 sharedQueryId,
                 decomposedQueryId,
                 decomposedQueryVersion,
                 faultToleranceType,
                 numberOfOrigins,
                 std::make_unique<Windowing::MultiOriginWatermarkProcessor>(numberOfOrigins)) {
    if (faultToleranceType == FaultToleranceType::AS || faultToleranceType == FaultToleranceType::UB) {
        updateWatermarkCallback = [this](Runtime::TupleBuffer& inputBuffer) {
            updateWatermark(inputBuffer);
        };
    }
    else if (faultToleranceType == FaultToleranceType::M){
        updateWatermarkCallback = [this](Runtime::TupleBuffer& inputBuffer) {
            watermarkProcessor->updateWatermark(inputBuffer.getWatermark(), inputBuffer.getSequenceNumber(), inputBuffer.getOriginId());
        };
    }
    else {
        updateWatermarkCallback = [](Runtime::TupleBuffer&) {
        };
    }
    if (faultToleranceType == FaultToleranceType::M || faultToleranceType == FaultToleranceType::AS) {
        duplicateDetectionCallback = [this](Runtime::TupleBuffer& inputBuffer){
            return watermarkProcessor->isDuplicate(inputBuffer.getSequenceNumber(), inputBuffer.getOriginId());
        };
    }
    else {
        duplicateDetectionCallback = [](Runtime::TupleBuffer&){
            return false;
        };
    }
}

NullOutputSink::~NullOutputSink() = default;

SinkMediumTypes NullOutputSink::getSinkMediumType() { return SinkMediumTypes::NULL_SINK; }

bool NullOutputSink::writeData(Runtime::TupleBuffer& inputBuffer, Runtime::WorkerContextRef workerContext) {
    workerContext.printStatistics(inputBuffer);
    updateWatermarkCallback(inputBuffer);
    if(!duplicateDetectionCallback(inputBuffer)) {
        return true;
    }
    return true;
}

std::string NullOutputSink::toString() const {
    std::stringstream ss;
    ss << "NULL_SINK(";
    ss << ")";
    return ss.str();
}

void NullOutputSink::setup() {
    // currently not required
}
void NullOutputSink::shutdown() {
    // currently not required
}

}// namespace NES
