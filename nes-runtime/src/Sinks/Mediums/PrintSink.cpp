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
#include <Sinks/Mediums/PrintSink.hpp>
#include <Util/Logger/Logger.hpp>
#include <sstream>
#include <string>
#include <utility>

namespace NES {
PrintSink::PrintSink(SinkFormatPtr format,
                     Runtime::NodeEnginePtr nodeEngine,
                     uint32_t numOfProducers,
                     SharedQueryId sharedQueryId,
                     DecomposedQueryId decomposedQueryId,
                     DecomposedQueryPlanVersion decomposedQueryVersion,
                     std::ostream& pOutputStream,
                     FaultToleranceType faultToleranceType,
                     CheckpointStorageType checkpointStorageType,
                     uint64_t numberOfOrigins)
    : SinkMedium(std::move(format),
                 std::move(nodeEngine),
                 numOfProducers,
                 sharedQueryId,
                 decomposedQueryId,
                 decomposedQueryVersion,
                 faultToleranceType,
                 checkpointStorageType,
                 numberOfOrigins, std::make_unique<Windowing::MultiOriginWatermarkProcessor>(numberOfOrigins)),
      outputStream(pOutputStream) {
    if (faultToleranceType == FaultToleranceType::M) {
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

PrintSink::~PrintSink() = default;

SinkMediumTypes PrintSink::getSinkMediumType() { return SinkMediumTypes::PRINT_SINK; }

bool PrintSink::writeData(Runtime::TupleBuffer& inputBuffer, Runtime::WorkerContextRef workerContext) {
    std::unique_lock lock(writeMutex);
    if (!inputBuffer) {
        throw Exceptions::RuntimeException("PrintSink::writeData input buffer invalid");
    }
    NES_TRACE("PrintSink::getData: write data");
    auto buffer = sinkFormat->getFormattedBuffer(inputBuffer);
    NES_TRACE("PrintSink::getData: write buffer of size  {}", buffer.size());
    if(!duplicateDetectionCallback(inputBuffer)) {
        updateWatermarkCallback(inputBuffer);
        workerContext.printStatistics(inputBuffer, buffer);
        outputStream << buffer << std::endl;
    }
    return true;
}

std::string PrintSink::toString() const {
    std::stringstream ss;
    ss << "PRINT_SINK(";
    ss << "SCHEMA(" << sinkFormat->getSchemaPtr()->toString() << ")";
    ss << ")";
    return ss.str();
}

void PrintSink::setup() {
    // currently not required
}
void PrintSink::shutdown() {
    // currently not required
}

}// namespace NES
