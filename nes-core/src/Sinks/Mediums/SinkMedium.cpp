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

#include <API/Schema.hpp>
#include <Components/NesWorker.hpp>
#include <Runtime/Execution/ExecutableQueryPlan.hpp>
#include <Runtime/NodeEngine.hpp>
#include <Runtime/QueryManager.hpp>
#include <Runtime/QueryTerminationType.hpp>
#include <Sinks/Mediums/SinkMedium.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES {

SinkMedium::SinkMedium(SinkFormatPtr sinkFormat,
#ifndef UNIKERNEL_SUPPORT_LIB
                       Runtime::NodeEnginePtr nodeEngine,
#endif
                       uint32_t numOfProducers,
                       QueryId queryId,
                       QuerySubPlanId querySubPlanId)
    : SinkMedium(sinkFormat,
#ifndef UNIKERNEL_SUPPORT_LIB
                 nodeEngine,
#endif
                 numOfProducers, queryId, querySubPlanId, FaultToleranceType::NONE, 1, nullptr) {}

SinkMedium::SinkMedium(SinkFormatPtr sinkFormat,
#ifndef UNIKERNEL_SUPPORT_LIB
                       Runtime::NodeEnginePtr nodeEngine,
#endif
                       uint32_t numOfProducers,
                       QueryId queryId,
                       QuerySubPlanId querySubPlanId,
                       FaultToleranceType faultToleranceType,
                       uint64_t numberOfOrigins,
                       Windowing::MultiOriginWatermarkProcessorPtr watermarkProcessor)
    : sinkFormat(std::move(sinkFormat)),
#ifndef UNIKERNEL_SUPPORT_LIB
      nodeEngine(std::move(nodeEngine)),
#endif
      activeProducers(numOfProducers), queryId(queryId), querySubPlanId(querySubPlanId), faultToleranceType(faultToleranceType),
      numberOfOrigins(numberOfOrigins), watermarkProcessor(std::move(watermarkProcessor)) {
    bufferCount = 0;
#ifndef UNIKERNEL_SUPPORT_LIB
    NES_ASSERT2_FMT(this->nodeEngine, "Invalid node engine");
    buffersPerEpoch = this->nodeEngine->getQueryManager()->getNumberOfBuffersPerEpoch();
#else
    buffersPerEpoch = 1000;//TODO
#endif
    schemaWritten = false;
    NES_ASSERT2_FMT(numOfProducers > 0, "Invalid num of producers on Sink");
    if (faultToleranceType == FaultToleranceType::AT_LEAST_ONCE) {
        updateWatermarkCallback = [this](Runtime::TupleBuffer& inputBuffer) {
            updateWatermark(inputBuffer);
        };
    } else {
        updateWatermarkCallback = [](Runtime::TupleBuffer&) {
        };
    }
}

OperatorId SinkMedium::getOperatorId() const { return 0; }

uint64_t SinkMedium::getNumberOfWrittenOutBuffers() {
    std::unique_lock lock(writeMutex);
    return sentBuffer;
}

void SinkMedium::updateWatermark(Runtime::TupleBuffer& inputBuffer) {
    NES_ASSERT(watermarkProcessor != nullptr, "SinkMedium::updateWatermark watermark processor is null");
    watermarkProcessor->updateWatermark(inputBuffer.getWatermark(), inputBuffer.getSequenceNumber(), inputBuffer.getOriginId());
    if (!(bufferCount % buffersPerEpoch) && bufferCount != 0) {
        auto timestamp = watermarkProcessor->getCurrentWatermark();
        if (timestamp) {
            notifyEpochTermination(timestamp);
        }
    }
    bufferCount++;
}

uint64_t SinkMedium::getNumberOfWrittenOutTuples() {
    std::unique_lock lock(writeMutex);
    return sentTuples;
}

SchemaPtr SinkMedium::getSchemaPtr() const { return sinkFormat->getSchemaPtr(); }

std::string SinkMedium::getSinkFormat() { return sinkFormat->toString(); }

QuerySubPlanId SinkMedium::getParentPlanId() const { return querySubPlanId; }

QueryId SinkMedium::getQueryId() const { return queryId; }

bool SinkMedium::notifyEpochTermination(uint64_t epochBarrier) const {
#ifndef UNIKERNEL_SUPPORT_LIB
    uint64_t queryId = nodeEngine->getQueryManager()->getQueryId(querySubPlanId);
    NES_ASSERT(queryId >= 0, "SinkMedium: no queryId found for querySubPlanId");
    if (auto listener = nodeEngine->getQueryStatusListener(); listener) {
        bool success = listener->notifyEpochTermination(epochBarrier, queryId);
        if (success) {
            return true;
        }
    }
    return false;
#else
    NES_DEBUG("EPOCH: {}", epochBarrier)
    return true;
#endif
}

void SinkMedium::reconfigure(Runtime::ReconfigurationMessage& message, Runtime::WorkerContext& context) {
    Reconfigurable::reconfigure(message, context);
}

uint64_t SinkMedium::getCurrentEpochBarrier() { return watermarkProcessor->getCurrentWatermark(); }

void SinkMedium::postReconfigurationCallback(Runtime::ReconfigurationMessage& message) {
    Reconfigurable::postReconfigurationCallback(message);
    Runtime::QueryTerminationType terminationType = Runtime::QueryTerminationType::Invalid;
    switch (message.getType()) {
        case Runtime::ReconfigurationType::FailEndOfStream: {
            terminationType = Runtime::QueryTerminationType::Failure;
            break;
        }
        case Runtime::ReconfigurationType::SoftEndOfStream: {
            terminationType = Runtime::QueryTerminationType::Graceful;
            break;
        }
        case Runtime::ReconfigurationType::HardEndOfStream: {
            terminationType = Runtime::QueryTerminationType::HardStop;
            break;
        }
        default: {
            break;
        }
    }
    if (terminationType != Runtime::QueryTerminationType::Invalid) {
        NES_DEBUG("Got EoS on Sink  {}", toString());
        if (activeProducers.fetch_sub(1) == 1) {
            shutdown();
#ifndef UNIKERNEL_SUPPORT_LIB
            nodeEngine->getQueryManager()->notifySinkCompletion(querySubPlanId,
                                                                std::static_pointer_cast<SinkMedium>(shared_from_this()),
                                                                terminationType);
#endif
            NES_DEBUG("Sink [ {} ] is completed with  {}", toString(), terminationType);
        }
    }
}
}// namespace NES
