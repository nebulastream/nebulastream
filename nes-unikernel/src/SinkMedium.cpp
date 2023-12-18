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
#include <Runtime/NodeEngine.hpp>
#include <Runtime/ReconfigurationMessage.hpp>
#include <Runtime/QueryTerminationType.hpp>
#include <Sinks/Mediums/SinkMedium.hpp>
#include <Util/Logger/Logger.hpp>
namespace NES {

SinkMedium::SinkMedium(uint32_t numOfProducers,
                       QueryId queryId,
                       QuerySubPlanId querySubPlanId,
                       FaultToleranceType faultToleranceType,
                       uint64_t numberOfOrigins)
    : queryId(queryId), querySubPlanId(querySubPlanId), faultToleranceType(faultToleranceType), numberOfOrigins(numberOfOrigins) {
    bufferCount = 0;
    buffersPerEpoch = 1000;//TODO

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

uint64_t SinkMedium::getNumberOfWrittenOutBuffers() { return sentBuffer; }

void SinkMedium::updateWatermark([[maybe_unused]] Runtime::TupleBuffer& inputBuffer) {
    NES_DEBUG("Unikernel Not Implemented update Watermark");
}

uint64_t SinkMedium::getNumberOfWrittenOutTuples() { return sentTuples; }

QuerySubPlanId SinkMedium::getParentPlanId() const { return querySubPlanId; }

QueryId SinkMedium::getQueryId() const { return queryId; }

bool SinkMedium::notifyEpochTermination(uint64_t epochBarrier) const {
    NES_DEBUG("EPOCH: {}", epochBarrier)
    return true;
}

void SinkMedium::reconfigure(Runtime::ReconfigurationMessage& message, Runtime::WorkerContext& context) {
    Reconfigurable::reconfigure(message, context);
}

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
        shutdown();
        NES_DEBUG("Sink [ {} ] is completed with  {}", toString(), terminationType);
    }
}
}// namespace NES
