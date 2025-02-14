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
#include <Reconfiguration/Metadata/DrainQueryMetadata.hpp>
#include <Reconfiguration/Metadata/UpdateAndDrainQueryMetadata.hpp>
#include <Runtime/NodeEngine.hpp>
#include <Runtime/QueryManager.hpp>
#include <Runtime/QueryTerminationType.hpp>
#include <Sinks/Mediums/SinkMedium.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES {

SinkMedium::SinkMedium(SinkFormatPtr sinkFormat,
                       Runtime::NodeEnginePtr nodeEngine,
                       uint32_t numOfProducers,
                       SharedQueryId sharedQueryId,
                       DecomposedQueryId decomposedQueryId,
                       DecomposedQueryPlanVersion decomposedQueryVersion)
    : SinkMedium(sinkFormat, nodeEngine, numOfProducers, sharedQueryId, decomposedQueryId, decomposedQueryVersion, 1) {}

SinkMedium::SinkMedium(SinkFormatPtr sinkFormat,
                       Runtime::NodeEnginePtr nodeEngine,
                       uint32_t numOfProducers,
                       SharedQueryId sharedQueryId,
                       DecomposedQueryId decomposedQueryId,
                       DecomposedQueryPlanVersion decomposedQueryVersion,
                       uint64_t numberOfOrigins)
    : sinkFormat(std::move(sinkFormat)), nodeEngine(std::move(nodeEngine)), activeProducers(numOfProducers),
      sharedQueryId(sharedQueryId), decomposedQueryId(decomposedQueryId), decomposedQueryVersion(decomposedQueryVersion),
      numberOfOrigins(numberOfOrigins) {
    schemaWritten = false;
    NES_ASSERT2_FMT(numOfProducers > 0, "Invalid num of producers on Sink");
    NES_ASSERT2_FMT(this->nodeEngine, "Invalid node engine");
}

OperatorId SinkMedium::getOperatorId() const { return INVALID_OPERATOR_ID; }

uint64_t SinkMedium::getNumberOfWrittenOutBuffers() {
    std::unique_lock lock(writeMutex);
    return sentBuffer;
}

uint64_t SinkMedium::getNumberOfWrittenOutTuples() {
    std::unique_lock lock(writeMutex);
    return sentTuples;
}

SchemaPtr SinkMedium::getSchemaPtr() const { return sinkFormat->getSchemaPtr(); }

std::string SinkMedium::getSinkFormat() { return sinkFormat->toString(); }

DecomposedQueryId SinkMedium::getParentPlanId() const { return decomposedQueryId; }

DecomposedQueryPlanVersion SinkMedium::getParentPlanVersion() const { return decomposedQueryVersion; }

SharedQueryId SinkMedium::getSharedQueryId() const { return sharedQueryId; }

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
        case Runtime::ReconfigurationType::ReconfigurationMarker: {
            auto marker = message.getUserData<ReconfigurationMarkerPtr>();
            auto event = marker->getReconfigurationEvent(DecomposedQueryIdWithVersion(decomposedQueryId, decomposedQueryVersion));
            NES_ASSERT2_FMT(event, "Markers should only be propageted to a network sink if the plan is to be reconfigured");

            if (!event.value()->reconfigurationMetadata->instanceOf<DrainQueryMetadata>()
                && !event.value()->reconfigurationMetadata->instanceOf<UpdateAndDrainQueryMetadata>()) {
                NES_WARNING("Non drain reconfigurations not yes supported");
                NES_NOT_IMPLEMENTED();
            }

            //todo #5119: handle other cases
            terminationType = Runtime::reconfigurationTypeToTerminationType(message.getType());
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
            nodeEngine->getQueryManager()->notifySinkCompletion(decomposedQueryId,
                                                                decomposedQueryVersion,
                                                                std::static_pointer_cast<SinkMedium>(shared_from_this()),
                                                                terminationType);
            NES_DEBUG("Sink [ {} ] is completed with  {}", toString(), terminationType);
        }
    }
}

void SinkMedium::setMigrationFlag() { migration = true; }

bool SinkMedium::isForMigration() const { return migration; }

void SinkMedium::setReplayData() {
    NES_ERROR("setting source to replay data")
    replayData = true;
}

bool SinkMedium::getReplayData() { return replayData; }

}// namespace NES
