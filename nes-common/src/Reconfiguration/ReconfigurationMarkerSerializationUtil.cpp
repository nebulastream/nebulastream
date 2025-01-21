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

#include <Reconfiguration/Metadata/DrainQueryMetadata.hpp>
#include <Reconfiguration/Metadata/UpdateAndDrainQueryMetadata.hpp>
#include <Reconfiguration/Metadata/UpdateQueryMetadata.hpp>
#include <Reconfiguration/ReconfigurationMarker.hpp>
#include <Reconfiguration/ReconfigurationMarkerSerializationUtil.hpp>
#include <SerializableReconfigurationMarker.pb.h>
#include <Util/Logger/Logger.hpp>
#include <Util/QueryStateSerializationUtil.hpp>

namespace NES {

void ReconfigurationMarkerSerializationUtil::serialize(const ReconfigurationMarkerPtr& reconfigurationMarker,
                                                       SerializableReconfigurationMarker& serializableReconfigurationMarker) {
    NES_DEBUG("Serialize reconfiguration marker.");
    auto& mutableReconfigurationMarkerEvents = *serializableReconfigurationMarker.mutable_reconfigurationmarkerevents();

    for (const auto& [decomposedQueryIdWithVersion, value] : reconfigurationMarker->getAllReconfigurationMarkerEvents()) {

        auto reconfigurationMetadataType = value->reconfigurationMetadata->reconfigurationMetadataType;
        auto queryState = value->queryState;
        switch (reconfigurationMetadataType) {
            case ReconfigurationMetadataType::DrainQuery: {
                auto drainQueryMetadata = value->reconfigurationMetadata->as<DrainQueryMetadata>();
                auto serializableReconfigurationMetadata = SerializableReconfigurationMarkerEvent();
                auto serializableDrainQueryMetadata = SerializableReconfigurationMetadata_SerializableDrainQueryMetadata();
                serializableDrainQueryMetadata.set_numberofsources(drainQueryMetadata->numberOfSources);
                serializableReconfigurationMetadata.mutable_reconfigurationmetadata()->mutable_details()->PackFrom(
                    serializableDrainQueryMetadata);
                serializableReconfigurationMetadata.set_querystate(QueryStateSerializationUtil::serializeQueryState(queryState));
                mutableReconfigurationMarkerEvents[packDecomposedPlanAsString(decomposedQueryIdWithVersion.id.getRawValue(),
                                                                              decomposedQueryIdWithVersion.version)] =
                    serializableReconfigurationMetadata;
                break;
            }
            case ReconfigurationMetadataType::UpdateQuery: {
                auto updateQueryMetadata = value->reconfigurationMetadata->as<UpdateQueryMetadata>();
                auto serializableReconfigurationMetadata = SerializableReconfigurationMarkerEvent();
                auto serializableUpdateQueryMetadata = SerializableReconfigurationMetadata_SerializableUpdateQueryMetadata();

                auto mutableSinkUpdates = serializableUpdateQueryMetadata.mutable_sinkupdates();
                for (auto update : updateQueryMetadata->networkSinkUpdates) {
                    auto serializableUpdate = mutableSinkUpdates->Add();
                    auto nodeLocation = serializableUpdate->mutable_nodelocation();
                    nodeLocation->set_hostname(update.nodeLocation.hostname);
                    nodeLocation->set_nodeid(update.nodeLocation.workerId.getRawValue());
                    nodeLocation->set_port(update.nodeLocation.port);

                    auto nesPartition = serializableUpdate->mutable_nespartition();
                    nesPartition->set_operatorid(update.nesPartition.operatorId.getRawValue());
                    nesPartition->set_partitionid(update.nesPartition.partitionId.getRawValue());
                    nesPartition->set_subpartitionid(update.nesPartition.subpartitionId.getRawValue());
                    nesPartition->set_queryid(update.nesPartition.sharedQueryId.getRawValue());

                    serializableUpdate->set_waittime(update.waitTime.count());
                    serializableUpdate->set_retrytimes(update.retryTimes);
                    serializableUpdate->set_version(update.version);
                    serializableUpdate->set_uniquenetworksinkdescriptorid(update.uniqueNetworkSinkId.getRawValue());
                    serializableUpdate->set_numberoforigins(update.numberOfOrigins);
                }

                serializableUpdateQueryMetadata.set_sharedqueryid(updateQueryMetadata->sharedQueryId.getRawValue());
                serializableUpdateQueryMetadata.set_decomposedqueryid(updateQueryMetadata->decomposedQueryId.getRawValue());
                serializableUpdateQueryMetadata.set_workerid(updateQueryMetadata->workerId.getRawValue());
                serializableUpdateQueryMetadata.set_decomposedqueryplanversion(updateQueryMetadata->decomposedQueryPlanVersion);
                serializableReconfigurationMetadata.mutable_reconfigurationmetadata()->mutable_details()->PackFrom(
                    serializableUpdateQueryMetadata);
                serializableReconfigurationMetadata.set_querystate(QueryStateSerializationUtil::serializeQueryState(queryState));
                mutableReconfigurationMarkerEvents[packDecomposedPlanAsString(decomposedQueryIdWithVersion.id.getRawValue(),
                                                                              decomposedQueryIdWithVersion.version)] =
                    serializableReconfigurationMetadata;
                break;
            }
            case ReconfigurationMetadataType::UpdateAndDrainQuery: {
                auto updateAndDrainQueryMetadata = value->reconfigurationMetadata->as<UpdateAndDrainQueryMetadata>();
                auto serializableReconfigurationMetadata = SerializableReconfigurationMarkerEvent();
                auto serializableUpdateAndDrainQueryMetadata =
                    SerializableReconfigurationMetadata_SerializableUpdateAndDrainMetadata();
                serializableUpdateAndDrainQueryMetadata.set_sharedqueryid(
                    updateAndDrainQueryMetadata->sharedQueryId.getRawValue());
                serializableUpdateAndDrainQueryMetadata.set_decomposedqueryid(
                    updateAndDrainQueryMetadata->decomposedQueryId.getRawValue());
                serializableUpdateAndDrainQueryMetadata.set_workerid(updateAndDrainQueryMetadata->workerId.getRawValue());
                serializableUpdateAndDrainQueryMetadata.set_decomposedqueryplanversion(
                    updateAndDrainQueryMetadata->decomposedQueryPlanVersion);
                serializableUpdateAndDrainQueryMetadata.set_numberofsources(updateAndDrainQueryMetadata->numberOfSources);
                serializableReconfigurationMetadata.mutable_reconfigurationmetadata()->mutable_details()->PackFrom(
                    serializableUpdateAndDrainQueryMetadata);
                serializableReconfigurationMetadata.set_querystate(QueryStateSerializationUtil::serializeQueryState(queryState));
                mutableReconfigurationMarkerEvents[packDecomposedPlanAsString(decomposedQueryIdWithVersion.id.getRawValue(),
                                                                              decomposedQueryIdWithVersion.version)] =
                    serializableReconfigurationMetadata;
                break;
            }
        }
    }
}

void ReconfigurationMarkerSerializationUtil::deserialize(
    const SerializableReconfigurationMarker& serializableReconfigurationMarker,
    ReconfigurationMarkerPtr reconfigurationMarker) {

    NES_DEBUG("Deserialize reconfiguration marker.");
    for (const auto& item : serializableReconfigurationMarker.reconfigurationmarkerevents()) {

        auto key = item.first;
        auto queryState = QueryStateSerializationUtil::deserializeQueryState(item.second.querystate());
        auto reconfigurationMetaData = item.second.reconfigurationmetadata();
        if (reconfigurationMetaData.details().Is<SerializableReconfigurationMetadata_SerializableDrainQueryMetadata>()) {
            auto serializedDrainQueryMetadata = SerializableReconfigurationMetadata_SerializableDrainQueryMetadata();
            reconfigurationMetaData.details().UnpackTo(&serializedDrainQueryMetadata);
            auto reConfMetaData = std::make_shared<DrainQueryMetadata>(serializedDrainQueryMetadata.numberofsources());
            auto markerEvent = ReconfigurationMarkerEvent::create(queryState, reConfMetaData);
            auto planWithVersion = unpackDecomposedPlanFromString(key);
            reconfigurationMarker->addReconfigurationEvent(planWithVersion.id, planWithVersion.version, markerEvent);
        } else if (reconfigurationMetaData.details()
                       .Is<SerializableReconfigurationMetadata_SerializableUpdateAndDrainMetadata>()) {
            auto serializedUpdateAndDrainQueryMetadata = SerializableReconfigurationMetadata_SerializableUpdateAndDrainMetadata();
            reconfigurationMetaData.details().UnpackTo(&serializedUpdateAndDrainQueryMetadata);
            auto reConfMetaData = std::make_shared<UpdateAndDrainQueryMetadata>(
                WorkerId(serializedUpdateAndDrainQueryMetadata.workerid()),
                SharedQueryId(serializedUpdateAndDrainQueryMetadata.sharedqueryid()),
                DecomposedQueryId(serializedUpdateAndDrainQueryMetadata.decomposedqueryid()),
                serializedUpdateAndDrainQueryMetadata.decomposedqueryplanversion(),
                serializedUpdateAndDrainQueryMetadata.numberofsources());
            auto markerEvent = ReconfigurationMarkerEvent::create(queryState, reConfMetaData);
            auto planWithVersion = unpackDecomposedPlanFromString(key);
            reconfigurationMarker->addReconfigurationEvent(planWithVersion.id, planWithVersion.version, markerEvent);
        } else if (reconfigurationMetaData.details().Is<SerializableReconfigurationMetadata_SerializableUpdateQueryMetadata>()) {
            auto serializedUpdateQueryMetadata = SerializableReconfigurationMetadata_SerializableUpdateQueryMetadata();
            reconfigurationMetaData.details().UnpackTo(&serializedUpdateQueryMetadata);
            std::vector<NetworkSinkUpdateInfo> sinkUpdates;
            for (auto update : serializedUpdateQueryMetadata.sinkupdates()) {
                auto serializableNodeLocation = update.nodelocation();
                NodeLocationUpdateInfo nodeLocationUpdate{
                    WorkerId(serializableNodeLocation.nodeid()),
                    serializableNodeLocation.hostname(),
                    serializableNodeLocation.port(),
                };
                auto serializablePartition = update.nespartition();
                NesPartitionUpdateInfo partitionUpdate{SharedQueryId(serializablePartition.queryid()),
                                                       OperatorId(serializablePartition.operatorid()),
                                                       PartitionId(serializablePartition.partitionid()),
                                                       SubpartitionId(serializablePartition.subpartitionid())};

                NetworkSinkUpdateInfo updateInfo{nodeLocationUpdate,
                                                 partitionUpdate,
                                                 std::chrono::milliseconds(update.waittime()),
                                                 update.retrytimes(),
                                                 DecomposedQueryPlanVersion(update.version()),
                                                 OperatorId(update.uniquenetworksinkdescriptorid()),
                                                 update.numberoforigins()};

                sinkUpdates.push_back(updateInfo);
            }
            auto reConfMetaData =
                std::make_shared<UpdateQueryMetadata>(WorkerId(serializedUpdateQueryMetadata.workerid()),
                                                      SharedQueryId(serializedUpdateQueryMetadata.sharedqueryid()),
                                                      DecomposedQueryId(serializedUpdateQueryMetadata.decomposedqueryid()),
                                                      serializedUpdateQueryMetadata.decomposedqueryplanversion(),
                                                      sinkUpdates);
            auto markerEvent = ReconfigurationMarkerEvent::create(queryState, reConfMetaData);
            auto planWithVersion = unpackDecomposedPlanFromString(key);
            reconfigurationMarker->addReconfigurationEvent(planWithVersion.id, planWithVersion.version, markerEvent);
        }
    }
}

std::string ReconfigurationMarkerSerializationUtil::packDecomposedPlanAsString(uint64_t first, uint32_t second) {
    std::ostringstream oss;
    oss << first << "," << second;
    return oss.str();
}

DecomposedQueryIdWithVersion ReconfigurationMarkerSerializationUtil::unpackDecomposedPlanFromString(const std::string& key) {
    std::istringstream iss(key);
    uint64_t first;
    uint32_t second;
    char delimiter;
    iss >> first >> delimiter >> second;
    return DecomposedQueryIdWithVersion(first, second);
}

}// namespace NES
