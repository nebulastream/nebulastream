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

#include <BaseUnitTest.hpp>
#include <Reconfiguration/Metadata/DrainQueryMetadata.hpp>
#include <Reconfiguration/Metadata/UpdateAndDrainQueryMetadata.hpp>
#include <Reconfiguration/Metadata/UpdateQueryMetadata.hpp>
#include <Reconfiguration/ReconfigurationMarker.hpp>
#include <Reconfiguration/ReconfigurationMarkerSerializationUtil.hpp>
#include <Util/Logger/Logger.hpp>
#include <WorkerRPCService.grpc.pb.h>
#include <WorkerRPCService.pb.h>
#include <gtest/gtest.h>
#include <iostream>

using namespace NES;

class ReconfigurationMarkerSerializationUtilTest : public Testing::BaseUnitTest {

  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("ReconfigurationMarkerSerilizationUtilTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup ReconfigurationMarkerSerilizationUtilTest test class.");
    }
};

TEST_F(ReconfigurationMarkerSerializationUtilTest, reconfigurationMarkerSerialization) {

    auto originalReconfigurationMarker = ReconfigurationMarker::create();

    NodeLocationUpdateInfo locationUpdate{
        WorkerId(3),
        "127.0.0.1",
        1234,
    };

    NesPartitionUpdateInfo partitionUpdate{
        SharedQueryId(4),
        OperatorId(5),
        PartitionId(6),
        SubpartitionId(7),
    };

    NetworkSinkUpdateInfo sinkUpdate{locationUpdate,
                                     partitionUpdate,
                                     std::chrono::milliseconds(8),
                                     9,
                                     DecomposedQueryPlanVersion(10),
                                     OperatorId(11),
                                     12};

    std::vector<NetworkSinkUpdateInfo> sinkUpdates = {sinkUpdate};
    // reconfiguration metadata 1
    auto reconfigurationMetaData1 =
        std::make_shared<UpdateQueryMetadata>(WorkerId(1), SharedQueryId(1), DecomposedQueryId(1), 1, sinkUpdates);
    auto reconfigurationMarkerEvent1 =
        ReconfigurationMarkerEvent::create(QueryState::MARKED_FOR_REDEPLOYMENT, reconfigurationMetaData1);
    DecomposedQueryIdWithVersion idWithVersion1(DecomposedQueryId(1), DecomposedQueryPlanVersion(0));
    originalReconfigurationMarker->addReconfigurationEvent(idWithVersion1, reconfigurationMarkerEvent1);

    // reconfiguration metadata 2
    auto reconfigurationMetaData2 = std::make_shared<DrainQueryMetadata>(1);
    auto reconfigurationMarkerEvent2 =
        ReconfigurationMarkerEvent::create(QueryState::MARKED_FOR_MIGRATION, reconfigurationMetaData2);
    DecomposedQueryIdWithVersion idWithVersion2(DecomposedQueryId(2), DecomposedQueryPlanVersion(0));
    originalReconfigurationMarker->addReconfigurationEvent(idWithVersion2, reconfigurationMarkerEvent2);

    // reconfiguration metadata 3
    auto reconfigurationMetaData3 =
        std::make_shared<UpdateAndDrainQueryMetadata>(WorkerId(1), SharedQueryId(1), DecomposedQueryId(1), 1, 1);
    auto reconfigurationMarkerEvent3 =
        ReconfigurationMarkerEvent::create(QueryState::MARKED_FOR_MIGRATION, reconfigurationMetaData3);
    DecomposedQueryIdWithVersion idWithVersion3(DecomposedQueryId(3), DecomposedQueryPlanVersion(0));
    originalReconfigurationMarker->addReconfigurationEvent(idWithVersion3, reconfigurationMarkerEvent3);

    // serialize and deserialize reconfiguration marker
    ReconfigurationMarkerRequest request;
    auto serializableReconfigurationMarker = request.mutable_serializablereconfigurationmarker();
    ReconfigurationMarkerSerializationUtil::serialize(originalReconfigurationMarker, *serializableReconfigurationMarker);
    auto deserializedReconfigurationMarker = ReconfigurationMarker::create();
    ReconfigurationMarkerSerializationUtil::deserialize(*serializableReconfigurationMarker, deserializedReconfigurationMarker);

    // Validate if same number of reconfiguration marker events exists post serialization
    ASSERT_EQ(originalReconfigurationMarker->getAllReconfigurationMarkerEvents().size(),
              serializableReconfigurationMarker->reconfigurationmarkerevents().size());

    // Validate if same number of reconfiguration marker events exists post deserialization
    ASSERT_EQ(serializableReconfigurationMarker->reconfigurationmarkerevents().size(),
              deserializedReconfigurationMarker->getAllReconfigurationMarkerEvents().size());

    for (const auto& [originalKey, originalReconfigurationMarkerEvent] :
         originalReconfigurationMarker->getAllReconfigurationMarkerEvents()) {
        const auto& deserializedReconfigurationEvent = deserializedReconfigurationMarker->getReconfigurationEvent(originalKey);
        EXPECT_TRUE(deserializedReconfigurationEvent.has_value());
    }

    // Validate if reconfiguration event for key 1 got properly serialized and deserialized
    const auto& deserializedReconfigurationEvent1Optional1 =
        deserializedReconfigurationMarker->getReconfigurationEvent(idWithVersion1);
    const auto& deserializedReconfigurationMetadata1 = deserializedReconfigurationEvent1Optional1.value();
    EXPECT_EQ(reconfigurationMarkerEvent1->queryState, deserializedReconfigurationMetadata1->queryState);
    EXPECT_EQ(reconfigurationMarkerEvent1->reconfigurationMetadata->as<UpdateQueryMetadata>()->workerId,
              deserializedReconfigurationMetadata1->reconfigurationMetadata->as<UpdateQueryMetadata>()->workerId);
    EXPECT_EQ(reconfigurationMarkerEvent1->reconfigurationMetadata->as<UpdateQueryMetadata>()->sharedQueryId,
              deserializedReconfigurationMetadata1->reconfigurationMetadata->as<UpdateQueryMetadata>()->sharedQueryId);
    EXPECT_EQ(reconfigurationMarkerEvent1->reconfigurationMetadata->as<UpdateQueryMetadata>()->decomposedQueryId,
              deserializedReconfigurationMetadata1->reconfigurationMetadata->as<UpdateQueryMetadata>()->decomposedQueryId);
    EXPECT_EQ(
        reconfigurationMarkerEvent1->reconfigurationMetadata->as<UpdateQueryMetadata>()->decomposedQueryPlanVersion,
        deserializedReconfigurationMetadata1->reconfigurationMetadata->as<UpdateQueryMetadata>()->decomposedQueryPlanVersion);
    auto deserializedSinkUpdate =
        deserializedReconfigurationMetadata1->reconfigurationMetadata->as<UpdateQueryMetadata>()->networkSinkUpdates.front();
    auto deserializedLocationUpdate = deserializedSinkUpdate.nodeLocation;
    EXPECT_EQ(deserializedLocationUpdate.workerId, locationUpdate.workerId);
    EXPECT_EQ(deserializedLocationUpdate.hostname, locationUpdate.hostname);
    EXPECT_EQ(deserializedLocationUpdate.port, locationUpdate.port);

    auto deserializedPartitionUpdate = deserializedSinkUpdate.nesPartition;
    EXPECT_EQ(deserializedPartitionUpdate.sharedQueryId, partitionUpdate.sharedQueryId);
    EXPECT_EQ(deserializedPartitionUpdate.operatorId, partitionUpdate.operatorId);
    EXPECT_EQ(deserializedPartitionUpdate.partitionId, partitionUpdate.partitionId);
    EXPECT_EQ(deserializedPartitionUpdate.subpartitionId, partitionUpdate.subpartitionId);

    EXPECT_EQ(deserializedSinkUpdate.waitTime, sinkUpdate.waitTime);
    EXPECT_EQ(deserializedSinkUpdate.retryTimes, sinkUpdate.retryTimes);
    EXPECT_EQ(deserializedSinkUpdate.version, sinkUpdate.version);
    EXPECT_EQ(deserializedSinkUpdate.uniqueNetworkSinkId, sinkUpdate.uniqueNetworkSinkId);
    EXPECT_EQ(deserializedSinkUpdate.numberOfOrigins, sinkUpdate.numberOfOrigins);

    // Validate if reconfiguration event for key 2 got properly serialized and deserialized
    const auto& deserializedReconfigurationEvent1Optional2 =
        deserializedReconfigurationMarker->getReconfigurationEvent(idWithVersion2);
    const auto& deserializedReconfigurationMetadata2 = deserializedReconfigurationEvent1Optional2.value();
    EXPECT_EQ(reconfigurationMarkerEvent2->queryState, deserializedReconfigurationMetadata2->queryState);
    EXPECT_EQ(reconfigurationMarkerEvent2->reconfigurationMetadata->as<DrainQueryMetadata>()->numberOfSources,
              deserializedReconfigurationMetadata2->reconfigurationMetadata->as<DrainQueryMetadata>()->numberOfSources);

    // Validate if reconfiguration event for key 3 got properly serialized and deserialized
    const auto& deserializedReconfigurationEvent1Optional3 =
        deserializedReconfigurationMarker->getReconfigurationEvent(idWithVersion3);
    const auto& deserializedReconfigurationMetadata3 = deserializedReconfigurationEvent1Optional3.value();
    EXPECT_EQ(reconfigurationMarkerEvent3->queryState, deserializedReconfigurationMetadata3->queryState);
    EXPECT_EQ(reconfigurationMarkerEvent3->reconfigurationMetadata->as<UpdateAndDrainQueryMetadata>()->workerId,
              deserializedReconfigurationMetadata3->reconfigurationMetadata->as<UpdateAndDrainQueryMetadata>()->workerId);
    EXPECT_EQ(reconfigurationMarkerEvent3->reconfigurationMetadata->as<UpdateAndDrainQueryMetadata>()->sharedQueryId,
              deserializedReconfigurationMetadata3->reconfigurationMetadata->as<UpdateAndDrainQueryMetadata>()->sharedQueryId);
    EXPECT_EQ(
        reconfigurationMarkerEvent3->reconfigurationMetadata->as<UpdateAndDrainQueryMetadata>()->decomposedQueryId,
        deserializedReconfigurationMetadata3->reconfigurationMetadata->as<UpdateAndDrainQueryMetadata>()->decomposedQueryId);
    EXPECT_EQ(reconfigurationMarkerEvent3->reconfigurationMetadata->as<UpdateAndDrainQueryMetadata>()->decomposedQueryPlanVersion,
              deserializedReconfigurationMetadata3->reconfigurationMetadata->as<UpdateAndDrainQueryMetadata>()
                  ->decomposedQueryPlanVersion);
    EXPECT_EQ(reconfigurationMarkerEvent3->reconfigurationMetadata->as<UpdateAndDrainQueryMetadata>()->numberOfSources,
              deserializedReconfigurationMetadata3->reconfigurationMetadata->as<UpdateAndDrainQueryMetadata>()->numberOfSources);
}
