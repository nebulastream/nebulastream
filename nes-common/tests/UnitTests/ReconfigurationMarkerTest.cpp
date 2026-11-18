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

#include <Reconfiguration/ReconfigurationMarker.hpp>

#include <variant>
#include <Identifiers/Identifiers.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/UUID.hpp>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

using namespace ::testing;

class ReconfigurationMarkerTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite() { Logger::setupLogging("ReconfigurationMarkerTest.log", LogLevel::LOG_DEBUG); }
};

TEST_F(ReconfigurationMarkerTest, EventsPerLocalQuery)
{
    auto marker = ReconfigurationMarker::create();
    const auto drained = LocalQueryId(generateUUID());
    const auto updated = LocalQueryId(generateUUID());
    marker.addReconfigurationEvent(drained, DrainQueryMetadata{.detachedChannels = {"c1"}});
    marker.addReconfigurationEvent(
        updated, UpdateQueryMetadata{.networkSinkUpdates = {{.oldChannel = "c0", .newChannel = "c2", .newDataEndpoint = "base2:9090"}}});

    EXPECT_THAT(
        marker.getReconfigurationEvent(drained),
        Optional(VariantWith<DrainQueryMetadata>(Field(&DrainQueryMetadata::detachedChannels, ElementsAre("c1")))));
    EXPECT_TRUE(std::holds_alternative<UpdateQueryMetadata>(*marker.getReconfigurationEvent(updated)));
    EXPECT_THAT(marker.getReconfigurationEvent(LocalQueryId(generateUUID())), Eq(std::nullopt));
    EXPECT_THAT(ReconfigurationMarker::create().getId(), Ne(marker.getId()));
}

/// Drain and update-and-drain events have the same fields, the serialization keeps them apart.
TEST_F(ReconfigurationMarkerTest, SerializationRoundtrip)
{
    auto marker = ReconfigurationMarker::create();
    marker.addReconfigurationEvent(LocalQueryId(generateUUID()), DrainQueryMetadata{.detachedChannels = {"c1", "c2"}});
    marker.addReconfigurationEvent(LocalQueryId(generateUUID()), UpdateAndDrainQueryMetadata{.detachedChannels = {"c3"}});
    marker.addReconfigurationEvent(
        LocalQueryId(generateUUID()),
        UpdateQueryMetadata{.networkSinkUpdates = {{.oldChannel = "c0", .newChannel = "c4", .newDataEndpoint = "base2:9090"}}});
    marker.addReconfigurationEvent(LocalQueryId(generateUUID()), DrainQueryMetadata{});

    EXPECT_THAT(ReconfigurationMarker::deserialize(marker.serialize()), Eq(marker));
    EXPECT_THROW((void)ReconfigurationMarker::deserialize("not a marker"), Exception);
}

}
