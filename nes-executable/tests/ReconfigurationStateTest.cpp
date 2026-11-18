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

#include <ReconfigurationState.hpp>

#include <string>
#include <variant>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Reconfiguration/ReconfigurationMarker.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/UUID.hpp>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>

namespace NES
{

using namespace ::testing;

class ReconfigurationStateTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite() { Logger::setupLogging("ReconfigurationStateTest.log", LogLevel::LOG_DEBUG); }

    const LocalQueryId query{generateUUID()};
    ReconfigurationState state{query};
};

TEST_F(ReconfigurationStateTest, IgnoresMarkersWithoutEventAndDuplicates)
{
    auto marker = ReconfigurationMarker::create();
    marker.addReconfigurationEvent(LocalQueryId(generateUUID()), DrainQueryMetadata{});
    EXPECT_THAT(state.recordMarker(marker), Eq(std::nullopt));
    EXPECT_THAT(state.getDrainMarker(), Eq(std::nullopt));
    EXPECT_THAT(state.getChannelTermination("c"), Eq(ChannelTermination::Close));

    marker.addReconfigurationEvent(query, DrainQueryMetadata{.detachedChannels = {"detached"}});
    EXPECT_TRUE(state.recordMarker(marker).has_value());
    EXPECT_THAT(state.recordMarker(marker), Eq(std::nullopt)) << "the same marker reached the query twice";
    EXPECT_THAT(state.getDrainMarker(), ::testing::Optional(marker));
}

TEST_F(ReconfigurationStateTest, DrainDecidesChannelTermination)
{
    auto marker = ReconfigurationMarker::create();
    marker.addReconfigurationEvent(query, UpdateAndDrainQueryMetadata{.detachedChannels = {"detached"}});
    ASSERT_TRUE(state.recordMarker(marker).has_value());
    EXPECT_TRUE(state.isUpdateAndDrain());
    EXPECT_THAT(state.getChannelTermination("detached"), Eq(ChannelTermination::Detach));
    EXPECT_THAT(state.getChannelTermination("other"), Eq(ChannelTermination::ForwardMarker));

    /// A second drain marker does not change the drain.
    auto other = ReconfigurationMarker::create();
    other.addReconfigurationEvent(query, DrainQueryMetadata{});
    (void)state.recordMarker(other);
    EXPECT_THAT(state.getDrainMarker(), ::testing::Optional(marker));
}

/// An update is handed to the registered sink right away and the sink is then known under its new channel. An update recorded before the
/// sink registered is handed over during the registration.
TEST_F(ReconfigurationStateTest, UpdatesAreHandedToTheSink)
{
    std::vector<std::string> applied;
    auto handler = [&](const NetworkSinkUpdate& update, const ReconfigurationMarker&)
    {
        applied.emplace_back(update.newChannel);
        return update.newChannel;
    };
    auto update = [&](const std::string& oldChannel, const std::string& newChannel)
    {
        auto marker = ReconfigurationMarker::create();
        marker.addReconfigurationEvent(
            query,
            UpdateQueryMetadata{.networkSinkUpdates = {{.oldChannel = oldChannel, .newChannel = newChannel, .newDataEndpoint = "w:1"}}});
        return marker;
    };

    ASSERT_TRUE(state.recordMarker(update("c1", "c2")).has_value());
    EXPECT_THAT(applied, IsEmpty());
    EXPECT_THAT(state.getDrainMarker(), Eq(std::nullopt));
    const int owner = 0;
    state.registerSinkUpdateHandler("c1", &owner, handler);
    EXPECT_THAT(applied, ElementsAre("c2"));

    ASSERT_TRUE(state.recordMarker(update("c2", "c3")).has_value());
    EXPECT_THAT(applied, ElementsAre("c2", "c3"));

    state.unregisterSinkUpdateHandlers(&owner);
    ASSERT_TRUE(state.recordMarker(update("c3", "c4")).has_value());
    EXPECT_THAT(applied, ElementsAre("c2", "c3"));
}

}
