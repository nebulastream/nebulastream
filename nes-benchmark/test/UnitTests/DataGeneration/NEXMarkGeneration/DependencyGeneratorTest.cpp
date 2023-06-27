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

#include <NesBaseTest.hpp>
#include <DataGeneration/NEXMarkGeneration/DependencyGenerator.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/MemoryLayout/DynamicTupleBuffer.hpp>
#include <Util/Core.hpp>
#include <Util/Common.hpp>

namespace NES::Benchmark::DataGeneration {
class DependencyGeneratorTest : public Testing::NESBaseTest {
  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("DependencyGeneratorTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup DependencyGeneratorTest test class.");
    }

    /* Will be called before a test is executed. */
    void SetUp() override {
        Testing::NESBaseTest::SetUp();
        NES_INFO("Setup DependencyGeneratorTest test case.");
    }

    /* Will be called before a test is executed. */
    void TearDown() override {
        NES_INFO("Tear down DependencyGeneratorTest test case.");
        Testing::NESBaseTest::TearDown();
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_INFO("Tear down DependencyGeneratorTest test class."); }

    std::shared_ptr<Runtime::BufferManager> bufferManager = std::make_shared<Runtime::BufferManager>(128, 2048);
    NEXMarkGeneration::DependencyGenerator& dependencyGeneratorInstance = NEXMarkGeneration::DependencyGenerator::getInstance(
        bufferManager->getNumOfPooledBuffers(), bufferManager->getBufferSize());
};

TEST_F(DependencyGeneratorTest, getInstanceTest) {
    // though the function is called with different arguments it should still return the original instance
    auto& newDependencyGeneratorInstance = NEXMarkGeneration::DependencyGenerator::getInstance(64, 512);
    ASSERT_EQ(&dependencyGeneratorInstance, &newDependencyGeneratorInstance);
}

TEST_F(DependencyGeneratorTest, getPersonsTest) {
    auto persons = dependencyGeneratorInstance.getPersons();
    // check if number of persons is correct
    ASSERT_EQ(persons.size(), 72);

    // check if values of persons are correct
    for (auto i = 0UL; i < persons.size(); ++i) {
        // verify that time wasn't decreased
        if (i == 0) {
            ASSERT_TRUE(persons[0] >= 0);
        }
        else {
            ASSERT_TRUE(persons[i] >= persons[i - 1]);
        }

        //NES_INFO("Person " << i << ": timestamp is " << persons[i]);
    }

    // timestamp should have increased
    EXPECT_NE(persons[persons.size() - 1], persons[0]);
}

TEST_F(DependencyGeneratorTest, getAuctionsTest) {
    auto auctions = dependencyGeneratorInstance.getAuctions();
    auto persons = dependencyGeneratorInstance.getPersons();
    // check if number of auctions is correct
    ASSERT_EQ(auctions.size(), 304);

    // check if values of auctions are correct
    bool sellerIdLargerInit = false;

    for (auto i = 0UL; i < auctions.size(); ++i) {
        auto sellerId = std::get<0>(auctions[i]);
        auto price = std::get<1>(auctions[i]);
        auto timestamp = std::get<2>(auctions[i]);
        auto expires = std::get<3>(auctions[i]);

        // check if seller already exists at this time
        ASSERT_TRUE(persons[sellerId] <= timestamp);
        // verify that price and endTime are within an interval of accepted values
        ASSERT_TRUE(price > 0);
        ASSERT_TRUE(expires >= timestamp + 2 * 60 * 60 && expires < timestamp + 26 * 60 * 60);
        // verify that time wasn't decreased
        if (i == 0) {
            ASSERT_TRUE(timestamp >= 0);
        }
        else {
            ASSERT_TRUE(timestamp >= std::get<2>(auctions[i - 1]));
        }

        if (sellerId >= NEXMarkGeneration::recordsInit) {
            sellerIdLargerInit = true;
        }
        //NES_INFO("Auction " << i << ": sellerId is " << sellerId << ", price is " << price << ", timestamp is " << timestamp << ", endTime is " << expires);
    }

    // timestamp should have increased
    EXPECT_NE(std::get<2>(auctions[auctions.size() - 1]), std::get<2>(auctions[0]));
    // sellerId should also increase over time and not just be drawn from 0 to constant x every time
    EXPECT_TRUE(sellerIdLargerInit);
}

TEST_F(DependencyGeneratorTest, getBidsTest) {
    auto bids = dependencyGeneratorInstance.getBids();
    auto auctions = dependencyGeneratorInstance.getAuctions();
    auto persons = dependencyGeneratorInstance.getPersons();
    // check if number of bids is correct
    ASSERT_EQ(bids.size(), 2530);

    // check if values of bids are correct
    std::vector<uint64_t> prices(auctions.size(), 0);
    bool auctionIdLargerInit = false;
    bool bidderIdLargerInit = false;

    for (auto i = 0UL; i < bids.size(); ++i) {
        auto auctionId = std::get<0>(bids[i]);
        auto bidderId = std::get<1>(bids[i]);
        auto price = std::get<2>(bids[i]);
        auto timestamp = std::get<3>(bids[i]);

        // check if auction and bidder already exist at this time
        ASSERT_TRUE(std::get<2>(auctions[auctionId]) <= timestamp);
        ASSERT_TRUE(persons[bidderId] <= timestamp);
        // verify that new bid on an auction is larger than the last bid on that auction but not larger than the final bid
        ASSERT_TRUE(std::get<1>(auctions[auctionId]) >= price);
        ASSERT_TRUE(prices[auctionId] < price);
        prices[auctionId] = price;
        // verify that time wasn't decreased
        if (i == 0) {
            ASSERT_TRUE(timestamp >= 0);
        }
        else {
            ASSERT_TRUE(timestamp >= std::get<3>(bids[i - 1]));
        }

        if (auctionId >= NEXMarkGeneration::recordsInit) {
            auctionIdLargerInit = true;
        }
        if (bidderId >= NEXMarkGeneration::recordsInit) {
            bidderIdLargerInit = true;
        }
        //NES_INFO("Bid " << i << ": auctionId is " << auctionId << ", bidderId is " << bidderId << ", price is " << price << ", timestamp is " << timestamp);
    }

    // timestamp should have increased
    EXPECT_NE(std::get<3>(bids[bids.size() - 1]), std::get<3>(bids[0]));
    // auctionId and bidderId should also increase over time and not just be drawn from 0 to constant x every time
    EXPECT_TRUE(auctionIdLargerInit && bidderIdLargerInit);
}

TEST_F(DependencyGeneratorTest, getNumberOfRecordsTest) {
    auto numberOfRecordsDefault = dependencyGeneratorInstance.getNumberOfRecords();
    ASSERT_EQ(numberOfRecordsDefault, 262);
}

} //namespace NES::Benchmark::DataGeneration
