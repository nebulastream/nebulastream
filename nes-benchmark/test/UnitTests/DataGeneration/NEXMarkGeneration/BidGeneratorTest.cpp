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
#include <DataGeneration/NEXMarkGeneration/BidGenerator.hpp>
#include <DataGeneration/NEXMarkGeneration/DependencyGenerator.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/MemoryLayout/DynamicTupleBuffer.hpp>
#include <Util/Core.hpp>
#include <Util/Common.hpp>

namespace NES::Benchmark::DataGeneration {
class BidGeneratorTest : public Testing::NESBaseTest {
  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("BidGeneratorTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup BidGeneratorTest test class.");
    }

    /* Will be called before a test is executed. */
    void SetUp() override {
        Testing::NESBaseTest::SetUp();
        NES_INFO("Setup BidGeneratorTest test case.");
    }

    /* Will be called before a test is executed. */
    void TearDown() override {
        NES_INFO("Tear down BidGeneratorTest test case.");
        Testing::NESBaseTest::TearDown();
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_INFO("Tear down BidGeneratorTest test class."); }

    std::shared_ptr<Runtime::BufferManager> bufferManager = std::make_shared<Runtime::BufferManager>(128, 2048);
    NEXMarkGeneration::DependencyGenerator& dependencyGeneratorInstance = NEXMarkGeneration::DependencyGenerator::getInstance(
        bufferManager->getNumOfPooledBuffers(), bufferManager->getBufferSize());
};

TEST_F(BidGeneratorTest, getSchemaTest) {
    auto bidGenerator = std::make_unique<NEXMarkGeneration::BidGenerator>();

    auto schemaDefault = bidGenerator->getSchema();
    auto expectedSchema = Schema::create()
                              ->addField(createField("auctionId", BasicType::UINT64))
                              ->addField(createField("bidderId", BasicType::UINT64))
                              ->addField(createField("price", BasicType::UINT64))
                              ->addField(createField("timestamp", BasicType::UINT64));
    ASSERT_TRUE(expectedSchema->equals(schemaDefault, true));
}

TEST_F(BidGeneratorTest, getNameTest) {
    auto bidGenerator = std::make_unique<NEXMarkGeneration::BidGenerator>();

    auto nameDefault = bidGenerator->getName();
    auto expectedName = "NEXMarkBid";
    ASSERT_EQ(nameDefault, expectedName);
}

TEST_F(BidGeneratorTest, toStringTest) {
    auto bidGenerator = std::make_unique<NEXMarkGeneration::BidGenerator>();

    auto stringDefault = bidGenerator->toString();
    auto expectedString = "NEXMarkBid()";
    ASSERT_EQ(stringDefault, expectedString);
}

TEST_F(BidGeneratorTest, createDataTest) {
    auto bidGenerator = std::make_unique<NEXMarkGeneration::BidGenerator>();
    bidGenerator->setBufferManager(bufferManager);

    auto dataDefault = bidGenerator->createData(bufferManager->getNumOfPooledBuffers(), bufferManager->getBufferSize());
    auto bids = dependencyGeneratorInstance.getBids();
    ASSERT_FALSE(dataDefault.empty());

    auto index = 0UL;
    for (auto& buffer : dataDefault) {
        auto numBuffers = buffer.getNumberOfTuples();
        auto bufferDefault = Util::printTupleBufferAsCSV(buffer, bidGenerator->getSchema());
        //NES_INFO(bufferDefault);

        std::ostringstream expectedBuffer;
        for (auto i = index; i < index + numBuffers; ++i) {
            if (i < bids.size()) {
                auto bid = bids[i];
                expectedBuffer << std::get<0>(bid) << ",";
                expectedBuffer << std::get<1>(bid) << ",";
                expectedBuffer << std::get<2>(bid) << ",";
                expectedBuffer << std::get<3>(bid) << "\n";
            }
            else {
                expectedBuffer << "0,0,0,0\n";
            }
        }
        ASSERT_EQ(expectedBuffer.str(), bufferDefault);

        index += numBuffers;
    }
}

} //namespace NES::Benchmark::DataGeneration
