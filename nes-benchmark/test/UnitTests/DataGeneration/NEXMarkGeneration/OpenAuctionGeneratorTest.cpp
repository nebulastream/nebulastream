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
#include <DataGeneration/NEXMarkGeneration/OpenAuctionGenerator.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/MemoryLayout/DynamicTupleBuffer.hpp>
#include <Util/Core.hpp>
#include <Util/Common.hpp>

namespace NES::Benchmark::DataGeneration {
class OpenAuctionGeneratorTest : public Testing::NESBaseTest {
  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("OpenAuctionGeneratorTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO2("Setup OpenAuctionGeneratorTest test class.");
    }

    /* Will be called before a test is executed. */
    void SetUp() override {
        Testing::NESBaseTest::SetUp();
        NES_INFO2("Setup OpenAuctionGeneratorTest test case.");
    }

    /* Will be called before a test is executed. */
    void TearDown() override {
        NES_INFO2("Tear down OpenAuctionGeneratorTest test case.");
        Testing::NESBaseTest::TearDown();
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_INFO2("Tear down OpenAuctionGeneratorTest test class."); }

    std::shared_ptr<Runtime::BufferManager> bufferManager = std::make_shared<Runtime::BufferManager>(128, 2048);
    NEXMarkGeneration::DependencyGenerator& dependencyGeneratorInstance = NEXMarkGeneration::DependencyGenerator::getInstance(
        bufferManager->getNumOfPooledBuffers(), bufferManager->getBufferSize());
};

TEST_F(OpenAuctionGeneratorTest, getSchemaTest) {
    auto openAuctionGenerator = std::make_unique<NEXMarkGeneration::OpenAuctionGenerator>();

    auto schemaDefault = openAuctionGenerator->getSchema();
    auto expectedSchema = Schema::create()
                              ->addField(createField("id", BasicType::UINT64))
                              ->addField(createField("reserve", BasicType::UINT64))
                              ->addField(createField("privacy", BasicType::BOOLEAN))
                              ->addField(createField("sellerId", BasicType::UINT64))
                              ->addField(createField("category", BasicType::UINT16))
                              ->addField(createField("quantity", BasicType::UINT8))
                              ->addField(createField("type", BasicType::TEXT))
                              ->addField(createField("startTime", BasicType::UINT64))
                              ->addField(createField("endTime", BasicType::UINT64));
    ASSERT_TRUE(expectedSchema->equals(schemaDefault, true));
}

TEST_F(OpenAuctionGeneratorTest, getNameTest) {
    auto openAuctionGenerator = std::make_unique<NEXMarkGeneration::OpenAuctionGenerator>();

    auto nameDefault = openAuctionGenerator->getName();
    auto expectedName = "NEXMarkOpenAuction";
    ASSERT_EQ(nameDefault, expectedName);
}

TEST_F(OpenAuctionGeneratorTest, toStringTest) {
    auto openAuctionGenerator = std::make_unique<NEXMarkGeneration::OpenAuctionGenerator>();

    auto stringDefault = openAuctionGenerator->toString();
    auto expectedString = "NEXMarkOpenAuction()";
    ASSERT_EQ(stringDefault, expectedString);
}

TEST_F(OpenAuctionGeneratorTest, createDataTest) {
    auto openAuctionGenerator = std::make_unique<NEXMarkGeneration::OpenAuctionGenerator>();
    openAuctionGenerator->setBufferManager(bufferManager);

    auto dataDefault = openAuctionGenerator->createData(bufferManager->getNumOfPooledBuffers(), bufferManager->getBufferSize());
    auto auctions = dependencyGeneratorInstance.getAuctions();
    ASSERT_FALSE(dataDefault.empty());

    auto index = 0UL;
    for (auto& buffer : dataDefault) {
        auto numBuffers = buffer.getNumberOfTuples();
        auto bufferAsCSV = Util::printTupleBufferAsCSV(buffer, openAuctionGenerator->getSchema());
        auto bufferRecordsAsString = Util::splitWithStringDelimiter<std::string>(bufferAsCSV, "\n");
        //NES_INFO(bufferAsCSV);

        for (auto i = index; i < index + numBuffers; ++i) {
            auto bufferRecord = bufferRecordsAsString[i % bufferRecordsAsString.size()];

            if (i < auctions.size()) {
                auto items = Util::splitWithStringDelimiter<std::string>(bufferRecord, ",");
                auto auction = auctions[i];

                auto auctionId = std::stoi(items[0]);
                auto reserve = std::stoi(items[1]);
                auto privacy = std::stoi(items[2]);
                auto sellerId = std::stoi(items[3]);
                auto category = std::stoi(items[4]);
                auto quantity = std::stoi(items[5]);
                auto type = items[6];
                auto startTime = std::stoi(items[7]);
                auto endTime = std::stoi(items[8]);

                ASSERT_EQ(auctionId, i);
                ASSERT_TRUE(reserve == 0 || (reserve >= round(std::get<1>(auction) * 2.2) && reserve <= round(std::get<1>(auction) * 3.2)));
                ASSERT_TRUE(privacy == 0 || privacy == 1);
                ASSERT_EQ(sellerId, std::get<0>(auction));
                ASSERT_TRUE(category >= 0 || category <= 302);
                ASSERT_TRUE(quantity >= 1 || quantity <= 10);
                ASSERT_TRUE(type == "Featured" || type == "Regular" || type == "Featured; Dutch" || type == "Regular; Dutch");
                ASSERT_EQ(startTime, std::get<2>(auction));
                ASSERT_EQ(endTime, std::get<3>(auction));
            }
            else {
                auto expectedRecord = "0,0,0,0,0,0,0,0,0";
                ASSERT_EQ(expectedRecord, bufferRecord);
            }
        }

        index += numBuffers;
    }
}

} //namespace NES::Benchmark::DataGeneration
