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
#include <DataGeneration/NEXMarkGeneration/OpenAuctionGenerator.hpp>
#include <DataGeneration/NEXMarkGeneration/PersonGenerator.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/MemoryLayout/DynamicTupleBuffer.hpp>
#include <Runtime/TupleBuffer.hpp>

namespace NES::Benchmark::DataGeneration {
class NEXMarkDataGeneratorTest : public Testing::NESBaseTest {
  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("NEXMarkDataGeneratorTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup NEXMarkDataGeneratorTest test class.");
    }

    /* Will be called before a test is executed. */
    void SetUp() override {
        Testing::NESBaseTest::SetUp();
        NES_INFO("Setup NEXMarkDataGeneratorTest test case.");
    }

    /* Will be called before a test is executed. */
    void TearDown() override {
        NES_INFO("Tear down NEXMarkDataGeneratorTest test case.");
        Testing::NESBaseTest::TearDown();
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_INFO("Tear down NEXMarkDataGeneratorTest test class."); }

    std::shared_ptr<Runtime::BufferManager> bufferManager = std::make_shared<Runtime::BufferManager>();
    NEXMarkGeneration::DependencyGenerator& dependencyGeneratorInstance = NEXMarkGeneration::DependencyGenerator::getInstance(
        bufferManager->getNumOfPooledBuffers(), bufferManager->getBufferSize());
};

/**
 * @brief Tests if DependencyGenerator works by comparing the results of getInstance(), getPersons(), getAuctions(),
 * getBids() and getNumberOfRecords() to expected values
 */
TEST_F(NEXMarkDataGeneratorTest, dependencyGeneratorTest) {
    // testing getInstance - though the function is called with different arguments it should still return the original instance
    auto& newDependencyGeneratorInstance = NEXMarkGeneration::DependencyGenerator::getInstance(64, 512);
    ASSERT_EQ(&dependencyGeneratorInstance, &newDependencyGeneratorInstance);

    // testing getPersons()
    auto persons = dependencyGeneratorInstance.getPersons();

    // testing getAuctions()
    auto auctions = dependencyGeneratorInstance.getAuctions();

    // testing getBids()
    auto bids = dependencyGeneratorInstance.getBids();

    // testing getNumberOfRecords
    auto numberOfRecordsDefault = dependencyGeneratorInstance.getNumberOfRecords();
    auto expectedNumberOfRecords = 166UL;
    ASSERT_EQ(numberOfRecordsDefault, expectedNumberOfRecords);

    // TODO check if generated data has expected values
}

/**
 * @brief Tests if PersonGenerator works by comparing the results of getSchema(), getName(), toString() and
 * createData() to expected values
 */
TEST_F(NEXMarkDataGeneratorTest, personGeneratorTest) {
    auto personGenerator = std::make_unique<NEXMarkGeneration::PersonGenerator>();
    personGenerator->setBufferManager(bufferManager);

    // testing getSchema()
    auto schemaDefault = personGenerator->getSchema();
    auto expectedSchema = Schema::create()
                              ->addField(createField("id", BasicType::UINT64))
                              ->addField(createField("name", BasicType::TEXT))
                              ->addField(createField("email", BasicType::TEXT))
                              ->addField(createField("phone", BasicType::TEXT))
                              ->addField(createField("street", BasicType::TEXT))
                              ->addField(createField("city", BasicType::TEXT))
                              ->addField(createField("country", BasicType::TEXT))
                              ->addField(createField("province", BasicType::TEXT))
                              ->addField(createField("zipcode", BasicType::UINT32))
                              ->addField(createField("homepage", BasicType::TEXT))
                              ->addField(createField("creditcard", BasicType::TEXT))
                              ->addField(createField("income", BasicType::FLOAT64))
                              ->addField(createField("interest", BasicType::TEXT))
                              ->addField(createField("education", BasicType::TEXT))
                              ->addField(createField("gender", BasicType::TEXT))
                              ->addField(createField("business", BasicType::BOOLEAN))
                              ->addField(createField("age", BasicType::UINT8))
                              ->addField(createField("timestamp", BasicType::UINT64));
    ASSERT_TRUE(expectedSchema->equals(schemaDefault, true));

    // testing getName()
    auto nameDefault = personGenerator->getName();
    auto expectedName = "NEXMarkPerson";
    ASSERT_EQ(nameDefault, expectedName);

    // testing toString()
    auto stringDefault = personGenerator->toString();
    auto expectedString = "NEXMarkPerson()";
    ASSERT_EQ(stringDefault, expectedString);

    // testing createData()
    auto dataDefault = personGenerator->createData(bufferManager->getNumOfPooledBuffers(), bufferManager->getBufferSize());
    // TODO generate expected data
}

/**
 * @brief Tests if OpenAuctionGenerator works by comparing the results of getSchema(), getName(), toString() and
 * createData() to expected values
 */
TEST_F(NEXMarkDataGeneratorTest, openAuctionGeneratorTest) {
    auto openAuctionGenerator = std::make_unique<NEXMarkGeneration::OpenAuctionGenerator>();
    openAuctionGenerator->setBufferManager(bufferManager);

    // testing getSchema()
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

    // testing getName()
    auto nameDefault = openAuctionGenerator->getName();
    auto expectedName = "NEXMarkOpenAuction";
    ASSERT_EQ(nameDefault, expectedName);

    // testing toString()
    auto stringDefault = openAuctionGenerator->toString();
    auto expectedString = "NEXMarkOpenAuction()";
    ASSERT_EQ(stringDefault, expectedString);

    // testing createData()
    auto dataDefault = openAuctionGenerator->createData(bufferManager->getNumOfPooledBuffers(), bufferManager->getBufferSize());
    // TODO generate expected data
}

/**
 * @brief Tests if BidGenerator works by comparing the results of getSchema(), getName(), toString() and
 * createData() to expected values
 */
TEST_F(NEXMarkDataGeneratorTest, bidGeneratorTest) {
    auto bidGenerator = std::make_unique<NEXMarkGeneration::BidGenerator>();
    bidGenerator->setBufferManager(bufferManager);

    // testing getSchema()
    auto schemaDefault = bidGenerator->getSchema();
    auto expectedSchema = Schema::create()
                              ->addField(createField("auctionId", BasicType::UINT64))
                              ->addField(createField("bidderId", BasicType::UINT64))
                              ->addField(createField("price", BasicType::UINT64))
                              ->addField(createField("timestamp", BasicType::UINT64));
    ASSERT_TRUE(expectedSchema->equals(schemaDefault, true));

    // testing getName()
    auto nameDefault = bidGenerator->getName();
    auto expectedName = "NEXMarkBid";
    ASSERT_EQ(nameDefault, expectedName);

    // testing toString()
    auto stringDefault = bidGenerator->toString();
    auto expectedString = "NEXMarkBid()";
    ASSERT_EQ(stringDefault, expectedString);

    // testing createData()
    auto dataDefault = bidGenerator->createData(bufferManager->getNumOfPooledBuffers(), bufferManager->getBufferSize());
    // TODO generate expected data
}

} //namespace NES::Benchmark::DataGeneration
