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
#include <Util/Core.hpp>
#include <Util/Common.hpp>

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

    std::shared_ptr<Runtime::BufferManager> bufferManager = std::make_shared<Runtime::BufferManager>(128, 2048);
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

    // ------------------------------------------------------------------
    // testing getPersons()
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

    // ------------------------------------------------------------------
    // testing getAuctions()
    auto auctions = dependencyGeneratorInstance.getAuctions();
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

    // ------------------------------------------------------------------
    // testing getBids()
    auto bids = dependencyGeneratorInstance.getBids();
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

    // ------------------------------------------------------------------
    // testing getNumberOfRecords()
    auto numberOfRecordsDefault = dependencyGeneratorInstance.getNumberOfRecords();
    ASSERT_EQ(numberOfRecordsDefault, 262);
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
    auto persons = dependencyGeneratorInstance.getPersons();
    ASSERT_FALSE(dataDefault.empty());

    auto index = 0UL;
    for (auto& buffer : dataDefault) {
        auto numBuffers = buffer.getNumberOfTuples();
        auto bufferAsCSV = Util::printTupleBufferAsCSV(buffer, personGenerator->getSchema());
        auto bufferRecordsAsString = Util::splitWithStringDelimiter<std::string>(bufferAsCSV, "\n");
        //NES_INFO(bufferAsCSV);

        for (auto i = index; i < index + numBuffers; ++i) {
            auto bufferRecord = bufferRecordsAsString[i % bufferRecordsAsString.size()];

            if (i < persons.size()) {
                auto items = Util::splitWithStringDelimiter<std::string>(bufferRecord, ",");
                auto person = persons[i];

                auto personId = std::stoi(items[0]);
                auto name = items[1];
                auto email = items[2];
                auto phone = items[3];
                auto street = items[4];
                auto city = items[5];
                auto country = items[6];
                auto province = items[7];
                auto zipcode = std::stoi(items[8]);
                auto homepage = items[9];
                auto creditcard = items[10];
                auto income = std::stod(items[11]);
                auto interest = items[12];
                auto education = items[13];
                auto gender = items[14];
                auto business = std::stoi(items[15]);
                auto age = std::stoi(items[16]);
                auto timestamp = std::stoi(items[17]);

                ASSERT_EQ(personId, i);
                ASSERT_FALSE(name.empty());
                ASSERT_FALSE(email.empty());
                ASSERT_TRUE(phone.empty() || phone.starts_with("+"));
                ASSERT_TRUE(street.empty() || street.ends_with(" St"));
                // city, country, province could be anything so there is no need to check the values
                ASSERT_TRUE(zipcode >= 0 && zipcode <= 99999);
                ASSERT_TRUE(homepage.empty() || homepage.starts_with("http://www."));
                ASSERT_TRUE(creditcard.empty() || creditcard.size() == 19);
                ASSERT_TRUE(income == 0.0 || (income >= 40000.00 && income <= 69999.99));
                ASSERT_TRUE(interest.empty() || interest.size() <= 18);
                ASSERT_TRUE(education == "High School" || education == "College" || education == "Graduate School" || education == "Other" || education.empty());
                ASSERT_TRUE(gender == "female" || gender == "male" || gender.empty());
                ASSERT_TRUE(business == 0 || business == 1);
                ASSERT_TRUE(age == 0 || (age >= 30 && age <= 44));
                ASSERT_EQ(timestamp, person);
            }
            else {
                auto expectedRecord = "0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0";
                ASSERT_EQ(expectedRecord, bufferRecord);
            }
        }

        index += numBuffers;
    }
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
