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
#include <DataGeneration/NEXMarkGeneration/PersonGenerator.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/MemoryLayout/DynamicTupleBuffer.hpp>
#include <Util/Core.hpp>
#include <Util/Common.hpp>

namespace NES::Benchmark::DataGeneration {
class PersonGeneratorTest : public Testing::NESBaseTest {
  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("PersonGeneratorTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO2("Setup PersonGeneratorTest test class.");
    }

    /* Will be called before a test is executed. */
    void SetUp() override {
        Testing::NESBaseTest::SetUp();
        NES_INFO2("Setup PersonGeneratorTest test case.");
    }

    /* Will be called before a test is executed. */
    void TearDown() override {
        NES_INFO2("Tear down PersonGeneratorTest test case.");
        Testing::NESBaseTest::TearDown();
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_INFO2("Tear down PersonGeneratorTest test class."); }

    std::shared_ptr<Runtime::BufferManager> bufferManager = std::make_shared<Runtime::BufferManager>(128, 2048);
    NEXMarkGeneration::DependencyGenerator& dependencyGeneratorInstance = NEXMarkGeneration::DependencyGenerator::getInstance(
        bufferManager->getNumOfPooledBuffers(), bufferManager->getBufferSize());
};

TEST_F(PersonGeneratorTest, getSchemaTest) {
    auto personGenerator = std::make_unique<NEXMarkGeneration::PersonGenerator>();

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
}

TEST_F(PersonGeneratorTest, getNameTest) {
    auto personGenerator = std::make_unique<NEXMarkGeneration::PersonGenerator>();

    auto nameDefault = personGenerator->getName();
    auto expectedName = "NEXMarkPerson";
    ASSERT_EQ(nameDefault, expectedName);
}

TEST_F(PersonGeneratorTest, toStringTest) {
    auto personGenerator = std::make_unique<NEXMarkGeneration::PersonGenerator>();

    auto stringDefault = personGenerator->toString();
    auto expectedString = "NEXMarkPerson()";
    ASSERT_EQ(stringDefault, expectedString);
}

TEST_F(PersonGeneratorTest, createDataTest) {
    auto personGenerator = std::make_unique<NEXMarkGeneration::PersonGenerator>();
    personGenerator->setBufferManager(bufferManager);

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

} //namespace NES::Benchmark::DataGeneration
