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

#include <DataGeneration/NEXMarkGeneration/PersonGenerator.hpp>
#include <Runtime/MemoryLayout/DynamicTupleBuffer.hpp>
#include <Runtime/TupleBuffer.hpp>

namespace NES::Benchmark::DataGeneration::NEXMarkGeneration {

PersonGenerator::PersonGenerator()
    : DataGenerator() {}

std::vector<Runtime::TupleBuffer> PersonGenerator::createData(size_t numberOfBuffers, size_t bufferSize) {
    std::random_device rndDevice;
    std::mt19937 generator(rndDevice());
    std::uniform_int_distribution<uint8_t> uniformBooleanDistribution(0, 1);
    std::uniform_int_distribution<uint16_t> uniformFirstnameDistribution(0, PersonDataPool().firstnames.size() - 1);
    std::uniform_int_distribution<uint16_t> uniformLastnameDistribution(0, PersonDataPool().lastnames.size() - 1);
    std::uniform_int_distribution<uint8_t> uniformEmailDistribution(0, PersonDataPool().emails.size() - 1);
    std::uniform_int_distribution<uint16_t> uniformCityDistribution(0, PersonDataPool().cities.size() - 1);
    std::uniform_int_distribution<uint8_t> uniformCountryDistribution(0, PersonDataPool().countries.size() - 1);
    std::uniform_int_distribution<uint8_t> uniformProvinceDistribution(0, PersonDataPool().provinces.size() - 1);
    std::uniform_int_distribution<uint8_t> uniformEducationDistribution(0, PersonDataPool().education.size() - 1);
    std::uniform_int_distribution<uint32_t> uniformZipcodeDistribution(1, 99999);
    std::uniform_int_distribution<uint8_t> uniformStreetNumDistribution(1, 99);
    std::uniform_int_distribution<uint8_t> uniformCountryUSDistribution(0, 3);
    std::uniform_int_distribution<uint16_t> uniformCreditcardDistribution(1000, 9999);
    std::uniform_int_distribution<uint32_t> uniformIncomeDistribution(40000, 69000);
    std::uniform_int_distribution<uint8_t> uniformAgeDistribution(30, 44);
    std::uniform_int_distribution<uint8_t> uniformInterestDistribution(0, 4);
    std::uniform_int_distribution<uint16_t> uniformCategoryDistribution(0, 999);
    std::uniform_int_distribution<uint16_t> uniformPhonePrefixDistribution(10, 999);
    std::uniform_int_distribution<uint32_t> uniformPhoneDistribution(123457, 9987653);

    auto& dependencyGeneratorInstance = DependencyGenerator::getInstance(numberOfBuffers, bufferSize);
    auto persons = dependencyGeneratorInstance.getPersons();
    auto numberOfPersons = persons.size();
    auto numberOfRecords = dependencyGeneratorInstance.getNumberOfRecords();
    auto personsToProcess = (numberOfRecords / 10) < numberOfPersons ? (numberOfRecords / 10) : numberOfPersons;

    std::vector<Runtime::TupleBuffer> createdBuffers;
    uint64_t numberOfBuffersToCreate = 1 + personsToProcess * getSchema()->getSchemaSizeInBytes() / bufferSize;
    createdBuffers.reserve(numberOfBuffersToCreate);

    auto memoryLayout = this->getMemoryLayout(bufferSize);
    auto processedPersons = 0UL;

    for (uint64_t curBuffer = 0; curBuffer < numberOfBuffersToCreate; ++curBuffer) {
        Runtime::TupleBuffer bufferRef = allocateBuffer();
        auto dynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(memoryLayout, bufferRef);

        // TODO add designated branch for RowLayout to make it faster (cmp. DefaultDataGenerator.cpp)
        for (uint64_t curRecord = 0; curRecord < dynamicBuffer.getCapacity() && processedPersons < personsToProcess; ++curRecord) {
            auto personsIndex = processedPersons++;

            // create random data
            std::vector<std::string> fields;
            auto firstnameIdx = uniformFirstnameDistribution(generator);
            auto lastnameIdx = uniformLastnameDistribution(generator);
            fields.emplace_back(PersonDataPool().firstnames[firstnameIdx] + " " + PersonDataPool().lastnames[lastnameIdx]);
            auto emailIdx = uniformEmailDistribution(generator);
            fields.emplace_back(PersonDataPool().lastnames[lastnameIdx] + "@" + PersonDataPool().emails[emailIdx]);
            std::ostringstream phone;
            if (uniformBooleanDistribution(generator)) phone << "+" << uniformStreetNumDistribution(generator) << "(" << uniformPhonePrefixDistribution(generator) << ")" << uniformPhoneDistribution(generator);
            fields.emplace_back(phone.str());
            std::ostringstream street;
            std::ostringstream city;
            std::ostringstream country;
            std::ostringstream province;
            if (uniformBooleanDistribution(generator)) {
                auto streetIdx = uniformLastnameDistribution(generator);
                auto cityIdx = uniformCityDistribution(generator);
                auto countryIdx = uniformCountryUSDistribution(generator) != 0 ? 0 : uniformCountryDistribution(generator);
                auto provinceIdx = countryIdx == 0 ? uniformProvinceDistribution(generator) : uniformLastnameDistribution(generator);
                street << uniformStreetNumDistribution(generator) << " " << PersonDataPool().lastnames[streetIdx] << " St";
                city << PersonDataPool().cities[cityIdx];
                if (countryIdx == 0) {
                    country << "United States";
                    province << PersonDataPool().provinces[provinceIdx];
                } else {
                    country << PersonDataPool().countries[countryIdx];
                    province << PersonDataPool().lastnames[provinceIdx];
                }
            }
            fields.emplace_back(street.str());
            fields.emplace_back(city.str());
            fields.emplace_back(country.str());
            fields.emplace_back(province.str());
            auto zipcode = uniformZipcodeDistribution(generator);
            std::ostringstream homepage;
            if (uniformBooleanDistribution(generator)) homepage << "http://www." << PersonDataPool().emails[emailIdx] << "/~" << PersonDataPool().lastnames[lastnameIdx];
            fields.emplace_back(homepage.str());
            std::ostringstream creditcard;
            if (uniformBooleanDistribution(generator)) creditcard << uniformCreditcardDistribution(generator) << " " << uniformCreditcardDistribution(generator) << " " << uniformCreditcardDistribution(generator) << " " << uniformCreditcardDistribution(generator);
            fields.emplace_back(creditcard.str());
            std::ostringstream interest;
            std::ostringstream education;
            std::ostringstream gender;
            auto income = 0.0;
            auto business = false;
            auto age = 0;
            if (uniformBooleanDistribution(generator)) {
                auto numInterests = uniformInterestDistribution(generator);
                for (auto i = 0; i < numInterests; ++i) {
                    interest << uniformCategoryDistribution(generator);
                    if (i < numInterests - 1) interest << ", ";
                }
                if (uniformBooleanDistribution(generator)) education << PersonDataPool().education[uniformEducationDistribution(generator)];
                if (uniformBooleanDistribution(generator)) gender << (uniformBooleanDistribution(generator) == 0 ? "male" : "female");
                if (uniformBooleanDistribution(generator)) age = uniformAgeDistribution(generator);
                if (uniformBooleanDistribution(generator)) business = true;
                income = uniformIncomeDistribution(generator) + uniformStreetNumDistribution(generator) / 100.0;
            }
            fields.emplace_back(interest.str());
            fields.emplace_back(education.str());
            fields.emplace_back(gender.str());

            // write strings to childBuffer in order to store them in TupleBuffer
            std::vector<uint32_t> childIdx;
            for (const std::string& field : fields) {
                auto childTupleBuffer = allocateBuffer();
                auto sizeOfInputField = field.size();
                (*childTupleBuffer.getBuffer<uint32_t>()) = sizeOfInputField;
                std::memcpy(childTupleBuffer.getBuffer() + sizeof(uint32_t), field.c_str(), sizeOfInputField);
                childIdx.emplace_back(dynamicBuffer.getBuffer().storeChildBuffer(childTupleBuffer));
            }

            dynamicBuffer[curRecord]["id"].write<uint64_t>(personsIndex);
            dynamicBuffer[curRecord]["name"].write<Runtime::TupleBuffer::NestedTupleBufferKey>(childIdx[0]);
            dynamicBuffer[curRecord]["email"].write<Runtime::TupleBuffer::NestedTupleBufferKey>(childIdx[1]);
            dynamicBuffer[curRecord]["phone"].write<Runtime::TupleBuffer::NestedTupleBufferKey>(childIdx[2]);
            dynamicBuffer[curRecord]["street"].write<Runtime::TupleBuffer::NestedTupleBufferKey>(childIdx[3]);
            dynamicBuffer[curRecord]["city"].write<Runtime::TupleBuffer::NestedTupleBufferKey>(childIdx[4]);
            dynamicBuffer[curRecord]["country"].write<Runtime::TupleBuffer::NestedTupleBufferKey>(childIdx[5]);
            dynamicBuffer[curRecord]["province"].write<Runtime::TupleBuffer::NestedTupleBufferKey>(childIdx[6]);
            dynamicBuffer[curRecord]["zipcode"].write<uint32_t>(zipcode);
            dynamicBuffer[curRecord]["homepage"].write<Runtime::TupleBuffer::NestedTupleBufferKey>(childIdx[7]);
            dynamicBuffer[curRecord]["creditcard"].write<Runtime::TupleBuffer::NestedTupleBufferKey>(childIdx[8]);
            dynamicBuffer[curRecord]["income"].write<double>(income);
            dynamicBuffer[curRecord]["interest"].write<Runtime::TupleBuffer::NestedTupleBufferKey>(childIdx[9]);
            dynamicBuffer[curRecord]["education"].write<Runtime::TupleBuffer::NestedTupleBufferKey>(childIdx[10]);
            dynamicBuffer[curRecord]["gender"].write<Runtime::TupleBuffer::NestedTupleBufferKey>(childIdx[11]);
            dynamicBuffer[curRecord]["business"].write<bool>(business);
            dynamicBuffer[curRecord]["age"].write<uint8_t>(age);
            dynamicBuffer[curRecord]["timestamp"].write<uint64_t>(persons[personsIndex]);
        }

        dynamicBuffer.setNumberOfTuples(dynamicBuffer.getCapacity());
        createdBuffers.emplace_back(bufferRef);
    }

    return createdBuffers;
}

SchemaPtr PersonGenerator::getSchema() {
    return Schema::create()
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
}

std::string PersonGenerator::getName() { return "NEXMarkPerson"; }

std::string PersonGenerator::toString() {
    std::ostringstream oss;

    oss << getName() << "()";

    return oss.str();
}

} // namespace NES::Benchmark::DataGeneration::NEXMarkGeneration
