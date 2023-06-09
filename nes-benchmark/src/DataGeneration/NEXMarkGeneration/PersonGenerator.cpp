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
#include <Util/Core.hpp>
#include <cmath>

namespace NES::Benchmark::DataGeneration::NEXMarkGeneration {

std::vector<Runtime::TupleBuffer> PersonGenerator::createData(size_t numberOfBuffers, size_t bufferSize) {
    auto& dependencyGeneratorInstance = DependencyGenerator::getInstance(numberOfBuffers, bufferSize);
    auto persons = dependencyGeneratorInstance.getPersons();
    auto numberOfPersons = persons.size();
    auto numberOfRecords = dependencyGeneratorInstance.getNumberOfRecords();
    auto personsToProcess = (recordsInit + numberOfRecords / 10) < numberOfPersons ? (recordsInit + numberOfRecords / 10) : numberOfPersons;

    std::vector<Runtime::TupleBuffer> createdBuffers;
    uint64_t numberOfBuffersToCreate = std::ceil(personsToProcess * getSchema()->getSchemaSizeInBytes() / (bufferSize * 1.0));
    createdBuffers.reserve(numberOfBuffersToCreate);
    NES_INFO("personsToProcess: " << personsToProcess << "\tnumberOfPersonsBuffersToCreate: " << numberOfBuffersToCreate);

    auto memoryLayout = this->getMemoryLayout(bufferSize);
    auto processedPersons = 0UL;

    for (uint64_t curBuffer = 0; curBuffer < numberOfBuffersToCreate; ++curBuffer) {
        Runtime::TupleBuffer bufferRef = allocateBuffer();
        auto dynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(memoryLayout, bufferRef);

        for (uint64_t curRecord = 0; curRecord < dynamicBuffer.getCapacity() && processedPersons < personsToProcess; ++curRecord) {
            auto personsIndex = processedPersons++;
            auto record = generatePersonRecord(persons, personsIndex, dynamicBuffer);

            dynamicBuffer[curRecord]["id"].write<uint64_t>(personsIndex);
            dynamicBuffer[curRecord]["name"].write<Runtime::TupleBuffer::NestedTupleBufferKey>(record.name);
            dynamicBuffer[curRecord]["email"].write<Runtime::TupleBuffer::NestedTupleBufferKey>(record.email);
            dynamicBuffer[curRecord]["phone"].write<Runtime::TupleBuffer::NestedTupleBufferKey>(record.phone);
            dynamicBuffer[curRecord]["street"].write<Runtime::TupleBuffer::NestedTupleBufferKey>(record.street);
            dynamicBuffer[curRecord]["city"].write<Runtime::TupleBuffer::NestedTupleBufferKey>(record.city);
            dynamicBuffer[curRecord]["country"].write<Runtime::TupleBuffer::NestedTupleBufferKey>(record.country);
            dynamicBuffer[curRecord]["province"].write<Runtime::TupleBuffer::NestedTupleBufferKey>(record.province);
            dynamicBuffer[curRecord]["zipcode"].write<uint32_t>(record.zipcode);
            dynamicBuffer[curRecord]["homepage"].write<Runtime::TupleBuffer::NestedTupleBufferKey>(record.homepage);
            dynamicBuffer[curRecord]["creditcard"].write<Runtime::TupleBuffer::NestedTupleBufferKey>(record.creditcard);
            dynamicBuffer[curRecord]["income"].write<double>(record.income);
            dynamicBuffer[curRecord]["interest"].write<Runtime::TupleBuffer::NestedTupleBufferKey>(record.interest);
            dynamicBuffer[curRecord]["education"].write<Runtime::TupleBuffer::NestedTupleBufferKey>(record.education);
            dynamicBuffer[curRecord]["gender"].write<Runtime::TupleBuffer::NestedTupleBufferKey>(record.gender);
            dynamicBuffer[curRecord]["business"].write<bool>(record.business);
            dynamicBuffer[curRecord]["age"].write<uint8_t>(record.age);
            dynamicBuffer[curRecord]["timestamp"].write<uint64_t>(record.timestamp);
        }

        dynamicBuffer.setNumberOfTuples(dynamicBuffer.getCapacity());
        createdBuffers.emplace_back(bufferRef);
    }

    return createdBuffers;
}

PersonRecord PersonGenerator::generatePersonRecord(std::vector<uint64_t>& persons, uint64_t personsIndex,
                                                   Runtime::MemoryLayouts::DynamicTupleBuffer dynamicBuffer) {
    // using seed to generate a predictable sequence of values for deterministic behavior
    static std::mt19937 generator(42);
    static std::uniform_int_distribution<uint8_t> uniformBooleanDistribution(0, 1);
    static std::uniform_int_distribution<uint16_t> uniformFirstnameDistribution(0, PersonDataPool().firstnames.size() - 1);
    static std::uniform_int_distribution<uint16_t> uniformLastnameDistribution(0, PersonDataPool().lastnames.size() - 1);
    static std::uniform_int_distribution<uint8_t> uniformEmailDistribution(0, PersonDataPool().emails.size() - 1);
    static std::uniform_int_distribution<uint16_t> uniformCityDistribution(0, PersonDataPool().cities.size() - 1);
    static std::uniform_int_distribution<uint8_t> uniformCountryDistribution(0, PersonDataPool().countries.size() - 1);
    static std::uniform_int_distribution<uint8_t> uniformProvinceDistribution(0, PersonDataPool().provinces.size() - 1);
    static std::uniform_int_distribution<uint8_t> uniformEducationDistribution(0, PersonDataPool().education.size() - 1);
    static std::uniform_int_distribution<uint32_t> uniformZipcodeDistribution(1, 99999);
    static std::uniform_int_distribution<uint8_t> uniformHundredDistribution(1, 99);
    static std::uniform_int_distribution<uint8_t> uniformCountryUSDistribution(0, 3);
    static std::uniform_int_distribution<uint16_t> uniformCreditcardDistribution(1000, 9999);
    static std::uniform_int_distribution<uint32_t> uniformIncomeDistribution(40000, 69999);
    static std::uniform_int_distribution<uint8_t> uniformAgeDistribution(30, 44);
    static std::uniform_int_distribution<uint8_t> uniformInterestDistribution(0, 4);
    static std::uniform_int_distribution<uint16_t> uniformCategoryDistribution(0, 999);
    static std::uniform_int_distribution<uint16_t> uniformPhonePrefixDistribution(10, 999);
    static std::uniform_int_distribution<uint32_t> uniformPhoneSuffixDistribution(123457, 9987653);

    PersonRecord record;
    std::vector<std::string> fields;
    fields.reserve(12);

    // create random data
    auto firstnameIdx = uniformFirstnameDistribution(generator);
    auto lastnameIdx = uniformLastnameDistribution(generator);
    fields.emplace_back(PersonDataPool().firstnames[firstnameIdx] + " " + PersonDataPool().lastnames[lastnameIdx]);
    auto emailIdx = uniformEmailDistribution(generator);
    fields.emplace_back(PersonDataPool().lastnames[lastnameIdx] + "@" + PersonDataPool().emails[emailIdx]);
    std::ostringstream phone;
    if (uniformBooleanDistribution(generator)) {
        phone << "+" << uniformHundredDistribution(generator) << "(" << uniformPhonePrefixDistribution(generator) << ")" << uniformPhoneSuffixDistribution(generator);
    }
    fields.emplace_back(phone.str());

    std::ostringstream street;
    std::ostringstream city;
    std::ostringstream country;
    std::ostringstream province;
    record.zipcode = 0;
    if (uniformBooleanDistribution(generator)) {
        auto streetIdx = uniformLastnameDistribution(generator);
        auto cityIdx = uniformCityDistribution(generator);
        auto countryIdx = uniformCountryUSDistribution(generator) != 0 ? 0 : uniformCountryDistribution(generator);
        auto provinceIdx = countryIdx == 0 ? uniformProvinceDistribution(generator) : uniformLastnameDistribution(generator);
        street << uniformHundredDistribution(generator) << " " << PersonDataPool().lastnames[streetIdx] << " St";
        city << PersonDataPool().cities[cityIdx];
        if (countryIdx == 0) {
            country << "United States";
            province << PersonDataPool().provinces[provinceIdx];
        } else {
            country << PersonDataPool().countries[countryIdx];
            province << PersonDataPool().lastnames[provinceIdx];
        }
        record.zipcode = uniformZipcodeDistribution(generator);
    }
    fields.emplace_back(street.str());
    fields.emplace_back(city.str());
    fields.emplace_back(country.str());
    fields.emplace_back(province.str());

    std::ostringstream homepage;
    if (uniformBooleanDistribution(generator)) {
        homepage << "http://www." << PersonDataPool().emails[emailIdx] << "/~" << PersonDataPool().lastnames[lastnameIdx];
    }
    fields.emplace_back(homepage.str());
    std::ostringstream creditcard;
    if (uniformBooleanDistribution(generator)) {
        creditcard << uniformCreditcardDistribution(generator) << " " << uniformCreditcardDistribution(generator) << " " << uniformCreditcardDistribution(generator) << " " << uniformCreditcardDistribution(generator);
    }
    fields.emplace_back(creditcard.str());

    std::ostringstream interest;
    std::ostringstream education;
    std::ostringstream gender;
    record.income = 0.0;
    record.business = false;
    record.age = 0;
    if (uniformBooleanDistribution(generator)) {
        auto numInterests = uniformInterestDistribution(generator);
        for (auto i = 0; i < numInterests; ++i) {
            interest << uniformCategoryDistribution(generator);
            if (i < numInterests - 1) {
                interest << ", ";
            }
        }
        if (uniformBooleanDistribution(generator)) {
            education << PersonDataPool().education[uniformEducationDistribution(generator)];
        }
        if (uniformBooleanDistribution(generator)) {
            gender << (uniformBooleanDistribution(generator) == 0 ? "male" : "female");
        }
        if (uniformBooleanDistribution(generator)) {
            record.age = uniformAgeDistribution(generator);
        }
        if (uniformBooleanDistribution(generator)) {
            record.business = true;
        }
        record.income = uniformIncomeDistribution(generator) + uniformHundredDistribution(generator) / 100.0;
    }
    fields.emplace_back(interest.str());
    fields.emplace_back(education.str());
    fields.emplace_back(gender.str());

    // write strings to childBuffer in order to store them in TupleBuffer
    std::vector<uint32_t> childIdx;
    childIdx.reserve(fields.size());
    for (const std::string& field : fields) {
        childIdx.emplace_back(Util::writeStringToTupleBuffer(dynamicBuffer.getBuffer(), allocateBuffer(), field));
    }

    record.name = childIdx[0];
    record.email = childIdx[1];
    record.phone = childIdx[2];
    record.street = childIdx[3];
    record.city = childIdx[4];
    record.country = childIdx[5];
    record.province = childIdx[6];
    record.homepage = childIdx[7];
    record.creditcard = childIdx[8];
    record.interest = childIdx[9];
    record.education = childIdx[10];
    record.gender = childIdx[11];

    // write person dependency to record
    record.timestamp = persons[personsIndex];

    return record;
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
