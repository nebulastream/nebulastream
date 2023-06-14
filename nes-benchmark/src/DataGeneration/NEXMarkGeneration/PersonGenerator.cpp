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
            dynamicBuffer.pushRecordToBuffer(record);
        }

        dynamicBuffer.setNumberOfTuples(dynamicBuffer.getCapacity());
        createdBuffers.emplace_back(bufferRef);
    }

    return createdBuffers;
}

PersonRecord PersonGenerator::generatePersonRecord(std::vector<uint64_t>& persons, uint64_t personsIndex,
                                                   Runtime::MemoryLayouts::DynamicTupleBuffer dynamicBuffer) {
    auto uniformIntDistributions = UniformIntDistributions();
    std::vector<std::string> fields;
    fields.reserve(12);

    // create random name and email
    auto firstnameIdx = uniformIntDistributions.generateRandomFirstname();
    auto lastnameIdx = uniformIntDistributions.generateRandomLastname();
    fields.emplace_back(PersonDataPool().firstnames[firstnameIdx] + " " + PersonDataPool().lastnames[lastnameIdx]);
    auto emailIdx = uniformIntDistributions.generateRandomEmail();
    fields.emplace_back(PersonDataPool().lastnames[lastnameIdx] + "@" + PersonDataPool().emails[emailIdx]);

    // create random phone number
    std::ostringstream phone;
    if (uniformIntDistributions.generateRandomBoolean()) {
        phone << "+" << uniformIntDistributions.generateRandomOneHundred() << "("
              << uniformIntDistributions.generateRandomPhonePrefix() << ")"
              << uniformIntDistributions.generateRandomPhoneSuffix();
    }
    fields.emplace_back(phone.str());

    // create random address
    auto zipcode = generatePersonAddress(uniformIntDistributions, fields);

    // create random homepage
    std::ostringstream homepage;
    if (uniformIntDistributions.generateRandomBoolean()) {
        homepage << "http://www." << PersonDataPool().emails[emailIdx] << "/~" << PersonDataPool().lastnames[lastnameIdx];
    }
    fields.emplace_back(homepage.str());

    // create random credit card
    std::ostringstream creditcard;
    if (uniformIntDistributions.generateRandomBoolean()) {
        creditcard << uniformIntDistributions.generateRandomCreditcard() << " "
                   << uniformIntDistributions.generateRandomCreditcard() << " "
                   << uniformIntDistributions.generateRandomCreditcard() << " "
                   << uniformIntDistributions.generateRandomCreditcard();
    }
    fields.emplace_back(creditcard.str());

    // create random profile
    auto profile = generatePersonProfile(uniformIntDistributions, fields);

    // write strings to childBuffer in order to store them in TupleBuffer
    std::vector<uint32_t> childIdx;
    childIdx.reserve(fields.size());
    for (const std::string& field : fields) {
        childIdx.emplace_back(Util::writeStringToTupleBuffer(dynamicBuffer.getBuffer(), allocateBuffer(), field));
    }

    // get person dependencies
    auto income = std::get<0>(profile);
    auto business = std::get<1>(profile);
    auto age = std::get<2>(profile);
    auto timestamp = persons[personsIndex];

    return std::make_tuple(personsIndex, childIdx[0], childIdx[1], childIdx[2], childIdx[3], childIdx[4], childIdx[5],
                           childIdx[6], zipcode, childIdx[7], childIdx[8], income, childIdx[9], childIdx[10], childIdx[11],
                           business, age, timestamp);
}

uint32_t PersonGenerator::generatePersonAddress(NEXMarkGeneration::UniformIntDistributions uniformIntDistributions,
                                                std::vector<std::string>& fields) {
    std::ostringstream street;
    std::ostringstream city;
    std::ostringstream country;
    std::ostringstream province;
    auto zipcode = 0U;

    if (uniformIntDistributions.generateRandomBoolean()) {
        auto streetIdx = uniformIntDistributions.generateRandomLastname();
        auto cityIdx = uniformIntDistributions.generateRandomCity();
        auto countryIdx = uniformIntDistributions.isCountryUS() ? 0 : uniformIntDistributions.generateRandomCountry();
        auto provinceIdx = countryIdx == 0 ? uniformIntDistributions.generateRandomProvince() : uniformIntDistributions.generateRandomLastname();

        street << uniformIntDistributions.generateRandomOneHundred() << " " << PersonDataPool().lastnames[streetIdx] << " St";
        city << PersonDataPool().cities[cityIdx];

        if (countryIdx == 0) {
            country << "United States";
            province << PersonDataPool().provinces[provinceIdx];
        } else {
            country << PersonDataPool().countries[countryIdx];
            province << PersonDataPool().lastnames[provinceIdx];
        }

        zipcode = uniformIntDistributions.generateRandomZipcode();
    }

    fields.emplace_back(street.str());
    fields.emplace_back(city.str());
    fields.emplace_back(country.str());
    fields.emplace_back(province.str());

    return zipcode;
}

std::tuple<double, bool, uint8_t> PersonGenerator::generatePersonProfile(NEXMarkGeneration::UniformIntDistributions uniformIntDistributions,
                                                                         std::vector<std::string>& fields) {
    std::ostringstream interest;
    std::ostringstream education;
    std::ostringstream gender;
    auto income = 0.0;
    auto business = false;
    auto age = 0U;

    if (uniformIntDistributions.generateRandomBoolean()) {
        auto numInterests = uniformIntDistributions.generateRandomInterest();
        for (auto i = 0; i < numInterests; ++i) {
            interest << uniformIntDistributions.generateRandomCategory();
            if (i < numInterests - 1) {
                interest << "; ";
            }
        }

        if (uniformIntDistributions.generateRandomBoolean()) {
            education << PersonDataPool().education[uniformIntDistributions.generateRandomEducation()];
        }

        if (uniformIntDistributions.generateRandomBoolean()) {
            gender << (uniformIntDistributions.generateRandomBoolean() == 0 ? "male" : "female");
        }

        if (uniformIntDistributions.generateRandomBoolean()) {
            age = uniformIntDistributions.generateRandomAge();
        }

        if (uniformIntDistributions.generateRandomBoolean()) {
            business = true;
        }

        income = uniformIntDistributions.generateRandomIncome() + uniformIntDistributions.generateRandomOneHundred() / 100.0;
    }

    fields.emplace_back(interest.str());
    fields.emplace_back(education.str());
    fields.emplace_back(gender.str());

    return std::make_tuple(income, business, age);
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
