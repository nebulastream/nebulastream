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

#include <DataGeneration/NEXMarkGeneration/UniformIntDistributions.hpp>
#include <DataGeneration/NEXMarkGeneration/PersonDataPool.hpp>

namespace NES::Benchmark::DataGeneration::NEXMarkGeneration {

// using seed to generate a predictable sequence of values for deterministic behavior
UniformIntDistributions::UniformIntDistributions() : generator(generatorSeed) {}

uint8_t UniformIntDistributions::generateRandomBoolean() {
    static std::uniform_int_distribution<uint8_t> uniformBooleanDistribution(0, 1);
    return uniformBooleanDistribution(generator);
}

uint16_t UniformIntDistributions::generateRandomReserve() {
    static std::uniform_int_distribution<uint16_t> uniformReserveDistribution(1000, 2000);
    return uniformReserveDistribution(generator);
}

uint16_t UniformIntDistributions::generateRandomCategory() {
    static std::uniform_int_distribution<uint16_t> uniformCategoryDistribution(0, 302);
    return uniformCategoryDistribution(generator);
}

uint8_t UniformIntDistributions::generateRandomQuantity() {
    static std::uniform_int_distribution<uint8_t> uniformQuantityDistribution(1, 10);
    return uniformQuantityDistribution(generator);
}

uint16_t UniformIntDistributions::generateRandomFirstname() {
    static std::uniform_int_distribution<uint16_t> uniformFirstnameDistribution(0, PersonDataPool().firstnames.size() - 1);
    return uniformFirstnameDistribution(generator);
}

uint16_t UniformIntDistributions::generateRandomLastname() {
    static std::uniform_int_distribution<uint16_t> uniformLastnameDistribution(0, PersonDataPool().lastnames.size() - 1);
    return uniformLastnameDistribution(generator);
}

uint8_t UniformIntDistributions::generateRandomEmail() {
    static std::uniform_int_distribution<uint8_t> uniformEmailDistribution(0, PersonDataPool().emails.size() - 1);
    return uniformEmailDistribution(generator);
}

uint16_t UniformIntDistributions::generateRandomCity() {
    static std::uniform_int_distribution<uint16_t> uniformCityDistribution(0, PersonDataPool().cities.size() - 1);
    return uniformCityDistribution(generator);
}

uint8_t UniformIntDistributions::generateRandomCountry() {
    static std::uniform_int_distribution<uint8_t> uniformCountryDistribution(0, PersonDataPool().countries.size() - 1);
    return uniformCountryDistribution(generator);
}

uint8_t UniformIntDistributions::generateRandomProvince() {
    static std::uniform_int_distribution<uint8_t> uniformProvinceDistribution(0, PersonDataPool().provinces.size() - 1);
    return uniformProvinceDistribution(generator);
}

uint8_t UniformIntDistributions::generateRandomEducation() {
    static std::uniform_int_distribution<uint8_t> uniformEducationDistribution(0, PersonDataPool().education.size() - 1);
    return uniformEducationDistribution(generator);
}

uint32_t UniformIntDistributions::generateRandomZipcode() {
    static std::uniform_int_distribution<uint32_t> uniformZipcodeDistribution(1, 99999);
    return uniformZipcodeDistribution(generator);
}

uint8_t UniformIntDistributions::generateRandomOneHundred() {
    static std::uniform_int_distribution<uint8_t> uniformHundredDistribution(1, 99);
    return uniformHundredDistribution(generator);
}

uint16_t UniformIntDistributions::generateRandomCreditcard() {
    static std::uniform_int_distribution<uint16_t> uniformCreditcardDistribution(1000, 9999);
    return uniformCreditcardDistribution(generator);
}

uint32_t UniformIntDistributions::generateRandomIncome() {
    static std::uniform_int_distribution<uint32_t> uniformIncomeDistribution(40000, 69999);
    return uniformIncomeDistribution(generator);
}

uint8_t UniformIntDistributions::generateRandomAge() {
    static std::uniform_int_distribution<uint8_t> uniformAgeDistribution(30, 44);
    return uniformAgeDistribution(generator);
}

uint8_t UniformIntDistributions::generateRandomInterest() {
    static std::uniform_int_distribution<uint8_t> uniformInterestDistribution(0, 4);
    return uniformInterestDistribution(generator);
}

uint16_t UniformIntDistributions::generateRandomPhonePrefix() {
    static std::uniform_int_distribution<uint16_t> uniformPhonePrefixDistribution(10, 999);
    return uniformPhonePrefixDistribution(generator);
}

uint32_t UniformIntDistributions::generateRandomPhoneSuffix() {
    static std::uniform_int_distribution<uint32_t> uniformPhoneSuffixDistribution(123457, 9987653);
    return uniformPhoneSuffixDistribution(generator);
}

bool UniformIntDistributions::isCountryUS() {
    static std::uniform_int_distribution<uint8_t> uniformCountryUSDistribution(0, 3);
    return uniformCountryUSDistribution(generator) != 0;
}

} //namespace NES::Benchmark::DataGeneration::NEXMarkGeneration
