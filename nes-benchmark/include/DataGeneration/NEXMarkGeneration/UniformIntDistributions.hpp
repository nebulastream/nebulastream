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

#ifndef NES_UNIFORMINTDISTRIBUTIONS_HPP
#define NES_UNIFORMINTDISTRIBUTIONS_HPP

#include <cstdint>
#include <random>

namespace NES::Benchmark::DataGeneration::NEXMarkGeneration {
class UniformIntDistributions {
  public:
    /**
     * @brief constructor of UniformIntDistribution - initializes generator with seed
     */
    UniformIntDistributions();

    /**
     * @brief generates a random boolean
     * @return either 0 or 1
     */
    uint8_t generateRandomBoolean();

    /**
     * @brief generates a random reserve (value between 1000 and 2000)
     * @return random reserve
     */
    uint16_t generateRandomReserve();

    /**
     * @brief generates a random category (value between 0 and 302)
     * @return random category
     */
    uint16_t generateRandomCategory();

    /**
     * @brief generates a random quantity (value between 1 and 10)
     * @return random quantity
     */
    uint8_t generateRandomQuantity();

    /**
     * @brief generates the index for a random firstname
     * @return random index
     */
    uint16_t generateRandomFirstname();

    /**
     * @brief generates the index for a random lastname
     * @return random index
     */
    uint16_t generateRandomLastname();

    /**
     * @brief generates the index for a random email
     * @return random index
     */
    uint8_t generateRandomEmail();
    /**
     * @brief generates the index for a random city
     * @return random index
     */
    uint16_t generateRandomCity();

    /**
     * @brief generates the index for a random country
     * @return random index
     */
    uint8_t generateRandomCountry();

    /**
     * @brief generates the index for a random province
     * @return random index
     */
    uint8_t generateRandomProvince();

    /**
     * @brief generates the index for a random education
     * @return random index
     */
    uint8_t generateRandomEducation();

    /**
     * @brief generates a random zipcode (value between 1 and 99999)
     * @return random zipcode
     */
    uint32_t generateRandomZipcode();

    /**
     * @brief generates a random number between 1 and 99
     * @return random number
     */
    uint8_t generateRandomOneHundred();

    /**
     * @brief generates a random credit card (value between 1000 and 9999)
     * @return random credit card
     */
    uint16_t generateRandomCreditcard();

    /**
     * @brief generates a random income (value between 40000 and 69999)
     * @return random income
     */
    uint32_t generateRandomIncome();

    /**
     * @brief generates a random age (value between 30 and 44)
     * @return random age
     */
    uint8_t generateRandomAge();

    /**
     * @brief generates a random number of interests (value between 0 and 4)
     * @return random number of interests
     */
    uint8_t generateRandomInterest();

    /**
     * @brief generates a random phone number prefix (value between 10 and 999)
     * @return random phone number prefix
     */
    uint16_t generateRandomPhonePrefix();

    /**
     * @brief generates a random phone number suffix (value between 123457 and 9987653)
     * @return random phone number suffix
     */
    uint32_t generateRandomPhoneSuffix();

    /**
     * @brief decides whether the country is the US or random country from the country vector
     * @return true or false
     */
    bool isCountryUS();

  private:
    std::mt19937 generator;
};
} //namespace NES::Benchmark::DataGeneration::NEXMarkGeneration

#endif//NES_UNIFORMINTDISTRIBUTIONS_HPP
