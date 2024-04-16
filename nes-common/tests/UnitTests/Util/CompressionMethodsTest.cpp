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

#include <BaseUnitTest.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/CompressionMethods.hpp>
#include <gtest/gtest.h>
#include <random>

namespace NES {

class CompressionMethodsTest : public Testing::BaseUnitTest {

  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("CompressionMethodsTest.log", NES::LogLevel::LOG_DEBUG);
        NES_DEBUG("Setup CompressionMethodsTest test class.");
    }

};

/**
 * @brief This test case tests the run length encoding and decoding methods for a simple hardcoded example
 */
TEST_F(CompressionMethodsTest, testRunLengthEncodingSimple) {
    const auto expectedDecodedData = "aaaabbbccccccccccc";
    // Testing encoding and decoding
    const auto encodedData = CompressionMethods::runLengthEncoding(expectedDecodedData);
    const auto decodedData = CompressionMethods::runLengthDecoding(encodedData);
    EXPECT_EQ(decodedData, expectedDecodedData);
}

/**
 * @brief This test case tests the run length encoding and decoding methods for random string.
 */
TEST_F(CompressionMethodsTest, testRunLengthEncodingRandomTexts) {
    constexpr auto numberOfRandomTexts = 100;
    constexpr auto numberOfCharsPerText = 1000;
    std::random_device rd;
    std::mt19937 gen(rd());

    // Setting the maximum number of characters to 5, as we want to increase the chance of similar chars next to each other
    std::uniform_int_distribution<> dis(1, 5);

    for (int i = 0; i < numberOfRandomTexts; i++) {
        std::string randomText;
        for (int j = 0; j < numberOfCharsPerText; j++) {
            randomText += static_cast<char>(dis(gen));
        }

        const auto encodedData = CompressionMethods::runLengthEncoding(randomText);
        const auto decodedData = CompressionMethods::runLengthDecoding(encodedData);
        EXPECT_EQ(decodedData, randomText);
    }
}

TEST_F(CompressionMethodsTest, testRunLengthEncodingForCountMinData) {
    // Test data
    std::vector<uint64_t> countMinData = {33, 47, 45, 35, 33, 37, 29, 40, 42, 45, 39, 34, 49, 36, 35};
    std::string countMinDataString;
    const auto dataSizeBytes = countMinData.size() * sizeof(uint64_t);
    countMinDataString.resize(dataSizeBytes);
    std::memcpy(countMinDataString.data(), countMinData.data(), dataSizeBytes);


    // Testing the encoding and decoding
    const auto encodedData = CompressionMethods::runLengthEncoding(countMinDataString);
    const auto decodedData = CompressionMethods::runLengthDecoding(encodedData);
    EXPECT_EQ(decodedData, countMinDataString);
}
}// namespace NES