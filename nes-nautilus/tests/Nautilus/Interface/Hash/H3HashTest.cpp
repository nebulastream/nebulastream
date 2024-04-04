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

#include <BaseIntegrationTest.hpp>
#include <Nautilus/Interface/DataTypes/Value.hpp>
#include <Nautilus/Interface/Hash/H3Hash.hpp>
#include <Nautilus/Interface/Hash/HashFunction.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Common.hpp>
#include <array>
#include <vector>

namespace NES::Nautilus::Interface {
class H3HashTest : public Testing::BaseUnitTest {
  public:
    static constexpr auto NUMBER_OF_KEYS_TO_TEST = 5;
    static constexpr auto NUMBER_OF_ROWS = 3;
    std::array<std::vector<uint64_t>, NUMBER_OF_ROWS> allH3Seeds;
    std::vector<std::unique_ptr<HashFunction>> allH3Hashes;

    static void SetUpTestCase() {
        NES::Logger::setupLogging("H3HashTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("H3HashTest test class SetUpTestCase.");
    }

    void SetUp() override {
        BaseUnitTest::SetUp();

        std::random_device rd;
        std::mt19937 gen(H3_SEED);
        std::uniform_int_distribution<uint64_t> distribution;
        auto numberOfBitsInKey = sizeof(uint64_t) * 8;

        for (auto row = 0UL; row < NUMBER_OF_ROWS; ++row) {
            for (auto keyBit = 0UL; keyBit < numberOfBitsInKey; ++keyBit) {
                allH3Seeds[row].emplace_back(distribution(gen));
            }
        }
        allH3Hashes.clear();
    }

    static void TearDownTestCase() { NES_INFO("H3HashTest test class TearDownTestCase."); }
};

/**
 * @brief Gets a bitmask for a given number of bits, for example 8 --> 0xFF and 32 -> 0xFFFF
 * @param numberOfKeyBits
 * @return uint64_t
 */
uint64_t getBitMask(uint64_t numberOfKeyBits) {
    switch (numberOfKeyBits) {
        case 8: return 0xFF;
        case 16: return 0xFFFF;
        case 32: return 0xFFFFFFFF;
        case 64: return 0xFFFFFFFFFFFFFFFF;
        default: NES_THROW_RUNTIME_ERROR("getBitMask got a numberOfKeyBits that was not expected!");
    }
}

/**
 * @brief Masks the bits that are larger than numberOfKeyBits
 * @param allSeeds
 */
void maskSeeds(std::vector<uint64_t>& allSeeds, uint64_t numberOfKeyBits) {
    uint64_t mask = getBitMask(numberOfKeyBits);
    for (auto& seed : allSeeds) {
        seed &= mask;
    }
}

TEST_F(H3HashTest, simpleH3testDouble) {
    std::vector<std::array<uint64_t, NUMBER_OF_ROWS>> expectedHashes = {
        {0x0, 0x0, 0x0},
        {0x5fe1dc66cbea3db3, 0x47eb52fb9b6698bb, 0x1c79d662a26e2c5},
        {0xf362035c2ef5950e, 0x8aee217b46a7e1ec, 0x82c055c788ba159a},
        {0xac83df3ae51fa8bd, 0xcd057380ddc17957, 0x8307c8a1a29cf75f},
        {0xbb63f46ac799d447, 0x24139c284bd8949c, 0x6adb72887c1dd13d},
        {0xe482280c0c73e9f4, 0x63f8ced3d0be0c27, 0x6b1cefee563b33f8}};

    const auto numberOfKeyBits = sizeof(uint64_t) * 8;
    for (auto row = 0UL; row < NUMBER_OF_ROWS; ++row) {
        maskSeeds(allH3Seeds[row], numberOfKeyBits);
        allH3Hashes.emplace_back(std::make_unique<H3Hash>(numberOfKeyBits));
    }

    for (uint64_t key = 0; key < NUMBER_OF_KEYS_TO_TEST; ++key) {
        // To use the same hashes as for the uint64_t, we have to memcpy it into a double
        double keyDouble;
        std::memcpy(&keyDouble, &key, sizeof(double));
        for (auto row = 0UL; row < NUMBER_OF_ROWS; ++row) {
            Value<Double> valKey(keyDouble);
            Value<MemRef> h3SeedMemRef((int8_t*) allH3Seeds[row].data());

            auto expectedHash = expectedHashes[key][row];
            auto calcHash = allH3Hashes[row]->calculateWithState(valKey, h3SeedMemRef).getValue().getValue();
            EXPECT_EQ(expectedHash, calcHash);
        }
    }
}

TEST_F(H3HashTest, simpleH3testFloat) {
    std::vector<std::array<uint32_t, NUMBER_OF_ROWS>> expectedHashes = {{0x0, 0x0, 0x0},
                                                                        {0xcbea3db3, 0x9b6698bb, 0x2a26e2c5},
                                                                        {0x2ef5950e, 0x46a7e1ec, 0x88ba159a},
                                                                        {0xe51fa8bd, 0xddc17957, 0xa29cf75f},
                                                                        {0xc799d447, 0x4bd8949c, 0x7c1dd13d},
                                                                        {0xc73e9f4, 0xd0be0c27, 0x563b33f8}};

    const auto numberOfKeyBits = sizeof(uint32_t) * 8;
    for (auto row = 0UL; row < NUMBER_OF_ROWS; ++row) {
        maskSeeds(allH3Seeds[row], numberOfKeyBits);
        allH3Hashes.emplace_back(std::make_unique<H3Hash>(numberOfKeyBits));
    }

    for (uint32_t key = 0; key < NUMBER_OF_KEYS_TO_TEST; ++key) {
        // To use the same hashes as for the uint32_t, we have to memcpy it into a double
        float keyFloat;
        std::memcpy(&keyFloat, &key, sizeof(float));
        for (auto row = 0UL; row < NUMBER_OF_ROWS; ++row) {
            Value<Float> valKey(keyFloat);
            Value<MemRef> h3SeedMemRef((int8_t*) allH3Seeds[row].data());

            auto expectedHash = expectedHashes[key][row];
            auto calcHash = allH3Hashes[row]->calculateWithState(valKey, h3SeedMemRef).getValue().getValue();
            EXPECT_EQ(expectedHash, calcHash);
        }
    }
}

TEST_F(H3HashTest, simpleH3testUInt64) {
    std::vector<std::array<uint64_t, NUMBER_OF_ROWS>> expectedHashes = {
        {0x0, 0x0, 0x0},
        {0x5fe1dc66cbea3db3, 0x47eb52fb9b6698bb, 0x1c79d662a26e2c5},
        {0xf362035c2ef5950e, 0x8aee217b46a7e1ec, 0x82c055c788ba159a},
        {0xac83df3ae51fa8bd, 0xcd057380ddc17957, 0x8307c8a1a29cf75f},
        {0xbb63f46ac799d447, 0x24139c284bd8949c, 0x6adb72887c1dd13d},
        {0xe482280c0c73e9f4, 0x63f8ced3d0be0c27, 0x6b1cefee563b33f8}};

    const auto numberOfKeyBits = sizeof(uint64_t) * 8;
    for (auto row = 0UL; row < NUMBER_OF_ROWS; ++row) {
        maskSeeds(allH3Seeds[row], numberOfKeyBits);
        allH3Hashes.emplace_back(std::make_unique<H3Hash>(numberOfKeyBits));
    }

    for (uint64_t key = 0; key < NUMBER_OF_KEYS_TO_TEST; ++key) {
        for (auto row = 0UL; row < NUMBER_OF_ROWS; ++row) {
            Value<UInt64> valKey(key);
            Value<MemRef> h3SeedMemRef((int8_t*) allH3Seeds[row].data());

            auto expectedHash = expectedHashes[key][row];
            auto calcHash = allH3Hashes[row]->calculateWithState(valKey, h3SeedMemRef).getValue().getValue();
            EXPECT_EQ(expectedHash, calcHash);
        }
    }
}

TEST_F(H3HashTest, simpleH3testUInt32) {
    std::vector<std::array<uint32_t, NUMBER_OF_ROWS>> expectedHashes = {{0x0, 0x0, 0x0},
                                                                        {0xcbea3db3, 0x9b6698bb, 0x2a26e2c5},
                                                                        {0x2ef5950e, 0x46a7e1ec, 0x88ba159a},
                                                                        {0xe51fa8bd, 0xddc17957, 0xa29cf75f},
                                                                        {0xc799d447, 0x4bd8949c, 0x7c1dd13d},
                                                                        {0xc73e9f4, 0xd0be0c27, 0x563b33f8}};

    constexpr auto numberOfKeyBits = sizeof(uint32_t) * 8;
    for (auto row = 0UL; row < NUMBER_OF_ROWS; ++row) {
        maskSeeds(allH3Seeds[row], numberOfKeyBits);
        allH3Hashes.emplace_back(std::make_unique<H3Hash>(numberOfKeyBits));
    }

    for (uint32_t key = 0; key < NUMBER_OF_KEYS_TO_TEST; ++key) {
        for (auto row = 0UL; row < NUMBER_OF_ROWS; ++row) {
            Value<UInt32> valKey(key);
            Value<MemRef> h3SeedMemRef((int8_t*) allH3Seeds[row].data());

            auto expectedHash = expectedHashes[key][row];
            auto calcHash = allH3Hashes[row]->calculateWithState(valKey, h3SeedMemRef).getValue().getValue();
            EXPECT_EQ(expectedHash, calcHash);
        }
    }
}

TEST_F(H3HashTest, simpleH3testUInt16) {
    std::vector<std::array<uint16_t, NUMBER_OF_ROWS>> expectedHashes = {{0x0, 0x0, 0x0},
                                                                        {0x3db3, 0x98bb, 0xe2c5},
                                                                        {0x950e, 0xe1ec, 0x159a},
                                                                        {0xa8bd, 0x7957, 0xf75f},
                                                                        {0xd447, 0x949c, 0xd13d},
                                                                        {0xe9f4, 0x0c27, 0x33f8}};

    const auto numberOfKeyBits = sizeof(uint16_t) * 8;
    for (auto row = 0UL; row < NUMBER_OF_ROWS; ++row) {
        maskSeeds(allH3Seeds[row], numberOfKeyBits);
        allH3Hashes.emplace_back(std::make_unique<H3Hash>(numberOfKeyBits));
    }

    for (uint16_t key = 0; key < NUMBER_OF_KEYS_TO_TEST; ++key) {
        for (auto row = 0UL; row < NUMBER_OF_ROWS; ++row) {
            Value<UInt16> valKey(key);
            Value<MemRef> h3SeedMemRef((int8_t*) allH3Seeds[row].data());

            auto expectedHash = expectedHashes[key][row];
            auto calcHash = allH3Hashes[row]->calculateWithState(valKey, h3SeedMemRef).getValue().getValue();
            EXPECT_EQ(expectedHash, calcHash);
        }
    }
}

TEST_F(H3HashTest, simpleH3testUInt8) {
    std::vector<std::array<uint8_t, NUMBER_OF_ROWS>> expectedHashes =
        {{0x00, 0x00, 0x00}, {0xb3, 0xbb, 0xc5}, {0x0e, 0xec, 0x9a}, {0xbd, 0x57, 0x5f}, {0x47, 0x9c, 0x3d}, {0xf4, 0x27, 0xf8}};

    const auto numberOfKeyBits = sizeof(uint8_t) * 8;
    for (auto row = 0UL; row < NUMBER_OF_ROWS; ++row) {
        maskSeeds(allH3Seeds[row], numberOfKeyBits);
        allH3Hashes.emplace_back(std::make_unique<H3Hash>(numberOfKeyBits));
    }

    for (uint8_t key = 0; key < NUMBER_OF_KEYS_TO_TEST; ++key) {
        for (auto row = 0UL; row < NUMBER_OF_ROWS; ++row) {
            Value<UInt8> valKey(key);
            Value<MemRef> h3SeedMemRef((int8_t*) allH3Seeds[row].data());

            auto expectedHash = expectedHashes[key][row];
            auto calcHash = allH3Hashes[row]->calculateWithState(valKey, h3SeedMemRef).getValue().getValue();
            EXPECT_EQ(expectedHash, calcHash);
        }
    }
}

}// namespace NES::Nautilus::Interface