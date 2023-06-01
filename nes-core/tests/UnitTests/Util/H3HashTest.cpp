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

#include <Util/Experimental/H3Hash.hpp>
#include <Util/Experimental/Hash.hpp>
#include <Util/Logger/Logger.hpp>
#include <NesBaseTest.hpp>
#include <vector>
#include <array>

namespace NES {
class H3HashTest : public Testing::NESBaseTest {
public:
    static void SetUpTestCase() {
        NES::Logger::setupLogging("H3HashTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO2("H3HashTest test class SetUpTestCase.");
    }
    static void TearDownTestCase() { NES_INFO2("H3HashTest test class TearDownTestCase."); }
};

TEST_F(H3HashTest, simpleH3testUInt64) {
    constexpr auto NUMBER_OF_KEYS_TO_TEST = 5;
    constexpr auto NUMBER_OF_ROWS = 3;
    auto numberOfBitsInKey = sizeof(uint64_t) * 8;
    Experimental::H3Hash h3Hash(NUMBER_OF_ROWS, numberOfBitsInKey);

    std::vector<std::array<Experimental::H3Hash::hash_t, NUMBER_OF_ROWS>> expectedHashes = {
            {0x0,0x0,0x0},
            {0x5fe1dc66cbea3db3,0x47eb52fb9b6698bb,0x1c79d662a26e2c5},
            {0xf362035c2ef5950e,0x8aee217b46a7e1ec,0x82c055c788ba159a},
            {0xac83df3ae51fa8bd,0xcd057380ddc17957,0x8307c8a1a29cf75f},
            {0xbb63f46ac799d447,0x24139c284bd8949c,0x6adb72887c1dd13d},
            {0xe482280c0c73e9f4,0x63f8ced3d0be0c27,0x6b1cefee563b33f8}
    };

    for (auto key = 0ULL; key < NUMBER_OF_KEYS_TO_TEST; ++key) {
        for (auto row = 0UL; row < NUMBER_OF_ROWS; ++row) {
            EXPECT_EQ(expectedHashes[key][row], h3Hash(key, row));
        }
    }
}

TEST_F(H3HashTest, simpleH3testUInt32) {
    constexpr auto NUMBER_OF_KEYS_TO_TEST = 5;
    constexpr auto NUMBER_OF_ROWS = 3;
    auto numberOfBitsInKey = sizeof(uint32_t) * 8;
    Experimental::H3Hash h3Hash(NUMBER_OF_ROWS, numberOfBitsInKey);

    std::vector<std::array<Experimental::H3Hash::hash_t, NUMBER_OF_ROWS>> expectedHashes = {
            {0x0,0x0,0x0},
            {0xcbea3db3,0x3655511,0x9b6698bb},
            {0x2ef5950e,0xf1342258,0x46a7e1ec},
            {0xe51fa8bd,0xf2517749,0xddc17957},
            {0xc799d447,0x9033a80d,0x4bd8949c},
            {0xc73e9f4,0x9356fd1c,0xd0be0c27}
    };

    for (auto key = 0U; key < NUMBER_OF_KEYS_TO_TEST; ++key) {
        for (auto row = 0UL; row < NUMBER_OF_ROWS; ++row) {
            EXPECT_EQ(expectedHashes[key][row], h3Hash(key, row));
        }
    }
}

} // namespace NES