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
#include <Nautilus/Interface/DataTypes/Integer/Int.hpp>
#include <Nautilus/Interface/DataTypes/Value.hpp>
#include <Nautilus/Interface/Hash/HashFunction.hpp>
#include <Nautilus/Interface/Hash/MurMur3HashFunction.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/StdInt.hpp>
#include <gtest/gtest.h>
#include <memory>
namespace NES::Nautilus::Interface {

class HashTest : public Testing::BaseUnitTest {
  public:
    std::unique_ptr<HashFunction> hf;
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("HashTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup HashTest test class.");
    }
    void SetUp() override {
        Testing::BaseUnitTest::SetUp();
        hf = std::make_unique<MurMur3HashFunction>();
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_INFO("Tear down HashTest test class."); }
};

TEST_F(HashTest, IntHashTest) {
    auto f1 = Value<Int8>(42_s8);
    auto res = hf->calculate(f1);
    ASSERT_EQ(res, 9297814887077134198_u64);
}

}// namespace NES::Nautilus::Interface