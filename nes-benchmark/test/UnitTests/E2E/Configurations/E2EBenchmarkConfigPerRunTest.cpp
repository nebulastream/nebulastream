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

//- test für jede option, ob diese richtig geparsed wurde
//- ein großer test oder mehrere kleine...
//- wahrscheinlich mehrere, da so auch default werte geprüft werden können

#include <gtest/gtest.h>
#include <Util/Logger/Logger.hpp>

namespace NES::Benchmark {
    class E2EBenchmarkConfigPerRunTest : public testing::Test {
      public:
        /* Will be called before any test in this class are executed. */
        static void SetUpTestCase() {
            NES::Logger::setupLogging("E2EBenchmarkConfigPerRunTest.log", NES::LogLevel::LOG_DEBUG);
            NES_INFO("Setup E2EBenchmarkConfigPerRunTest test class.");
        }

        /* Will be called before a test is executed. */
        void SetUp() override { NES_INFO("Setup E2EBenchmarkConfigPerRunTest test case."); }

        /* Will be called before a test is executed. */
        void TearDown() override { NES_INFO("Tear down E2EBenchmarkConfigPerRunTest test case."); }

        /* Will be called after all tests in this class are finished. */
        static void TearDownTestCase() { NES_INFO("Tear down E2EBenchmarkConfigPerRunTest test class."); }
    };

    TEST_F(E2EBenchmarkConfigPerRunTest, toStringTest) {

    }

    TEST_F(E2EBenchmarkConfigPerRunTest, generateAllConfigsPerRunTest) {

    }

    TEST_F(E2EBenchmarkConfigPerRunTest, getStrLogicalSrcToNumberOfPhysicalSrcTest) {

    }

    TEST_F(E2EBenchmarkConfigPerRunTest, generateMapsLogicalSrcPhysicalSourcesTest) {

    }
}//namespace NES::Benchmark