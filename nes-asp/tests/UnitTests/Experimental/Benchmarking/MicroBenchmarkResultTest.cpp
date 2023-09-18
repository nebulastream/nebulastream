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
#include <Experimental/Benchmarking/MicroBenchmarkResult.hpp>
#include <Util/Logger/LogLevel.hpp>

namespace NES::ASP::Benchmarking {
    class MicroBenchmarkResultTest : public Testing::BaseIntegrationTest {
      public:
        /* Will be called before any test in this class are executed. */
        static void SetUpTestCase() {
            NES::Logger::setupLogging("MicroBenchmarkResultTest.log", NES::LogLevel::LOG_DEBUG);
            NES_INFO("Setup MicroBenchmarkResultTest test class.");
        }
    };

    TEST_F(MicroBenchmarkResultTest, testAddToParams) {
        auto throughput = 12.5;
        auto accuracy = 42.99;

        MicroBenchmarkResult microBenchmarkResult(throughput, accuracy);
        EXPECT_EQ(microBenchmarkResult.getParam("throughput"), std::to_string(throughput));
        EXPECT_EQ(microBenchmarkResult.getParam("accuracy"), std::to_string(accuracy));

        microBenchmarkResult.addToParams("some_key", "some_value");
        microBenchmarkResult.addToParams("some_key123", "some_value123");
        microBenchmarkResult.addToParams("some_key456", "some_value456");

        EXPECT_EQ(microBenchmarkResult.getParam("some_key"), "some_value");
        EXPECT_EQ(microBenchmarkResult.getParam("some_key123"), "some_value123");
        EXPECT_EQ(microBenchmarkResult.getParam("some_key456"), "some_value456");
    }

    TEST_F(MicroBenchmarkResultTest, testgetHeaderCsvAndGetRowCsv) {
        auto throughput = 12.5;
        auto accuracy = 42.99;

        MicroBenchmarkResult microBenchmarkResult(throughput, accuracy);
        microBenchmarkResult.addToParams("some_key", "some_value");
        microBenchmarkResult.addToParams("some_key123", "some_value123");
        microBenchmarkResult.addToParams("some_key456", "some_value456");

        auto headerCsv = microBenchmarkResult.getHeaderAsCsv();
        auto rowCsv = microBenchmarkResult.getRowAsCsv();

        EXPECT_EQ(headerCsv, "accuracy,some_key,some_key123,some_key456,throughput");
        EXPECT_EQ(rowCsv, std::to_string(accuracy) + ",some_value,some_value123,some_value456," + std::to_string(throughput));
    }
} // namespace NES::ASP::Benchmarking