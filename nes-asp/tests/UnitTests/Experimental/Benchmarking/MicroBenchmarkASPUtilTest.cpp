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
#include <Experimental/Benchmarking/MicroBenchmarkASPUtil.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <iostream>

namespace NES::ASP::Util {
    class MicroBenchmarkASPUtilTest : public Testing::BaseIntegrationTest {
      public:
        /* Will be called before any test in this class are executed. */
        static void SetUpTestCase() {
            NES::Logger::setupLogging("MicroBenchmarkASPUtilTest.log", NES::LogLevel::LOG_DEBUG);
            NES_INFO("Setup MicroBenchmarkASPUtilTest test class.");
        }

        /* Will be called before a test is executed. */
        void SetUp() override {
            Testing::BaseIntegrationTest::SetUp();
            NES_INFO("Setup MicroBenchmarkASPUtilTest test case.");
        }
    };

    TEST_F(MicroBenchmarkASPUtilTest, testParseCsvFileFromYaml) {
        auto csvFile = "csvFile.csv";
        Yaml::Node node;
        node["csvFile"] = csvFile;

        std::string fileName = std::filesystem::path(TEST_CONFIGS_DIRECTORY) / "output.yaml";
        Yaml::Serialize(node, fileName.c_str(), Yaml::SerializeConfig());

        EXPECT_EQ(parseCsvFileFromYaml(fileName), csvFile);
        EXPECT_ANY_THROW(parseCsvFileFromYaml("some_not_existing_file.yaml"));
    }

    TEST_F(MicroBenchmarkASPUtilTest, testParseAggregations) {
        auto yamlConfigFile = std::filesystem::path(TEST_CONFIGS_DIRECTORY) / "some_example.yaml";
        auto data = std::filesystem::path("some_data_folder");

        Yaml::Node configFile;
        Yaml::Parse(configFile, yamlConfigFile.c_str());
        if (configFile.IsNone()) {
            NES_THROW_RUNTIME_ERROR("Could not parse " << yamlConfigFile << "!");
        }
        auto parsedAggregations = parseAggregations(configFile["aggregation"], data);

        ASSERT_EQ(parsedAggregations.size(), 3);
        EXPECT_EQ(parsedAggregations[0].first.type, Parsing::Aggregation_Type::MIN);
        EXPECT_EQ(parsedAggregations[1].first.type, Parsing::Aggregation_Type::SUM);
        EXPECT_EQ(parsedAggregations[2].first.type, Parsing::Aggregation_Type::MAX);

        EXPECT_EQ(parsedAggregations[0].second, data / "uniform_key_value_timestamp.csv");
        EXPECT_EQ(parsedAggregations[1].second, data / "uniform_key_value_timestamp.csv");
        EXPECT_EQ(parsedAggregations[2].second, data / "uniform_key_multiple_values_timestamp.csv");

        EXPECT_EQ(parsedAggregations[0].first.fieldNameKey, "id");
        EXPECT_EQ(parsedAggregations[1].first.fieldNameKey, "id");
        EXPECT_EQ(parsedAggregations[2].first.fieldNameKey, "id");

        EXPECT_EQ(parsedAggregations[0].first.fieldNameAggregation, "value");
        EXPECT_EQ(parsedAggregations[1].first.fieldNameAggregation, "value");
        EXPECT_EQ(parsedAggregations[2].first.fieldNameAggregation, "value1");

        EXPECT_EQ(parsedAggregations[0].first.fieldNameApproximate, "aggregation");
        EXPECT_EQ(parsedAggregations[1].first.fieldNameApproximate, "aggregation");
        EXPECT_EQ(parsedAggregations[2].first.fieldNameApproximate, "aggregation");

        EXPECT_EQ(parsedAggregations[0].first.timeStampFieldName, "ts");
        EXPECT_EQ(parsedAggregations[1].first.timeStampFieldName, "ts");
        EXPECT_EQ(parsedAggregations[2].first.timeStampFieldName, "ts");
    }

    TEST_F(MicroBenchmarkASPUtilTest, testParseNoBuffers) {
        Yaml::Node aggregationNode;
        aggregationNode["windowSize"] = "1";

        auto parsedNumberOfBuffers = parseNumberOfBuffers(aggregationNode["numberOfBuffers"]);
        EXPECT_EQ(parsedNumberOfBuffers.size(), 1);
        EXPECT_EQ(parsedNumberOfBuffers[0], 1024);

        parsedNumberOfBuffers = parseNumberOfBuffers(aggregationNode["numberOfBuffers"], 42);
        EXPECT_EQ(parsedNumberOfBuffers.size(), 1);
        EXPECT_EQ(parsedNumberOfBuffers[0], 42);
    }


    TEST_F(MicroBenchmarkASPUtilTest, testParseWindowSizesNoBuffersBufferSizesRepsFromFile) {
        auto yamlFile = std::filesystem::path(TEST_CONFIGS_DIRECTORY) / "some_example.yaml";

        Yaml::Node aggregationNode;
        Yaml::Parse(aggregationNode, yamlFile.c_str());

        auto parsedWindowSizes = parseWindowSizes(aggregationNode["windowSize"]);
        auto parsedNumberOfBuffers = parseNumberOfBuffers(aggregationNode["numberOfBuffers"]);
        auto parsedBufferSizes = parseBufferSizes(aggregationNode["bufferSize"]);
        auto parsedReps = parseReps(aggregationNode["reps"]);


        EXPECT_EQ(parsedReps, 2);
        EXPECT_EQ(parsedWindowSizes.size(), 3);
        EXPECT_EQ(parsedNumberOfBuffers.size(), 2);
        EXPECT_EQ(parsedBufferSizes.size(), 1);

        EXPECT_EQ(parsedWindowSizes[0], 11);
        EXPECT_EQ(parsedWindowSizes[1], 42);
        EXPECT_EQ(parsedWindowSizes[2], 1234);

        EXPECT_EQ(parsedNumberOfBuffers[0], 10240);
        EXPECT_EQ(parsedNumberOfBuffers[1], 1234);

        EXPECT_EQ(parsedBufferSizes[0], 1024);
    }

} // namespace NES::ASP::Util
