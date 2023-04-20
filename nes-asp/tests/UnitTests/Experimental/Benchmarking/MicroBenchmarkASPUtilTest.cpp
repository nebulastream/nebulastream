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

#include <NesBaseTest.hpp>
#include <Experimental/Benchmarking/MicroBenchmarkASPUtil.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <iostream>

namespace NES::ASP::Util {
    class MicroBenchmarkASPUtilTest : public Testing::NESBaseTest {
      public:
        /* Will be called before any test in this class are executed. */
        static void SetUpTestCase() {
            NES::Logger::setupLogging("MicroBenchmarkASPUtilTest.log", NES::LogLevel::LOG_DEBUG);
            NES_INFO("Setup MicroBenchmarkASPUtilTest test class.");
        }

        /* Will be called before a test is executed. */
        void SetUp() override {
            Testing::NESBaseTest::SetUp();
            NES_INFO("Setup MicroBenchmarkASPUtilTest test case.");
        }
    };

    TEST_F(MicroBenchmarkASPUtilTest, testParseCsvFileFromYaml) {
        auto csvFile = "csvFile.csv";
        Yaml::Node node;
        node["csvFile"] = csvFile;

        std::string fileName = "output.yaml";
        Yaml::Serialize(node, fileName, Yaml::SerializeConfig());

        EXPECT_EQ(parseCsvFileFromYaml(fileName), csvFile);
        EXPECT_ANY_THROW(parseCsvFileFromYaml("some_not_existing_file.yaml"));
    }

    TEST_F(MicroBenchmarkASPUtilTest, testParseSynopsisConfigurations) {
        auto yamlFile = std::filesystem::path(TEST_CONFIGS_DIRECTORY) / "some_example.yaml";
        auto data = "some_data_folder";

        Yaml::Node aggregationNode;
        Yaml::Parse(aggregationNode, yamlFile);
        auto parsedAggregations = parseAggregations(aggregationNode, data);

        ASSERT_EQ(parsedAggregations.size(), 3);
        EXPECT_EQ(parsedAggregations[0].type, Benchmarking::AGGREGATION_TYPE::MIN);
        EXPECT_EQ(parsedAggregations[1].type, Benchmarking::AGGREGATION_TYPE::SUM);
        EXPECT_EQ(parsedAggregations[2].type, Benchmarking::AGGREGATION_TYPE::MAX);

        EXPECT_EQ(parsedAggregations[0].inputFile, "some_input_file.csv");
        EXPECT_EQ(parsedAggregations[1].inputFile, "some_input_file_2.csv");
        EXPECT_EQ(parsedAggregations[2].inputFile, "some_other_input_file.csv");

        EXPECT_EQ(parsedAggregations[0].fieldNameAggregation, "value");
        EXPECT_EQ(parsedAggregations[1].fieldNameAggregation, "value");
        EXPECT_EQ(parsedAggregations[2].fieldNameAggregation, "value1");

        EXPECT_EQ(parsedAggregations[0].fieldNameApproximate, "aggregation");
        EXPECT_EQ(parsedAggregations[1].fieldNameApproximate, "aggregation421");
        EXPECT_EQ(parsedAggregations[2].fieldNameApproximate, "aggregation123");

        EXPECT_EQ(parsedAggregations[0].timeStampFieldName, "ts");
        EXPECT_EQ(parsedAggregations[1].timeStampFieldName, "ts123");
        EXPECT_EQ(parsedAggregations[2].timeStampFieldName, "ts451");
    }

    TEST_F(MicroBenchmarkASPUtilTest, testParseNoBuffersBuffer) {
        Yaml::Node aggregationNode;
        aggregationNode["windowSize"] = "1";

        auto parsedNumberOfBuffers = parseNumberOfBuffers(aggregationNode);
        EXPECT_EQ(parsedNumberOfBuffers.size(), 1);
        EXPECT_EQ(parsedNumberOfBuffers[0], 1024);

        parsedNumberOfBuffers = parseNumberOfBuffers(aggregationNode, 42);
        EXPECT_EQ(parsedNumberOfBuffers.size(), 1);
        EXPECT_EQ(parsedNumberOfBuffers[0], 42);
    }


    TEST_F(MicroBenchmarkASPUtilTest, testParseWindowSizesNoBuffersBufferSizesRepsFromFile) {
        auto yamlFile = std::filesystem::path(TEST_CONFIGS_DIRECTORY) / "some_example.yaml";

        Yaml::Node aggregationNode;
        Yaml::Parse(aggregationNode, yamlFile);

        auto parsedWindowSizes = parseWindowSizes(aggregationNode);
        auto parsedNumberOfBuffers = parseNumberOfBuffers(aggregationNode);
        auto parsedBufferSizes = parseBufferSizes(aggregationNode);
        auto parsedReps = parseReps(aggregationNode);


        EXPECT_EQ(parsedReps, 2);
        EXPECT_EQ(parsedWindowSizes.size(), 3);
        EXPECT_EQ(parsedNumberOfBuffers.size(), 2);
        EXPECT_EQ(parsedBufferSizes.size(), 1);

        EXPECT_EQ(parsedWindowSizes[0], 11);
        EXPECT_EQ(parsedWindowSizes[1], 42);
        EXPECT_EQ(parsedWindowSizes[2], 1234);

        EXPECT_EQ(parsedNumberOfBuffers[0], 10240);
        EXPECT_EQ(parsedNumberOfBuffers[1], 1234);

        EXPECT_EQ(parsedBufferSizes[2], 1024);
    }
    }
