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

#include <Execution/Aggregation/AggregationValue.hpp>
#include <Execution/Aggregation/AvgAggregation.hpp>
#include <Execution/Aggregation/CountAggregation.hpp>
#include <Execution/Aggregation/MaxAggregation.hpp>
#include <Execution/Aggregation/MinAggregation.hpp>
#include <Execution/Aggregation/SumAggregation.hpp>
#include <Experimental/Benchmarking/MicroBenchmarkSchemas.hpp>
#include <Experimental/Parsing/SynopsisAggregationConfig.hpp>
#include <NesBaseTest.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/magicenum/magic_enum.hpp>
#include <Util/yaml/Yaml.hpp>
#include <memory>

namespace NES::ASP::Parsing {
    class SynopsisAggregationConfigTest : public Testing::NESBaseTest {
      public:
        /* Will be called before any test in this class are executed. */
        static void SetUpTestCase() {
            NES::Logger::setupLogging("SynopsisAggregationConfigTest.log", NES::LogLevel::LOG_DEBUG);
            NES_INFO("Setup SynopsisAggregationConfigTest test class.");
        }

        /* Will be called before a test is executed. */
        void SetUp() override {
            Testing::NESBaseTest::SetUp();
            NES_INFO("Setup SynopsisAggregationConfigTest test case.");

            Yaml::Node aggregationNode;
            aggregationNode["type"] = std::string(magic_enum::enum_name(type));
            aggregationNode["fieldNameAgg"] = fieldNameAggregation;
            aggregationNode["fieldNameApprox"] = fieldNameApprox;
            aggregationNode["inputFile"] = inputFile;
            aggregationNode["timestamp"] = timeStampFieldName;

            synopsisAggregationConfig = SynopsisAggregationConfig::createAggregationFromYamlNode(aggregationNode, data);
        }

        AGGREGATION_TYPE type = AGGREGATION_TYPE::MAX;
        std::string fieldNameAggregation = "fieldName123";
        std::string fieldNameApprox = "fieldNameApprox123";
        std::string inputFile = "some_input_file.csv";
        std::filesystem::path data = std::filesystem::path("some_folder") / "test" / "test123";
        std::string timeStampFieldName = "timeStampFieldName1234123";
        synopsisAggregationConfig yamlAggregation;
    };

    TEST_F(SynopsisAggregationConfigTest, testCreateAggregation) {
        /* Creating values */
        auto numberOfTypes = magic_enum::enum_values<AGGREGATION_TYPE>().size();
        auto type = magic_enum::enum_cast<AGGREGATION_TYPE>(rand() % numberOfTypes).value();
        auto fieldNameAggregation = "fieldName123";
        auto fieldNameApprox = "fieldNameApprox123";
        auto inputFile = "some_input_file.csv";
        auto data = std::filesystem::path("some_folder") / "test" / "test123";
        auto timeStampFieldName = "timeStampFieldName1234123";

        auto inputSchema = inputFileSchemas[inputFile];
        auto outputSchema = getOutputSchemaFromTypeAndInputSchema(type, *inputSchema, fieldNameAggregation);


        Yaml::Node aggregationNode;
        aggregationNode["type"] = std::string(magic_enum::enum_name(type));
        aggregationNode["fieldNameAgg"] = fieldNameAggregation;
        aggregationNode["fieldNameApprox"] = fieldNameApprox;
        aggregationNode["inputFile"] = inputFile;
        aggregationNode["timestamp"] = timeStampFieldName;

        auto yamlAggregation = SynopsisAggregationConfig::createAggregationFromYamlNode(aggregationNode, data);
        EXPECT_EQ(yamlAggregation.type, type);
        EXPECT_EQ(yamlAggregation.fieldNameAggregation, fieldNameAggregation);
        EXPECT_EQ(yamlAggregation.fieldNameApproximate, fieldNameApprox);
        EXPECT_EQ(yamlAggregation.timeStampFieldName, timeStampFieldName);
        EXPECT_EQ(yamlAggregation.inputFile, data / inputFile);
        EXPECT_TRUE(yamlAggregation.inputSchema->equals(inputSchema));
        EXPECT_TRUE(yamlAggregation.outputSchema->equals(outputSchema));
    }

    TEST_F(SynopsisAggregationConfigTest, testgetHeaderCsvAndValuesCsvAndToString) {
        auto inputSchema = inputFileSchemas[inputFile];
        auto inputSchemaStr = inputFileSchemas[inputFile]->toString();
        auto outputSchemaStr = getOutputSchemaFromTypeAndInputSchema(type, *inputSchema, fieldNameAggregation)->toString();

        auto headerCsv = yamlAggregation.getHeaderAsCsv();
        auto valuesCsv = yamlAggregation.getValuesAsCsv();
        auto toString = yamlAggregation.toString();

        EXPECT_EQ(headerCsv, "aggregation_type,aggregation_fieldNameAggregation,aggregation_fieldNameApproximate,"
                             "aggregation_timeStampFieldName,aggregation_inputFile,aggregation_inputSchema,"
                             "aggregation_outputSchema");
        EXPECT_EQ(valuesCsv, "COUNT,fieldName123,fieldNameApprox123,timeStampFieldName1234123," + (data/inputFile).string() +
                             "," + inputSchemaStr + "," + outputSchemaStr);
        EXPECT_EQ(toString, "type (MAX) fieldNameAggregation (fieldName123) fieldNameAccuracy (fieldNameApprox123) "
                            "timeStampFieldName (timeStampFieldName1234123) inputFile (" + (data/inputFile).string() + ") " +
                            "inputSchema (" + inputSchemaStr + ") outputSchema (" + outputSchemaStr + ")");
    }

    TEST_F(SynopsisAggregationConfigTest, testCreateAggregationFunction) {

        for (auto& type : magic_enum::enum_values<AGGREGATION_TYPE>()) {
            yamlAggregation.type = type;
            auto aggregationFunction = yamlAggregation.createAggregationFunction();

            switch (yamlAggregation.type) {
                case AGGREGATION_TYPE::MIN:
                    EXPECT_NE(std::dynamic_pointer_cast<Runtime::Execution::Aggregation::MinAggregationFunction>(aggregationFunction), nullptr);
                case AGGREGATION_TYPE::MAX:
                    EXPECT_NE(std::dynamic_pointer_cast<Runtime::Execution::Aggregation::MaxAggregationFunction>(aggregationFunction), nullptr);
                case AGGREGATION_TYPE::SUM:
                    EXPECT_NE(std::dynamic_pointer_cast<Runtime::Execution::Aggregation::SumAggregationFunction>(aggregationFunction), nullptr);
                case AGGREGATION_TYPE::AVERAGE:
                    EXPECT_NE(std::dynamic_pointer_cast<Runtime::Execution::Aggregation::AvgAggregationFunction>(aggregationFunction), nullptr);
                case AGGREGATION_TYPE::COUNT:
                    EXPECT_NE(std::dynamic_pointer_cast<Runtime::Execution::Aggregation::CountAggregationFunction>(aggregationFunction), nullptr);
                case AGGREGATION_TYPE::NONE: ASSERT_TRUE(true);
            }
        }
    }

    TEST_F(SynopsisAggregationConfigTest, testCreateAggregationValue) {
        for (auto& type : magic_enum::enum_values<AGGREGATION_TYPE>()) {
            switch (yamlAggregation.type) {
                case AGGREGATION_TYPE::MIN: {
                    yamlAggregation.type = type;
                    auto aggregationValue = yamlAggregation.createAggregationValue();
                    EXPECT_NE(static_cast<Runtime::Execution::Aggregation::MinAggregationValue<int64_t>*>(aggregationValue.get()), nullptr);
                }
                case AGGREGATION_TYPE::MAX: {
                    yamlAggregation.type = type;
                    auto aggregationValue = yamlAggregation.createAggregationValue();
                    EXPECT_NE(static_cast<Runtime::Execution::Aggregation::MaxAggregationValue<int64_t>*>(aggregationValue.get()), nullptr);
                }
                case AGGREGATION_TYPE::SUM: {
                    yamlAggregation.type = type;
                    auto aggregationValue = yamlAggregation.createAggregationValue();
                    EXPECT_NE(static_cast<Runtime::Execution::Aggregation::SumAggregationValue<int64_t>*>(aggregationValue.get()), nullptr);
                }
                case AGGREGATION_TYPE::AVERAGE: {
                    yamlAggregation.type = type;
                    auto aggregationValue = yamlAggregation.createAggregationValue();
                    EXPECT_NE(static_cast<Runtime::Execution::Aggregation::AvgAggregationValue<int64_t>*>(aggregationValue.get()), nullptr);
                }
                case AGGREGATION_TYPE::COUNT: {
                    yamlAggregation.type = type;
                    auto aggregationValue = yamlAggregation.createAggregationValue();
                    EXPECT_NE(static_cast<Runtime::Execution::Aggregation::CountAggregationValue<int64_t>*>(aggregationValue.get()), nullptr);
                }
                case AGGREGATION_TYPE::NONE: EXPECT_ANY_THROW(yamlAggregation.createAggregationValue());
            }
        }
    }
}