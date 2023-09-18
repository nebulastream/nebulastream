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
#include <Execution/Aggregation/AggregationValue.hpp>
#include <Execution/Aggregation/AvgAggregation.hpp>
#include <Execution/Aggregation/CountAggregation.hpp>
#include <Execution/Aggregation/MaxAggregation.hpp>
#include <Execution/Aggregation/MinAggregation.hpp>
#include <Execution/Aggregation/SumAggregation.hpp>
#include <Experimental/Benchmarking/MicroBenchmarkSchemas.hpp>
#include <Experimental/Parsing/SynopsisAggregationConfig.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/magicenum/magic_enum.hpp>
#include <Util/yaml/Yaml.hpp>
#include <memory>

namespace NES::ASP::Parsing {
    class SynopsisAggregationConfigTest : public Testing::BaseIntegrationTest {
      public:
        /* Will be called before any test in this class are executed. */
        static void SetUpTestCase() {
            NES::Logger::setupLogging("SynopsisAggregationConfigTest.log", NES::LogLevel::LOG_DEBUG);
            NES_INFO("Setup SynopsisAggregationConfigTest test class.");
        }

        /* Will be called before a test is executed. */
        void SetUp() override {
            Testing::BaseIntegrationTest::SetUp();
            NES_INFO("Setup SynopsisAggregationConfigTest test case.");

            Yaml::Node aggregationNode;
            aggregationNode["type"] = std::string(magic_enum::enum_name(type));
            aggregationNode["fieldNameKey"] = fieldNameKey;
            aggregationNode["fieldNameAgg"] = fieldNameAggregation;
            aggregationNode["fieldNameApprox"] = fieldNameApprox;
            aggregationNode["inputFile"] = inputFile;
            aggregationNode["timestamp"] = timeStampFieldName;

            auto synopsisConfig = SynopsisAggregationConfig::createAggregationFromYamlNode(aggregationNode, data);
            synopsisAggregationConfig = synopsisConfig.first;
        }

        const Aggregation_Type type = Aggregation_Type::MAX;
        const std::string fieldNameKey = "id";
        const std::string fieldNameAggregation = "value";
        const std::string fieldNameApprox = "aggregation";
        const std::string inputFile = "uniform_key_value_timestamp.csv";
        const std::filesystem::path data = std::filesystem::path("some_folder") / "test" / "test123";
        const std::string timeStampFieldName = "ts";
        Parsing::SynopsisAggregationConfig synopsisAggregationConfig;
    };

    TEST_F(SynopsisAggregationConfigTest, testCreateAggregationFromYamlNode) {
        /* Creating values */
        auto numberOfTypes = magic_enum::enum_values<Aggregation_Type>().size();
        auto type = magic_enum::enum_cast<Aggregation_Type>(rand() % numberOfTypes).value();

        auto inputSchema = Benchmarking::inputFileSchemas[inputFile];
        auto outputSchema = Benchmarking::getOutputSchemaFromTypeAndInputSchema(type, *inputSchema,
                                                                                fieldNameKey, fieldNameAggregation,
                                                                                fieldNameKey, fieldNameApprox);


        Yaml::Node aggregationNode;
        aggregationNode["type"] = std::string(magic_enum::enum_name(type));
        aggregationNode["fieldNameKey"] = fieldNameKey;
        aggregationNode["fieldNameAgg"] = fieldNameAggregation;
        aggregationNode["fieldNameApprox"] = fieldNameApprox;
        aggregationNode["inputFile"] = inputFile;
        aggregationNode["timestamp"] = timeStampFieldName;

        auto synopsisAggregation = SynopsisAggregationConfig::createAggregationFromYamlNode(aggregationNode, data);
        EXPECT_EQ(synopsisAggregation.first.type, type);
        EXPECT_EQ(synopsisAggregation.first.fieldNameKey, fieldNameKey);
        EXPECT_EQ(synopsisAggregation.first.fieldNameAggregation, fieldNameAggregation);
        EXPECT_EQ(synopsisAggregation.first.fieldNameApproximate, fieldNameApprox);
        EXPECT_EQ(synopsisAggregation.first.timeStampFieldName, timeStampFieldName);
        EXPECT_EQ(synopsisAggregation.second, data / inputFile);
        EXPECT_TRUE(synopsisAggregation.first.inputSchema->equals(inputSchema));
        EXPECT_TRUE(synopsisAggregation.first.outputSchema->equals(outputSchema));
    }

    TEST_F(SynopsisAggregationConfigTest, testgetHeaderCsvAndValuesCsvAndToString) {
        auto inputSchema = Benchmarking::inputFileSchemas[inputFile];
        auto inputSchemaStr = Benchmarking::inputFileSchemas[inputFile]->toString();
        auto outputSchemaStr = Benchmarking::getOutputSchemaFromTypeAndInputSchema(type, *inputSchema,
                                                                                   fieldNameKey, fieldNameAggregation,
                                                                                   fieldNameKey, fieldNameApprox)->toString();

        auto headerCsv = synopsisAggregationConfig.getHeaderAsCsv();
        auto valuesCsv = synopsisAggregationConfig.getValuesAsCsv();
        auto toString = synopsisAggregationConfig.toString();

        EXPECT_EQ(headerCsv, "aggregation_type,aggregation_fieldNameKey,aggregation_fieldNameAggregation,"
                             "aggregation_fieldNameApproximate,aggregation_timeStampFieldName,aggregation_inputSchema,"
                             "aggregation_outputSchema");
        EXPECT_EQ(valuesCsv, "MAX,id,value,aggregation,ts," + inputSchemaStr + "," + outputSchemaStr);
        EXPECT_EQ(toString, "type (MAX) fieldNameKey (id) fieldNameAggregation (value) fieldNameAccuracy (aggregation) "
                            "timeStampFieldName (ts) inputSchema (" + inputSchemaStr + ") outputSchema (" + outputSchemaStr + ")");
    }

    TEST_F(SynopsisAggregationConfigTest, testCreateAggregationFunction) {

        for (auto& type : magic_enum::enum_values<Aggregation_Type>()) {
            if (type == Aggregation_Type::NONE) {
                // We do not do anything for NONE
                continue;
            }

            synopsisAggregationConfig.type = type;
            auto aggregationFunction = synopsisAggregationConfig.createAggregationFunction();

            switch (synopsisAggregationConfig.type) {
                case Aggregation_Type::MIN:
                    EXPECT_NE(std::dynamic_pointer_cast<Runtime::Execution::Aggregation::MinAggregationFunction>(aggregationFunction), nullptr);
                    break;
                case Aggregation_Type::MAX:
                    EXPECT_NE(std::dynamic_pointer_cast<Runtime::Execution::Aggregation::MaxAggregationFunction>(aggregationFunction), nullptr);
                    break;
                case Aggregation_Type::SUM:
                    EXPECT_NE(std::dynamic_pointer_cast<Runtime::Execution::Aggregation::SumAggregationFunction>(aggregationFunction), nullptr);
                    break;
                case Aggregation_Type::AVERAGE:
                    EXPECT_NE(std::dynamic_pointer_cast<Runtime::Execution::Aggregation::AvgAggregationFunction>(aggregationFunction), nullptr);
                    break;
                case Aggregation_Type::COUNT:
                    EXPECT_NE(std::dynamic_pointer_cast<Runtime::Execution::Aggregation::CountAggregationFunction>(aggregationFunction), nullptr);
                    break;
                case Aggregation_Type::NONE: ASSERT_TRUE(false);
            }
        }
    }
} // namespace NES::ASP:Parsing