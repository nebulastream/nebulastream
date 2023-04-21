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
#include <Experimental/Benchmarking/MicroBenchmarkRun.hpp>
#include <Experimental/Benchmarking/Parsing/MicroBenchmarkSchemas.hpp>
#include <Util/Logger/LogLevel.hpp>

namespace NES::ASP::Benchmarking {
    class MicroBenchmarkRunTest : public Testing::NESBaseTest {
      public:
        /* Will be called before any test in this class are executed. */
        static void SetUpTestCase() {
            NES::Logger::setupLogging("MicroBenchmarkRunTest.log", NES::LogLevel::LOG_DEBUG);
            NES_INFO("Setup MicroBenchmarkRunTest test class.");
        }
    };

    TEST_F(MicroBenchmarkRunTest, testParseFromYAMLFileAndGetHeaderCsvAndRowsAsCsvAndToString) {
        auto yamlFile = std::filesystem::path(TEST_CONFIGS_DIRECTORY) / "some_other_example.yaml";
        auto data = "data";
        auto parsedMicroBenchmarks = MicroBenchmarkRun::parseMicroBenchmarksFromYamlFile(yamlFile, data);

        auto inputSchema = inputFileSchemas["some_input_file.csv"];
        auto inputSchemaStr = inputSchema->toString();
        auto outputSchemaStr = getOutputSchemaFromTypeAndInputSchema(AGGREGATION_TYPE::MIN, *inputSchema, "aggregation")->toString();

        std::stringstream expectedToStringStream;
        expectedToStringStream << std::endl << " - synopsis arguments: "
                                       << "type (SRSWR) "
                                       << "width (10) height (1) windowSize (1)"
                               << std::endl << " - aggregation: "
                                       << " type (" << magic_enum::enum_name(AGGREGATION_TYPE::MIN) << ") "
                                       << "fieldNameAggregation (value) "
                                       << "fieldNameAccuracy (aggregation) "
                                       << "timeStampFieldName (ts) "
                                       << "inputFile (some_input_file.csv) "
                                       << "inputSchema (" << inputSchemaStr << ") "
                                       << "outputSchema (" << outputSchemaStr << ")"
                               << std::endl << " - bufferSize :" << 1024
                               << std::endl << " - numberOfBuffers: " << 1234
                               << std::endl << " - windowSize: " << 11
                               << std::endl << " - reps: " << 234;

        EXPECT_EQ(parsedMicroBenchmarks.size(), 1);
        EXPECT_EQ(parsedMicroBenchmarks[0].getHeaderAsCsv(), "synopsis_type,synopsis_width,synopsis_height,synopsis_windowSize,"
                                                             "aggregation_type,aggregation_fieldNameAggregation,aggregation_fieldNameApproximate,aggregation_timeStampFieldName"
                                                             ",aggregation_inputFile,aggregation_inputSchema,aggregation_outputSchema"
                                                             ",bufferSize,numberOfBuffers,windowSize,reps,"
                                                             "");
        EXPECT_EQ(parsedMicroBenchmarks[0].getRowsAsCsv(), "SRSWR,10,1,1,"
                                                           "MIN,value,aggregation,ts,some_input_file.csv," +
                                                           inputSchemaStr + "," + outputSchemaStr + ",1024,1234,11,234");
        EXPECT_EQ(parsedMicroBenchmarks[0].toString(), expectedToStringStream.str());
    }

    /**
     * @brief This does not test the correctness of run, as it would not be possible to know on what system it is run and
     * therefore, the values for the throughput and accuracy. It just checkes that run() does not throw an error
     */
    TEST_F(MicroBenchmarkRunTest, testParseFromYAMLAndRun) {
        auto yamlFile = std::filesystem::path(TEST_CONFIGS_DIRECTORY) / "some_other_example.yaml";
        auto data = std::filesystem::path(TEST_CONFIGS_DIRECTORY) / "../../../nes-asp" / "data";
        auto parsedMicroBenchmarks = MicroBenchmarkRun::parseMicroBenchmarksFromYamlFile(yamlFile, data);

        auto inputSchema = inputFileSchemas["some_input_file.csv"];
        auto inputSchemaStr = inputSchema->toString();
        auto outputSchemaStr = getOutputSchemaFromTypeAndInputSchema(AGGREGATION_TYPE::MIN, *inputSchema, "aggregation")->toString();

        std::stringstream expectedToStringStream;
        expectedToStringStream << std::endl << " - synopsis arguments: "
                               << "type (SRSWR) "
                               << "width (10) height (1) windowSize (1)"
                               << std::endl << " - aggregation: "
                               << " type (" << magic_enum::enum_name(AGGREGATION_TYPE::MIN) << ") "
                               << "fieldNameAggregation (value) "
                               << "fieldNameAccuracy (aggregation) "
                               << "timeStampFieldName (ts) "
                               << "inputFile (some_input_file.csv) "
                               << "inputSchema (" << inputSchemaStr << ") "
                               << "outputSchema (" << outputSchemaStr << ")"
                               << std::endl << " - bufferSize :" << 1024
                               << std::endl << " - numberOfBuffers: " << 1234
                               << std::endl << " - windowSize: " << 11
                               << std::endl << " - reps: " << 234;

        EXPECT_EQ(parsedMicroBenchmarks.size(), 1);

        parsedMicroBenchmarks[0].run();
    }

}