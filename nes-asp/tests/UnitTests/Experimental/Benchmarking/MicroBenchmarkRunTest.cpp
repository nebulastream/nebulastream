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
#include <Experimental/Benchmarking/MicroBenchmarkRun.hpp>
#include <Experimental/Benchmarking/MicroBenchmarkSchemas.hpp>
#include <Util/Logger/LogLevel.hpp>

namespace NES::ASP::Benchmarking {
    class MicroBenchmarkRunTest : public Testing::BaseIntegrationTest {
      public:
        /* Will be called before any test in this class are executed. */
        static void SetUpTestCase() {
            NES::Logger::setupLogging("MicroBenchmarkRunTest.log", NES::LogLevel::LOG_DEBUG);
            NES_INFO("Setup MicroBenchmarkRunTest test class.");
        }
    };

    /**
     * @brief Tests the methods parseMicroBenchmarksFromYamlFile(), getHeaderAsCsv(), getRowsAsCsv(), and toString()
     */
    TEST_F(MicroBenchmarkRunTest, testParseFromYAMLFileAndGetHeaderCsvAndRowsAsCsvAndToString) {
        auto yamlFile = std::filesystem::path(TEST_CONFIGS_DIRECTORY) / "some_other_example.yaml";
        auto data = "data";
        auto parsedMicroBenchmarks = MicroBenchmarkRun::parseMicroBenchmarksFromYamlFile(yamlFile, data);

        auto inputSchema = inputFileSchemas["uniform_key_value_timestamp.csv"];
        auto inputSchemaStr = inputSchema->toString();
        auto outputSchemaStr = getOutputSchemaFromTypeAndInputSchema(Parsing::Aggregation_Type::MIN, *inputSchema,
                                                                     "id", "value",
                                                                     "id", "aggregation")->toString();

        std::stringstream expectedToStringStream;
        expectedToStringStream << std::endl << " - synopsis arguments: "
                                   << "type (SRSWoR) width (10) height (1) windowSize (1)"
                               << std::endl << " - aggregation: "
                                   << "type (MIN) "
                                   << "fieldNameKey (id) "
                                   << "fieldNameAggregation (value) "
                                   << "fieldNameAccuracy (aggregation) "
                                   << "timeStampFieldName (ts) "
                                   << "inputSchema (" << inputSchemaStr << ") "
                                   << "outputSchema (" << outputSchemaStr << ")"
                               << std::endl << " - bufferSize : " << 1024
                               << std::endl << " - numberOfBuffers: " << 1234
                               << std::endl << " - windowSize: " << 11
                               << std::endl << " - inputFile: data/uniform_key_value_timestamp.csv"
                               << std::endl << " - reps: " << 234;

        EXPECT_EQ(parsedMicroBenchmarks.size(), 1);
        EXPECT_EQ(parsedMicroBenchmarks[0].getRowsAsCsv(), "");
        EXPECT_EQ(parsedMicroBenchmarks[0].getHeaderAsCsv(), "synopsis_type,synopsis_width,synopsis_height,synopsis_windowSize,"
                                                             "aggregation_type,aggregation_fieldNameKey,"
                                                             "aggregation_fieldNameAggregation,aggregation_fieldNameApproximate,"
                                                             "aggregation_timeStampFieldName,aggregation_inputSchema,"
                                                             "aggregation_outputSchema,bufferSize,numberOfBuffers,"
                                                             "windowSize,inputFile,reps");
    EXPECT_EQ(parsedMicroBenchmarks[0].toString(), expectedToStringStream.str());
    }

    /**
     * @brief This does not test the correctness of run, as it would not be possible to know on what system it is run and
     * therefore, the values for the throughput and accuracy. It just checks that run() does not throw an error
     */
    TEST_F(MicroBenchmarkRunTest, testParseFromYAMLAndRun) {
        auto yamlFile = std::filesystem::path(TEST_CONFIGS_DIRECTORY) / "some_other_example.yaml";
        auto data = std::filesystem::path(TEST_CONFIGS_DIRECTORY) / "../../../../nes-asp" / "data";
        auto parsedMicroBenchmarks = MicroBenchmarkRun::parseMicroBenchmarksFromYamlFile(yamlFile, data);

        EXPECT_EQ(parsedMicroBenchmarks.size(), 1);
        EXPECT_NO_THROW(parsedMicroBenchmarks[0].run());
    }

}