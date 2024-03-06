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

#include <API/Schema.hpp>
#include <BaseIntegrationTest.hpp>
#include <Execution/Expressions/ReadFieldExpression.hpp>
#include <Execution/Operators/ExecutionContext.hpp>
#include <Execution/Operators/Statistics/CountMinBuildOperator.hpp>
#include <Execution/Operators/Statistics/CountMinOperatorHandler.hpp>
#include <Execution/Operators/Streaming/MultiOriginWatermarkProcessor.hpp>
#include <Execution/Operators/Streaming/TimeFunction.hpp>
#include <Nautilus/Interface/DataTypes/MemRef.hpp>
#include <Nautilus/Interface/DataTypes/Text/Text.hpp>
#include <Nautilus/Interface/Hash/H3Hash.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/MemoryLayout/DynamicTupleBuffer.hpp>
#include <Runtime/WorkerContext.hpp>
#include <StatisticFieldIdentifiers.hpp>
#include <Statistics/Interval.hpp>
#include <TestUtils/MockedPipelineExecutionContext.hpp>
#include <TestUtils/RecordCollectOperator.hpp>
#include <Util/Logger/Logger.hpp>
#include <fstream>
#include <gtest/gtest.h>

namespace NES::Runtime::Execution::Operators {

using TimeFunctionPtr = std::unique_ptr<TimeFunction>;

class SynopsisOperatorTest : public Testing::BaseUnitTest {
  public:
    /* Will be called before any test in this class is executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("SynopsisOperatorTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup SynopsisOperatorTest test class.");
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_INFO("Tear down SynopsisOperatorTest test class."); }
};

std::vector<uint64_t> parseCsvLine(const std::string& line) {
    std::vector<uint64_t> result;

    std::istringstream iss(line);
    std::string token;

    // Tokenize the line by comma
    while (std::getline(iss, token, ',')) {
        // Convert each token to uint64_t and add to the vector
        try {
            uint64_t value = std::stoull(token);
            result.push_back(value);
        } catch (const std::invalid_argument& e) {
            std::cerr << "Invalid argument: " << e.what() << std::endl;
        } catch (const std::out_of_range& e) {
            std::cerr << "Out of range: " << e.what() << std::endl;
        }
    }

    return result;
}

std::vector<uint64_t> readCsvFile(const std::string& filename) {
    std::vector<uint64_t> csvData;

    // Open the CSV file
    std::ifstream file(filename);

    // Check if the file is opened successfully
    if (!file.is_open()) {
        std::cerr << "Could not open file: " << filename << std::endl;
        return csvData;// Return an empty vector
    }

    std::string line;
    while (std::getline(file, line)) {
        // Parse the CSV line into a vector of uint64_t values
        std::vector<uint64_t> lineData = parseCsvLine(line);

        // Append the values to the main vector
        csvData.insert(csvData.end(), lineData.begin(), lineData.end());
    }

    // Close the file
    file.close();

    return csvData;
}

/**
 * @brief Tests if we limit the number of tuples
 */
TEST_F(SynopsisOperatorTest, CountMinOperatorTest) {
    std::string DEFAULT_LOGICAL_SOURCE_NAME = "defaultLogicalSourceName";
    std::string DEFAULT_PHYSICAL_SOURCE_NAME = "defaultPhysicalSourceName";
    WorkerId WORKER_ID = 0;
    const uint64_t DEPTH = 3;
    uint64_t WIDTH = 8;
    uint64_t WINDOW_SIZE = 5000;
    uint64_t SLIDE_FACTOR = 5000;
    std::string ON_FIELD = DEFAULT_LOGICAL_SOURCE_NAME + "$" + "f1";
    const std::string TS = DEFAULT_LOGICAL_SOURCE_NAME + "$" + "ts";
    uint64_t OPERATOR_HANDLER_INDEX = 0;
    uint64_t KEY_SIZE_IN_BITS = sizeof(uint64_t) * 8;
    uint64_t TUPLES = 5;

    auto bm = std::make_shared<Runtime::BufferManager>();
    auto wc = std::make_shared<WorkerContext>(WORKER_ID, bm, 100);

    // FIELD_NAME --> the field which repeats the name from on which we build the statistic
    // ON_FIELD --> the field from which we read the data to build the statistic
    auto outputSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                            ->addField( DEFAULT_LOGICAL_SOURCE_NAME + "$" + NES::Experimental::Statistics::LOGICAL_SOURCE_NAME, BasicType::TEXT)
                            ->addField( DEFAULT_LOGICAL_SOURCE_NAME + "$" + NES::Experimental::Statistics::PHYSICAL_SOURCE_NAME, BasicType::TEXT)
                            ->addField( DEFAULT_LOGICAL_SOURCE_NAME + "$" + NES::Experimental::Statistics::FIELD_NAME, BasicType::TEXT)
                            ->addField( DEFAULT_LOGICAL_SOURCE_NAME + "$" + NES::Experimental::Statistics::OBSERVED_TUPLES, BasicType::UINT64)
                            ->addField( DEFAULT_LOGICAL_SOURCE_NAME + "$" + NES::Experimental::Statistics::DEPTH, BasicType::UINT64)
                            ->addField( DEFAULT_LOGICAL_SOURCE_NAME + "$" + NES::Experimental::Statistics::START_TIME, BasicType::UINT64)
                            ->addField( DEFAULT_LOGICAL_SOURCE_NAME + "$" + NES::Experimental::Statistics::END_TIME, BasicType::UINT64)
                            ->addField( DEFAULT_LOGICAL_SOURCE_NAME + "$" + NES::Experimental::Statistics::DATA, BasicType::TEXT)
                            ->addField( DEFAULT_LOGICAL_SOURCE_NAME + "$" + NES::Experimental::Statistics::WIDTH, BasicType::UINT64)
                            ->addField( DEFAULT_LOGICAL_SOURCE_NAME + "$" + ON_FIELD, BasicType::INT64)
                            ->addField( DEFAULT_LOGICAL_SOURCE_NAME + "$" + "ts", BasicType::UINT64);

    std::random_device rd;
    std::mt19937 gen(Nautilus::Interface::H3Hash::H3_SEED);
    std::uniform_int_distribution<uint64_t> distribution;

    std::vector<uint64_t> H3Seeds(DEPTH * KEY_SIZE_IN_BITS, 0);
    for (auto row = 0UL; row < DEPTH; ++row) {
        for (auto keyBit = 0UL; keyBit < KEY_SIZE_IN_BITS; ++keyBit) {
            H3Seeds[row * KEY_SIZE_IN_BITS + keyBit] = distribution(gen);
        }
    }

    std::vector<std::string> allPhysicalSources = {DEFAULT_PHYSICAL_SOURCE_NAME};
    std::vector<OriginId> allOriginIds = {0};

    // create CountMinOperatorHandler
    std::vector<OriginId> allOriginIdsPhysicalSourceNames = {0};

    auto handler = std::make_shared<Experimental::Statistics::CountMinOperatorHandler>(WINDOW_SIZE,
                                                                                       SLIDE_FACTOR,
                                                                                       DEFAULT_LOGICAL_SOURCE_NAME,
                                                                                       ON_FIELD,
                                                                                       DEPTH,
                                                                                       WIDTH,
                                                                                       outputSchema,
                                                                                       H3Seeds,
                                                                                       allOriginIdsPhysicalSourceNames);

    auto pipelineContext = MockedPipelineExecutionContext({handler}, bm);
    auto ctx = ExecutionContext(Value<MemRef>((int8_t*) wc.get()), Value<MemRef>((int8_t*) &pipelineContext));

    auto timeFunction = std::make_unique<Runtime::Execution::Operators::EventTimeFunction>(
        std::make_shared<Runtime::Execution::Expressions::ReadFieldExpression>( DEFAULT_LOGICAL_SOURCE_NAME + "$" + "ts"));

    auto cmOperator = NES::Experimental::Statistics::CountMinBuildOperator(OPERATOR_HANDLER_INDEX,
                                                                           DEFAULT_LOGICAL_SOURCE_NAME,
                                                                           WIDTH,
                                                                           DEPTH,
                                                                           ON_FIELD,
                                                                           KEY_SIZE_IN_BITS,
                                                                           std::move(timeFunction),
                                                                           outputSchema);

    auto collector = std::make_shared<CollectOperator>();
    cmOperator.setChild(collector);
    cmOperator.setup(ctx);

    //open comparison sketch from csv
    const std::string filename = std::filesystem::path(TEST_DATA_DIRECTORY) / "countmin.csv";

    std::vector<uint64_t> manualCMSketch = readCsvFile(filename);

    // explicitly create cm sketch using the implemented CM Operator and its execute function
    for (uint64_t tupleValue = 0; tupleValue < TUPLES; ++tupleValue) {
        auto record = Record(
            {{ON_FIELD, Value<>(tupleValue)},
             {TS, Value<>(tupleValue)},
             {DEFAULT_LOGICAL_SOURCE_NAME + "$" + NES::Experimental::Statistics::PHYSICAL_SOURCE_NAME, Value<Nautilus::Text>(DEFAULT_PHYSICAL_SOURCE_NAME.data())}});
        cmOperator.execute(ctx, record);
    }

    // compare both cm sketches for equality
    auto interval = NES::Experimental::Statistics::Interval(0, WINDOW_SIZE);
    auto tupBuffer = handler->getTupleBuffer(DEFAULT_PHYSICAL_SOURCE_NAME, 1);
    auto dynBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer::createDynamicTupleBuffer(tupBuffer, outputSchema);

    auto countMinString = Runtime::MemoryLayouts::readVarSizedData(
        dynBuffer.getBuffer(),
        dynBuffer[0][DEFAULT_LOGICAL_SOURCE_NAME + "$" + NES::Experimental::Statistics::DATA].read<Runtime::TupleBuffer::NestedTupleBufferKey>());
    auto depth = dynBuffer[0][DEFAULT_LOGICAL_SOURCE_NAME + "$" + NES::Experimental::Statistics::DEPTH].read<uint64_t>();
    auto width = dynBuffer[0][DEFAULT_LOGICAL_SOURCE_NAME + "$" + NES::Experimental::Statistics::WIDTH].read<uint64_t>();

    auto countMinSketch = Experimental::Statistics::CountMin::createFromString((void*) countMinString.data(), depth, width);
    auto countMinData = countMinSketch.getData();

    for (uint32_t row = 0; row < DEPTH; ++row) {
        for (uint32_t column = 0; column < WIDTH; ++column) {
            ASSERT_EQ(manualCMSketch.at(row * WIDTH + column), countMinData.at(row * WIDTH + column));
        }
    }
}
}// namespace NES::Runtime::Execution::Operators