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
#include <Execution/MemoryProvider/RowMemoryProvider.hpp>
#include <Execution/Operators/Scan.hpp>
#include <Execution/Operators/Statistics/CountMinBuildOperator.hpp>
#include <Execution/Operators/Statistics/CountMinOperatorHandler.hpp>
#include <Execution/Operators/Streaming/MultiOriginWatermarkProcessor.hpp>
#include <Execution/Operators/Streaming/TimeFunction.hpp>
#include <Execution/Pipelines/CompilationPipelineProvider.hpp>
#include <Execution/Pipelines/PhysicalOperatorPipeline.hpp>
#include <Execution/RecordBuffer.hpp>
#include <Nautilus/Interface/Hash/H3Hash.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/MemoryLayout/DynamicTupleBuffer.hpp>
#include <Runtime/MemoryLayout/RowLayout.hpp>
#include <Runtime/WorkerContext.hpp>
#include <StatisticFieldIdentifiers.hpp>
#include <TestUtils/AbstractPipelineExecutionTest.hpp>
#include <TestUtils/MockedPipelineExecutionContext.hpp>
#include <Util/Logger/Logger.hpp>
#include <fstream>
#include <gtest/gtest.h>
#include <memory>

namespace NES::Runtime::Execution {

class SynopsisPipelineTest : public Testing::BaseUnitTest, public AbstractPipelineExecutionTest {
  public:
    ExecutablePipelineProvider* provider;
    std::shared_ptr<Runtime::BufferManager> bm;
    std::shared_ptr<WorkerContext> wc;
    Nautilus::CompilationOptions options;
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("SynopsisPipelineTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup SynopsisPipelineTest test class.");
    }

    /* Will be called before a test is executed. */
    void SetUp() override {
        Testing::BaseUnitTest::SetUp();
        NES_INFO("Setup SynopsisPipelineTest test case.");
        if (!ExecutablePipelineProviderRegistry::hasPlugin(GetParam())) {
            GTEST_SKIP();
        }
        provider = ExecutablePipelineProviderRegistry::getPlugin(this->GetParam()).get();
        bm = std::make_shared<Runtime::BufferManager>();
        wc = std::make_shared<WorkerContext>(0, bm, 100);
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_INFO("Tear down SynopsisPipelineTest test class."); }
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
 * @brief CountMin pipeline that execute a CountMinBuildOperator and builds a Tuple that can be used to construct a CountMin sketch.
 */
TEST_P(SynopsisPipelineTest, countMinPipeline) {

    auto FIELD_NAME = "f1";
    auto tsFieldName = "ts";
    uint64_t OPERATOR_HANDLER_INDEX = 0;
    const uint64_t DEPTH = 3;
    uint64_t WIDTH = 8;
    uint64_t KEY_SIZE_IN_BITS = sizeof(uint64_t) * 8;
    uint64_t WINDOW_SIZE = 5000;
    uint64_t SLIDE_FACTOR = 5000;
    std::string DEFAULT_LOGICAL_SOURCE_NAME = "logicalSourceName1";
    std::string DEFAULT_PHYSICAL_SOURCE_NAME = "physicalSourceName1";
    WorkerId WORKER_ID_VALUE = 0;
    std::string ON_FIELD = "f1";
    uint64_t TUPLES = 6;
    uint64_t NUM_TUPLES_IN_WINDOW = 5;

    auto inputSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                           ->addField(FIELD_NAME, BasicType::UINT64)
                           ->addField(tsFieldName, BasicType::UINT64)
                           ->addField(NES::Experimental::Statistics::LOGICAL_SOURCE_NAME, BasicType::TEXT)
                           ->addField(NES::Experimental::Statistics::PHYSICAL_SOURCE_NAME, BasicType::TEXT)
                           ->addField(NES::Experimental::Statistics::WORKER_ID, BasicType::UINT64)
                           ->addField(NES::Experimental::Statistics::FIELD_NAME, BasicType::TEXT);

    auto operatorOutputSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                                    ->addField(NES::Experimental::Statistics::LOGICAL_SOURCE_NAME, BasicType::TEXT)
                                    ->addField(NES::Experimental::Statistics::PHYSICAL_SOURCE_NAME, BasicType::TEXT)
                                    ->addField(NES::Experimental::Statistics::WORKER_ID, BasicType::UINT64)
                                    ->addField(NES::Experimental::Statistics::FIELD_NAME, BasicType::TEXT)
                                    ->addField(NES::Experimental::Statistics::OBSERVED_TUPLES, BasicType::UINT64)
                                    ->addField(NES::Experimental::Statistics::DEPTH, BasicType::UINT64)
                                    ->addField(NES::Experimental::Statistics::START_TIME, BasicType::UINT64)
                                    ->addField(NES::Experimental::Statistics::END_TIME, BasicType::UINT64)
                                    ->addField(NES::Experimental::Statistics::DATA, BasicType::TEXT)
                                    ->addField(NES::Experimental::Statistics::WIDTH, BasicType::UINT64);

    // create scan Operator
    auto memoryLayout = Runtime::MemoryLayouts::RowLayout::create(inputSchema, bm->getBufferSize());
    auto scanMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(memoryLayout);
    auto scanOperator = std::make_shared<Operators::Scan>(std::move(scanMemoryProviderPtr));

    // create pipeline and set root operator
    auto pipeline = std::make_shared<PhysicalOperatorPipeline>();
    pipeline->setRootOperator(scanOperator);

    // create CountMinBuildOperator
    auto timeFunction = std::make_unique<Runtime::Execution::Operators::EventTimeFunction>(
        std::make_shared<Runtime::Execution::Expressions::ReadFieldExpression>(tsFieldName));

    std::random_device rd;
    std::mt19937 gen(Nautilus::Interface::H3Hash::H3_SEED);
    std::uniform_int_distribution<uint64_t> distribution;

    std::vector<uint64_t> H3Seeds(DEPTH * KEY_SIZE_IN_BITS, 0);
    for (auto row = 0UL; row < DEPTH; ++row) {
        for (auto keyBit = 0UL; keyBit < KEY_SIZE_IN_BITS; ++keyBit) {
            H3Seeds[row * KEY_SIZE_IN_BITS + keyBit] = distribution(gen);
        }
    }

    auto countMinBuildOperator = std::make_shared<NES::Experimental::Statistics::CountMinBuildOperator>(OPERATOR_HANDLER_INDEX,
                                                                                                        WIDTH,
                                                                                                        DEPTH,
                                                                                                        FIELD_NAME,
                                                                                                        KEY_SIZE_IN_BITS,
                                                                                                        std::move(timeFunction),
                                                                                                        operatorOutputSchema);
    // set as child operator of the scan
    scanOperator->setChild(countMinBuildOperator);

    // create data
    auto buffer = bm->getBufferBlocking();
    auto dynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer::createDynamicTupleBuffer(buffer, inputSchema);
    for (uint64_t i = 0; i < TUPLES; i++) {
        dynamicBuffer[i][FIELD_NAME].write(i);
        if (i != NUM_TUPLES_IN_WINDOW) {
            dynamicBuffer[i][tsFieldName].write(i * 1000);
        } else {
            dynamicBuffer[i][tsFieldName].write(i * 1000 + 1000);
        }
        dynamicBuffer[i].writeVarSized(NES::Experimental::Statistics::LOGICAL_SOURCE_NAME, DEFAULT_LOGICAL_SOURCE_NAME, bm.get());
        dynamicBuffer[i].writeVarSized(NES::Experimental::Statistics::PHYSICAL_SOURCE_NAME,
                                       DEFAULT_PHYSICAL_SOURCE_NAME,
                                       bm.get());
        dynamicBuffer[i][NES::Experimental::Statistics::WORKER_ID].write(WORKER_ID_VALUE);
        dynamicBuffer[i].writeVarSized(NES::Experimental::Statistics::FIELD_NAME, ON_FIELD, bm.get());
        dynamicBuffer.setNumberOfTuples(i + 1);
        dynamicBuffer.setSequenceNumber(1);
    }

    auto phys1 =
        Runtime::MemoryLayouts::readVarSizedData(dynamicBuffer.getBuffer(),
                                                 dynamicBuffer[0][NES::Experimental::Statistics::PHYSICAL_SOURCE_NAME]
                                                     .read<Runtime::TupleBuffer::NestedTupleBufferKey>());

//    NES_INFO("Creating a new tuple buffer to store a count min sketch for ts {} physicalSourceName {}", ts, physicalSourceName);

    auto filledBuffer = dynamicBuffer.getBuffer();
    filledBuffer.setNumberOfTuples(dynamicBuffer.getNumberOfTuples());
    filledBuffer.setWatermark(6000);

    // create CountMinOperatorHandler
    std::vector<std::pair<OriginId, std::string>> allOriginIdsPhysicalSourceNames = {
        std::make_pair(0, DEFAULT_PHYSICAL_SOURCE_NAME)};

    auto handler = std::make_shared<Experimental::Statistics::CountMinOperatorHandler>(WINDOW_SIZE,
                                                                                       SLIDE_FACTOR,
                                                                                       DEFAULT_LOGICAL_SOURCE_NAME,
                                                                                       WORKER_ID_VALUE,
                                                                                       ON_FIELD,
                                                                                       DEPTH,
                                                                                       WIDTH,
                                                                                       operatorOutputSchema,
                                                                                       H3Seeds,
                                                                                       allOriginIdsPhysicalSourceNames);

    auto executablePipeline = provider->create(pipeline, options);

    auto pipelineContext = MockedPipelineExecutionContext({handler}, bm);
    executablePipeline->setup(pipelineContext);
    executablePipeline->execute(buffer, pipelineContext, *wc);
    executablePipeline->stop(pipelineContext);

    ASSERT_EQ(pipelineContext.buffers.size(), 1);
    auto resultBuffer = pipelineContext.buffers[0];
    ASSERT_EQ(resultBuffer.getNumberOfTuples(), 1);

    auto dynResultBuffer =
        Runtime::MemoryLayouts::DynamicTupleBuffer::createDynamicTupleBuffer(resultBuffer, operatorOutputSchema);

    auto resultLogicalSourceName =
        Runtime::MemoryLayouts::readVarSizedData(dynResultBuffer.getBuffer(),
                                                 dynResultBuffer[0][NES::Experimental::Statistics::LOGICAL_SOURCE_NAME]
                                                     .read<Runtime::TupleBuffer::NestedTupleBufferKey>());

    auto resultPhysicalSourceName =
        Runtime::MemoryLayouts::readVarSizedData(dynResultBuffer.getBuffer(),
                                                 dynResultBuffer[0][NES::Experimental::Statistics::PHYSICAL_SOURCE_NAME]
                                                     .read<Runtime::TupleBuffer::NestedTupleBufferKey>());

    auto resultWorkerId = dynResultBuffer[0][NES::Experimental::Statistics::WORKER_ID].read<uint64_t>();

    auto resultFieldName =
        Runtime::MemoryLayouts::readVarSizedData(dynResultBuffer.getBuffer(),
                                                 dynResultBuffer[0][NES::Experimental::Statistics::FIELD_NAME]
                                                     .read<Runtime::TupleBuffer::NestedTupleBufferKey>());

    auto resultObservedTuples = dynResultBuffer[0][NES::Experimental::Statistics::OBSERVED_TUPLES].read<uint64_t>();
    auto resultStartTime = dynResultBuffer[0][NES::Experimental::Statistics::START_TIME].read<uint64_t>();
    auto resultEndTime = dynResultBuffer[0][NES::Experimental::Statistics::END_TIME].read<uint64_t>();
    auto resultDepth = dynResultBuffer[0][NES::Experimental::Statistics::DEPTH].read<uint64_t>();
    auto resultWidth = dynResultBuffer[0][NES::Experimental::Statistics::WIDTH].read<uint64_t>();

    ASSERT_EQ(DEFAULT_LOGICAL_SOURCE_NAME, resultLogicalSourceName);
    ASSERT_EQ(DEFAULT_PHYSICAL_SOURCE_NAME, resultPhysicalSourceName);
    ASSERT_EQ(WORKER_ID_VALUE, resultWorkerId);
    ASSERT_EQ(ON_FIELD, resultFieldName);
    ASSERT_EQ(NUM_TUPLES_IN_WINDOW, resultObservedTuples);
    ASSERT_EQ(0, resultStartTime);
    ASSERT_EQ(WINDOW_SIZE, resultEndTime);
    ASSERT_EQ(DEPTH, resultDepth);
    ASSERT_EQ(WIDTH, resultWidth);

    auto countMinString = Runtime::MemoryLayouts::readVarSizedData(
        dynResultBuffer.getBuffer(),
        dynResultBuffer[0][NES::Experimental::Statistics::DATA].read<Runtime::TupleBuffer::NestedTupleBufferKey>());

    auto countMinSketch = Experimental::Statistics::CountMin::createFromString((void*) countMinString.data(), DEPTH, WIDTH);
    auto countMinData = countMinSketch.getData();

    //open comparison sketch from csv
    const std::string filename = std::filesystem::path(TEST_DATA_DIRECTORY) / "countmin.csv";

    std::vector<uint64_t> manualCMSketch = readCsvFile(filename);

    for (uint32_t row = 0; row < DEPTH; ++row) {
        for (uint32_t column = 0; column < WIDTH; ++column) {
            ASSERT_EQ(manualCMSketch.at(row * WIDTH + column), countMinData.at(row * WIDTH + column));
        }
    }
}

INSTANTIATE_TEST_CASE_P(testIfCompilation,
                        SynopsisPipelineTest,
                        ::testing::Values("PipelineInterpreter", "BCInterpreter", "PipelineCompiler", "CPPPipelineCompiler"),
                        [](const testing::TestParamInfo<SynopsisPipelineTest::ParamType>& info) {
                            return info.param;
                        });
}// namespace NES::Runtime::Execution