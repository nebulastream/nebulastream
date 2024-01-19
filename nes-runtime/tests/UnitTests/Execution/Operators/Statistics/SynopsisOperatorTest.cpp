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
#include <Nautilus/Interface/DataTypes/MemRef.hpp>
#include <Nautilus/Interface/DataTypes/Text/Text.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
#include <Nautilus/Interface/Hash/H3Hash.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/WorkerContext.hpp>
#include <StatisticFieldIdentifiers.hpp>
#include <Statistics/Interval.hpp>
#include <TestUtils/MockedPipelineExecutionContext.hpp>
#include <TestUtils/RecordCollectOperator.hpp>
#include <Util/Logger/Logger.hpp>
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

/**
 * @brief Tests if we limit the number of tuples
 */
TEST_F(SynopsisOperatorTest, CountMinOperatorTest) {
    const uint64_t DEPTH = 3;
    uint64_t WIDTH = 8;
    uint64_t WINDOWSIZE = 5000;
    uint64_t SLIDEFACTOR = 5000;
    std::string ONFIELD = "f1";
    const std::string TS = "ts";
    uint64_t OPERATORHANDLERINDEX = 0;
    uint64_t KEYSIZEINBITS = sizeof(uint64_t) * 8;
    uint64_t TUPLES = 5;

    auto bm = std::make_shared<Runtime::BufferManager>();
    auto wc = std::make_shared<WorkerContext>(0, bm, 100);
    auto schema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                      ->addField(ONFIELD, BasicType::INT64)
                      ->addField("ts", BasicType::UINT64)
                      ->addField(NES::Experimental::Statistics::LOGICAL_SOURCE_NAME, BasicType::TEXT)
                      ->addField(NES::Experimental::Statistics::PHYSICAL_SOURCE_NAME, BasicType::TEXT)
                      ->addField(NES::Experimental::Statistics::WORKER_ID, BasicType::UINT64)
                      ->addField(NES::Experimental::Statistics::FIELD_NAME, BasicType::TEXT);

    auto metaDataSize = sizeof(NES::Experimental::Statistics::LOGICAL_SOURCE_NAME)
        + sizeof(NES::Experimental::Statistics::PHYSICAL_SOURCE_NAME) + sizeof(uint64_t)
        + sizeof(NES::Experimental::Statistics::FIELD_NAME);

    auto handler = std::make_shared<Experimental::Statistics::CountMinOperatorHandler>(WINDOWSIZE, SLIDEFACTOR, metaDataSize);
    auto pipelineContext = MockedPipelineExecutionContext({handler});
    auto ctx = ExecutionContext(Value<MemRef>((int8_t*) wc.get()), Value<MemRef>((int8_t*) &pipelineContext));

    std::random_device rd;
    std::mt19937 gen(Nautilus::Interface::H3Hash::H3_SEED);
    std::uniform_int_distribution<uint64_t> distribution;

    std::vector<uint64_t> H3Seeds(DEPTH * KEYSIZEINBITS, 0);
    for (auto row = 0UL; row < DEPTH; ++row) {
        for (auto keyBit = 0UL; keyBit < KEYSIZEINBITS; ++keyBit) {
            H3Seeds[row * KEYSIZEINBITS + keyBit] = distribution(gen);
        }
    }

    auto timeFunction = std::make_unique<Runtime::Execution::Operators::EventTimeFunction>(
        std::make_shared<Runtime::Execution::Expressions::ReadFieldExpression>("ts"));

    auto cmOperator = NES::Experimental::Statistics::CountMinBuildOperator(OPERATORHANDLERINDEX,
                                                                           WIDTH,
                                                                           DEPTH,
                                                                           ONFIELD,
                                                                           KEYSIZEINBITS,
                                                                           std::move(timeFunction),
                                                                           H3Seeds);
    auto collector = std::make_shared<CollectOperator>();
    cmOperator.setChild(collector);

    // generate and save hashes
    std::vector<std::vector<uint64_t>> h3Hashes;
    for (auto row = 0UL; row < DEPTH; ++row) {
        h3Hashes.emplace_back();
        for (auto tupleValue = 0UL; tupleValue < TUPLES; ++tupleValue) {
            Nautilus::Value<Nautilus::MemRef> h3SeedMemRef((int8_t*) H3Seeds.data());
            h3SeedMemRef = (h3SeedMemRef + sizeof(uint64_t) * WIDTH * row).as<Nautilus::MemRef>();
            Nautilus::Value<Nautilus::UInt64> columnIdNaut = 0_u64;
            auto key = Record({{ONFIELD, Value<>((uint64_t )tupleValue)}}).read(ONFIELD);
            Nautilus::Interface::H3Hash(KEYSIZEINBITS).calculateWithState(columnIdNaut, key, h3SeedMemRef);
            h3Hashes[row].emplace_back(columnIdNaut->getValue());
        }
    }

    // generate cm sketch manually from hashes for comparison
    std::vector<std::vector<uint64_t>> manualCMSketch(DEPTH, std::vector<uint64_t>(WIDTH, 0));
    for (uint32_t row = 0; row < DEPTH; ++row) {
        for (uint32_t column = 0; column < TUPLES; ++column) {
            auto hashValue = h3Hashes[row][column];
            manualCMSketch[row][hashValue % WIDTH] += 1;
        }
    }

    // explicitly create cm sketch using the implemented CM Operator and its execute function
    for (uint64_t tupleValue = 0; tupleValue < TUPLES; ++tupleValue) {
        auto record =
            Record({{ONFIELD, Value<>(tupleValue)},
                    {TS, Value<>(tupleValue)},
                    {NES::Experimental::Statistics::LOGICAL_SOURCE_NAME, Value<Nautilus::Text>("defaultLogicalStreamName")},
                    {NES::Experimental::Statistics::PHYSICAL_SOURCE_NAME, Value<Nautilus::Text>("defaultPhysicalStreamName")},
                    {NES::Experimental::Statistics::WORKER_ID, Value<>(0)},
                    {NES::Experimental::Statistics::FIELD_NAME, Value<Nautilus::Text>(ONFIELD.data())}});
        cmOperator.execute(ctx, record);
    }

    // compare both cm sketches for equality
    auto interval = NES::Experimental::Statistics::Interval(0, WINDOWSIZE);
    auto cm = handler->getCountMin(interval);
    auto cmData = cm->getData();
    for (uint32_t row = 0; row < DEPTH; ++row) {
        for (uint32_t column = 0; column < WIDTH; ++column) {
            ASSERT_EQ(manualCMSketch[row][column], cmData[row][column]);
        }
    }
}
}// namespace NES::Runtime::Execution::Operators