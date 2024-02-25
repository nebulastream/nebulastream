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
#include <API/QueryAPI.hpp>
#include <API/Schema.hpp>
#include <BaseUnitTest.hpp>
#include <Catalogs/Source/SourceCatalog.hpp>
#include <Catalogs/UDF/UDFCatalog.hpp>
#include <Operators/LogicalOperators/StatisticCollection/Characteristic/DataCharacteristic.hpp>
#include <Operators/LogicalOperators/StatisticCollection/Descriptor/CountMinDescriptor.hpp>
#include <Operators/LogicalOperators/StatisticCollection/Descriptor/HyperLogLogDescriptor.hpp>
#include <Operators/LogicalOperators/StatisticCollection/LogicalStatisticWindowOperator.hpp>
#include <Operators/LogicalOperators/StatisticCollection/SendingPolicy/SendingPolicyASAP.hpp>
#include <Operators/LogicalOperators/StatisticCollection/SendingPolicy/SendingPolicyAdaptive.hpp>
#include <Operators/LogicalOperators/StatisticCollection/SendingPolicy/SendingPolicyLazy.hpp>
#include <Operators/LogicalOperators/StatisticCollection/Statistic/Metric/BufferRate.hpp>
#include <Operators/LogicalOperators/StatisticCollection/Statistic/Metric/Cardinality.hpp>
#include <Operators/LogicalOperators/StatisticCollection/Statistic/Metric/IngestionRate.hpp>
#include <Operators/LogicalOperators/StatisticCollection/Statistic/Metric/MinVal.hpp>
#include <Operators/LogicalOperators/StatisticCollection/Statistic/Metric/Selectivity.hpp>
#include <Operators/LogicalOperators/StatisticCollection/TriggerCondition/NeverTrigger.hpp>
#include <Operators/LogicalOperators/StatisticCollection/WindowStatisticDescriptor.hpp>
#include <Operators/LogicalOperators/Windows/Measures/TimeMeasure.hpp>
#include <Operators/LogicalOperators/Windows/Types/TumblingWindow.hpp>
#include <Operators/LogicalOperators/Windows/WindowingForwardRefs.hpp>
#include <Optimizer/Phases/TypeInferencePhase.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <StatisticCollection/QueryGeneration/DefaultStatisticQueryGenerator.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES {

class DefaultStatisticQueryGeneratorTest : public Testing::BaseUnitTest {
  public:
    static void SetUpTestCase() {
        NES::Logger::setupLogging("DefaultStatisticQueryGeneratorTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup DefaultStatisticQueryGeneratorTest test class.");
    }

    /* Will be called before a test is executed. */
    void SetUp() override {
        NES::Testing::BaseUnitTest::SetUp();

        inputSchema = Schema::create()
                          ->addField("f1", BasicType::INT64)
                          ->addField("f2", BasicType::FLOAT64)
                          ->addField("ts", BasicType::UINT64);

        // Add all fields that are general for all statistic descriptors
        outputSchemaBuildOperator = Schema::create()
                                        ->addField("car$" + Statistic::OBSERVED_TUPLES_FIELD_NAME, BasicType::UINT64)
                                        ->addField("car$" + Statistic::BASE_FIELD_NAME_START, BasicType::UINT64)
                                        ->addField("car$" + Statistic::BASE_FIELD_NAME_END, BasicType::UINT64)
                                        ->addField("car$" + Statistic::STATISTIC_KEY_FIELD_NAME, BasicType::UINT64)
                                        ->addField("car$" + Statistic::STATISTIC_TYPE_FIELD_NAME, BasicType::UINT64);

        // Creating the TypeInferencePhase
        sourceCatalog = std::make_shared<Catalogs::Source::SourceCatalog>();
        sourceCatalog->addLogicalSource("car", inputSchema);
        udfCatalog = Catalogs::UDF::UDFCatalog::create();
        typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);
    }

    LogicalOperatorNodePtr checkWindowStatisticOperatorCorrect(QueryPlan& queryPlan, Windowing::WindowType& window) const {
        using namespace NES::Statistic;
        auto rootOperators = queryPlan.getRootOperators();
        EXPECT_EQ(rootOperators.size(), 1);

        auto sinkChild = rootOperators[0]->getChildren();
        EXPECT_EQ(sinkChild.size(), 1);
        EXPECT_TRUE(sinkChild[0]->instanceOf<LogicalStatisticWindowOperator>());
        auto statisticWindowOperatorNode = sinkChild[0]->as<LogicalStatisticWindowOperator>();
        EXPECT_TRUE(window.equal(statisticWindowOperatorNode->getWindowType()));
        EXPECT_TRUE(statisticWindowOperatorNode->getOutputSchema()->equals(outputSchemaBuildOperator, false));

        return statisticWindowOperatorNode;
    }

    Statistic::DefaultStatisticQueryGenerator defaultStatisticQueryGenerator;
    SchemaPtr inputSchema;
    SchemaPtr outputSchemaBuildOperator;
    Catalogs::Source::SourceCatalogPtr sourceCatalog;
    Catalogs::UDF::UDFCatalogPtr udfCatalog;
    Optimizer::TypeInferencePhasePtr typeInferencePhase;
};

/**
 * @brief Tests if a query is generated correctly for a cardinality, the outcome should be a HyperLogLog
 */
TEST_F(DefaultStatisticQueryGeneratorTest, cardinality) {
    using namespace NES::Statistic;
    using namespace Windowing;

    // Adding here the specific descriptor fields
    outputSchemaBuildOperator = outputSchemaBuildOperator
                                    ->addField("car$" + STATISTIC_DATA_FIELD_NAME, BasicType::TEXT)
                                    ->addField("car$" + WIDTH_FIELD_NAME, BasicType::UINT64);


    constexpr auto EXPECTED_WIDTH = 512;
    const auto dataCharacteristic = DataCharacteristic::create(Cardinality::create(Over("f1")), "car");
    const auto window = TumblingWindow::of(EventTime(Attribute("ts")), Seconds(10));
    const auto sendingPolicy = SENDING_ASAP;
    const auto triggerCondition = NeverTrigger::create();

    // Creating a statistic query and running the typeInference
    const auto statisticQuery = defaultStatisticQueryGenerator.createStatisticQuery(*dataCharacteristic,
                                                                                    window,
                                                                                    sendingPolicy,
                                                                                    triggerCondition);
    typeInferencePhase->execute(statisticQuery.getQueryPlan());

    // Checking if the operator is correct
    auto statisticWindowOperatorNode = checkWindowStatisticOperatorCorrect(*statisticQuery.getQueryPlan(), *window);
    auto descriptor = statisticWindowOperatorNode->as<LogicalStatisticWindowOperator>()->getWindowStatisticDescriptor();

    // Checking if the descriptor is correct
    ASSERT_TRUE(descriptor->instanceOf<HyperLogLogDescriptor>());
    auto hyperLoglogDescriptor = descriptor->as<HyperLogLogDescriptor>();
    auto operatorSendingPolicy = descriptor->getSendingPolicy();
    auto operatorTriggerCondition = descriptor->getTriggerCondition();
    EXPECT_TRUE(hyperLoglogDescriptor->getField()->equal(Over("f1")));
    EXPECT_TRUE(std::dynamic_pointer_cast<SendingPolicyASAP>(operatorSendingPolicy));
    EXPECT_TRUE(operatorTriggerCondition->instanceOf<NeverTrigger>());
    EXPECT_EQ(hyperLoglogDescriptor->getWidth(), EXPECTED_WIDTH);
}

/**
 * @brief Tests if a query is generated correctly for a selectivity, the outcome should be a CountMin
 */
TEST_F(DefaultStatisticQueryGeneratorTest, selectivity) {
    using namespace NES::Statistic;
    using namespace Windowing;

    // Adding here the specific descriptor fields
    outputSchemaBuildOperator = outputSchemaBuildOperator
                                    ->addField("car$" + STATISTIC_DATA_FIELD_NAME, BasicType::TEXT)
                                    ->addField("car$" + WIDTH_FIELD_NAME, BasicType::UINT64)
                                    ->addField("car$" + DEPTH_FIELD_NAME, BasicType::UINT64);


    constexpr auto EXPECTED_WIDTH = 55;
    constexpr auto EXPECTED_DEPTH = 3;
    const auto dataCharacteristic = DataCharacteristic::create(Selectivity::create(Over("f1")), "car");
    const auto window = SlidingWindow::of(EventTime(Attribute("ts")), Seconds(60), Seconds(10));
    const auto sendingPolicy = SENDING_LAZY;
    const auto triggerCondition = NeverTrigger::create();

    // Creating a statistic query and running the typeInference
    const auto statisticQuery = defaultStatisticQueryGenerator.createStatisticQuery(*dataCharacteristic,
                                                                                    window,
                                                                                    sendingPolicy,
                                                                                    triggerCondition);
    typeInferencePhase->execute(statisticQuery.getQueryPlan());

    // Checking if the operator is correct
    auto statisticWindowOperatorNode = checkWindowStatisticOperatorCorrect(*statisticQuery.getQueryPlan(), *window);
    auto descriptor = statisticWindowOperatorNode->as<LogicalStatisticWindowOperator>()->getWindowStatisticDescriptor();

    // Checking if the descriptor is correct
    ASSERT_TRUE(descriptor->instanceOf<CountMinDescriptor>());
    auto countMinDescriptor = descriptor->as<CountMinDescriptor>();
    auto operatorSendingPolicy = descriptor->getSendingPolicy();
    auto operatorTriggerCondition = descriptor->getTriggerCondition();
    EXPECT_TRUE(countMinDescriptor->getField()->equal(Over("f1")));
    EXPECT_TRUE(std::dynamic_pointer_cast<SendingPolicyLazy>(operatorSendingPolicy));
    EXPECT_TRUE(operatorTriggerCondition->instanceOf<NeverTrigger>());
    EXPECT_EQ(countMinDescriptor->getWidth(), EXPECTED_WIDTH);
    EXPECT_EQ(countMinDescriptor->getDepth(), EXPECTED_DEPTH);
}

/**
 * @brief Tests if a query is generated correctly for an ingestionRate, the outcome should be a CountMin
 */
TEST_F(DefaultStatisticQueryGeneratorTest, ingestionRate) {
    using namespace NES::Statistic;
    using namespace Windowing;

    // Adding here the specific descriptor fields
    outputSchemaBuildOperator = outputSchemaBuildOperator
                                    ->addField("car$" + STATISTIC_DATA_FIELD_NAME, BasicType::TEXT)
                                    ->addField("car$" + WIDTH_FIELD_NAME, BasicType::UINT64)
                                    ->addField("car$" + DEPTH_FIELD_NAME, BasicType::UINT64);


    constexpr auto EXPECTED_WIDTH = 55;
    constexpr auto EXPECTED_DEPTH = 3;
    const auto dataCharacteristic = DataCharacteristic::create(Selectivity::create(Over("f1")), "car");
    const auto window = SlidingWindow::of(EventTime(Attribute("ts")), Seconds(60), Seconds(10));
    const auto sendingPolicy = SENDING_LAZY;
    const auto triggerCondition = NeverTrigger::create();

    // Creating a statistic query and running the typeInference
    const auto statisticQuery = defaultStatisticQueryGenerator.createStatisticQuery(*dataCharacteristic,
                                                                                    window,
                                                                                    sendingPolicy,
                                                                                    triggerCondition);
    typeInferencePhase->execute(statisticQuery.getQueryPlan());

    // Checking if the operator is correct
    auto statisticWindowOperatorNode = checkWindowStatisticOperatorCorrect(*statisticQuery.getQueryPlan(), *window);
    auto descriptor = statisticWindowOperatorNode->as<LogicalStatisticWindowOperator>()->getWindowStatisticDescriptor();

    // Checking if the descriptor is correct
    ASSERT_TRUE(descriptor->instanceOf<CountMinDescriptor>());
    auto countMinDescriptor = descriptor->as<CountMinDescriptor>();
    auto operatorSendingPolicy = descriptor->getSendingPolicy();
    auto operatorTriggerCondition = descriptor->getTriggerCondition();
    EXPECT_TRUE(countMinDescriptor->getField()->equal(Over("f1")));
    EXPECT_TRUE(std::dynamic_pointer_cast<SendingPolicyLazy>(operatorSendingPolicy));
    EXPECT_TRUE(operatorTriggerCondition->instanceOf<NeverTrigger>());
    EXPECT_EQ(countMinDescriptor->getWidth(), EXPECTED_WIDTH);
    EXPECT_EQ(countMinDescriptor->getDepth(), EXPECTED_DEPTH);
}

/**
 * @brief Tests if a query is generated correctly for a bufferRate, the outcome should be a CountMin
 */
TEST_F(DefaultStatisticQueryGeneratorTest, bufferRate) {
    using namespace NES::Statistic;
    using namespace Windowing;

    // Adding here the specific descriptor fields
    outputSchemaBuildOperator = outputSchemaBuildOperator
                                    ->addField("car$" + STATISTIC_DATA_FIELD_NAME, BasicType::TEXT)
                                    ->addField("car$" + WIDTH_FIELD_NAME, BasicType::UINT64)
                                    ->addField("car$" + DEPTH_FIELD_NAME, BasicType::UINT64);


    constexpr auto EXPECTED_WIDTH = 55;
    constexpr auto EXPECTED_DEPTH = 3;
    const auto dataCharacteristic = DataCharacteristic::create(Selectivity::create(Over("f1")), "car");
    const auto window = SlidingWindow::of(EventTime(Attribute("ts")), Hours(24), Seconds(60));
    const auto sendingPolicy = SENDING_ADAPTIVE;
    const auto triggerCondition = NeverTrigger::create();

    // Creating a statistic query and running the typeInference
    const auto statisticQuery = defaultStatisticQueryGenerator.createStatisticQuery(*dataCharacteristic,
                                                                                    window,
                                                                                    sendingPolicy,
                                                                                    triggerCondition);
    typeInferencePhase->execute(statisticQuery.getQueryPlan());

    // Checking if the operator is correct
    auto statisticWindowOperatorNode = checkWindowStatisticOperatorCorrect(*statisticQuery.getQueryPlan(), *window);
    auto descriptor = statisticWindowOperatorNode->as<LogicalStatisticWindowOperator>()->getWindowStatisticDescriptor();

    // Checking if the descriptor is correct
    ASSERT_TRUE(descriptor->instanceOf<CountMinDescriptor>());
    auto countMinDescriptor = descriptor->as<CountMinDescriptor>();
    auto operatorSendingPolicy = descriptor->getSendingPolicy();
    auto operatorTriggerCondition = descriptor->getTriggerCondition();
    EXPECT_TRUE(countMinDescriptor->getField()->equal(Over("f1")));
    EXPECT_TRUE(std::dynamic_pointer_cast<SendingPolicyAdaptive>(operatorSendingPolicy));
    EXPECT_TRUE(operatorTriggerCondition->instanceOf<NeverTrigger>());
    EXPECT_EQ(countMinDescriptor->getWidth(), EXPECTED_WIDTH);
    EXPECT_EQ(countMinDescriptor->getDepth(), EXPECTED_DEPTH);
}

/**
 * @brief Tests if a query is generated correctly for a minVal, the outcome should be a CountMin
 */
TEST_F(DefaultStatisticQueryGeneratorTest, minVal) {
    using namespace NES::Statistic;
    using namespace Windowing;

    // Adding here the specific descriptor fields
    outputSchemaBuildOperator = outputSchemaBuildOperator
                                    ->addField("car$" + STATISTIC_DATA_FIELD_NAME, BasicType::TEXT)
                                    ->addField("car$" + WIDTH_FIELD_NAME, BasicType::UINT64)
                                    ->addField("car$" + DEPTH_FIELD_NAME, BasicType::UINT64);


    constexpr auto EXPECTED_WIDTH = 55;
    constexpr auto EXPECTED_DEPTH = 3;
    const auto dataCharacteristic = DataCharacteristic::create(Selectivity::create(Over("f1")), "car");
    const auto window = SlidingWindow::of(EventTime(Attribute("ts")), Seconds(60), Seconds(10));
    const auto sendingPolicy = SENDING_LAZY;
    const auto triggerCondition = NeverTrigger::create();

    // Creating a statistic query and running the typeInference
    const auto statisticQuery = defaultStatisticQueryGenerator.createStatisticQuery(*dataCharacteristic,
                                                                                    window,
                                                                                    sendingPolicy,
                                                                                    triggerCondition);
    typeInferencePhase->execute(statisticQuery.getQueryPlan());

    // Checking if the operator is correct
    auto statisticWindowOperatorNode = checkWindowStatisticOperatorCorrect(*statisticQuery.getQueryPlan(), *window);
    auto descriptor = statisticWindowOperatorNode->as<LogicalStatisticWindowOperator>()->getWindowStatisticDescriptor();

    // Checking if the descriptor is correct
    ASSERT_TRUE(descriptor->instanceOf<CountMinDescriptor>());
    auto countMinDescriptor = descriptor->as<CountMinDescriptor>();
    auto operatorSendingPolicy = descriptor->getSendingPolicy();
    auto operatorTriggerCondition = descriptor->getTriggerCondition();
    EXPECT_TRUE(countMinDescriptor->getField()->equal(Over("f1")));
    EXPECT_TRUE(std::dynamic_pointer_cast<SendingPolicyLazy>(operatorSendingPolicy));
    EXPECT_TRUE(operatorTriggerCondition->instanceOf<NeverTrigger>());
    EXPECT_EQ(countMinDescriptor->getWidth(), EXPECTED_WIDTH);
    EXPECT_EQ(countMinDescriptor->getDepth(), EXPECTED_DEPTH);
}
}// namespace NES