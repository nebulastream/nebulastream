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

// clang-format off
#include <gtest/gtest.h>
#include <NesBaseTest.hpp>
// clang-format on
#include <API/QueryAPI.hpp>
#include <Catalogs/Source/SourceCatalog.hpp>
#include <Catalogs/UDF/UDFCatalog.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Compiler/CPPCompiler/CPPCompiler.hpp>
#include <Compiler/JITCompilerBuilder.hpp>
#include <Operators/LogicalOperators/MapLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sinks/PrintSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/LogicalSourceDescriptor.hpp>
#include <Operators/LogicalOperators/WatermarkAssignerLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Windowing/WindowOperatorNode.hpp>
#include <Optimizer/Phases/SignatureInferencePhase.hpp>
#include <Optimizer/Phases/TypeInferencePhase.hpp>
#include <Optimizer/QueryMerger/HashSignatureBasedPartialQueryContainmentMergerRule.hpp>
#include <Optimizer/QueryValidation/SyntacticQueryValidation.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Plans/Global/Query/SharedQueryPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Plans/Utils/PlanIdGenerator.hpp>
#include <Services/QueryParsingService.hpp>
#include <Util/Logger/Logger.hpp>
#include <iostream>
#include <z3++.h>

using namespace NES;

class HashSignatureBasedPartialQueryContainmentMergerTestEntry {

  public:
    HashSignatureBasedPartialQueryContainmentMergerTestEntry(const std::string& testType,
                                                             const std::string& leftQuery,
                                                             const std::string& rightQuery,
                                                             const std::string& mergedQueryPlan)
        : testType(testType), leftQuery(leftQuery), rightQuery(rightQuery), mergedQueryPlan(mergedQueryPlan) {}

    std::string testType;
    std::string leftQuery;
    std::string rightQuery;
    std::string mergedQueryPlan;
};

class HashSignatureBasedPartialQueryContainmentMergerRuleTest
    : public Testing::TestWithErrorHandling,
      public testing::WithParamInterface<std::vector<HashSignatureBasedPartialQueryContainmentMergerTestEntry>> {

  public:
    SchemaPtr schema;
    SchemaPtr schemaHouseholds;
    Catalogs::Source::SourceCatalogPtr sourceCatalog;
    std::shared_ptr<Catalogs::UDF::UDFCatalog> udfCatalog;
    Optimizer::SyntacticQueryValidationPtr syntacticQueryValidation;

    /* Will be called before all tests in this class are started. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("HashSignatureBasedPartialQueryContainmentMergerTest.log", NES::LogLevel::LOG_TRACE);
        NES_INFO2("Setup HashSignatureBasedPartialQueryContainmentMergerTest test case.");
    }

    /* Will be called before a test is executed. */
    void SetUp() override {
        schema = Schema::create()
                     ->addField("ts", BasicType::UINT32)
                     ->addField("type", BasicType::UINT32)
                     ->addField("id", BasicType::UINT32)
                     ->addField("value", BasicType::UINT64)
                     ->addField("id1", BasicType::UINT32)
                     ->addField("value1", BasicType::UINT64)
                     ->addField("value2", BasicType::UINT64);

        schemaHouseholds = Schema::create()
                               ->addField("ts", BasicType::UINT32)
                               ->addField("type", DataTypeFactory::createFixedChar(8))
                               ->addField("id", BasicType::UINT32)
                               ->addField("value", BasicType::UINT64)
                               ->addField("id1", BasicType::UINT32)
                               ->addField("value1", BasicType::UINT64)
                               ->addField("value2", BasicType::UINT64)
                               ->addField("value3", BasicType::UINT64);

        sourceCatalog = std::make_shared<Catalogs::Source::SourceCatalog>(QueryParsingServicePtr());
        sourceCatalog->addLogicalSource("windTurbines", schema);
        sourceCatalog->addLogicalSource("solarPanels", schema);
        sourceCatalog->addLogicalSource("test", schema);
        sourceCatalog->addLogicalSource("households", schemaHouseholds);
        udfCatalog = Catalogs::UDF::UDFCatalog::create();
        auto cppCompiler = Compiler::CPPCompiler::create();
        auto jitCompiler = Compiler::JITCompilerBuilder().registerLanguageCompiler(cppCompiler).build();
        syntacticQueryValidation = Optimizer::SyntacticQueryValidation::create(QueryParsingService::create(jitCompiler));
    }

    /* Will be called after a test is executed. */
    void TearDown() override { NES_DEBUG2("Tear down HashSignatureBasedPartialQueryContainmentMergerTest test case."); }

    static auto createEqualityCases() {
        return std::vector<HashSignatureBasedPartialQueryContainmentMergerTestEntry>{
            HashSignatureBasedPartialQueryContainmentMergerTestEntry(
                "TestEquality",
                R"(Query::from("windTurbines").map(Attribute("value") = 40).filter(Attribute("id") < 45).filter(Attribute("id") < 45).filter(Attribute("id") < 45).filter(Attribute("id") < 45).sink(PrintSinkDescriptor::create());)",
                R"(Query::from("windTurbines").map(Attribute("value") = 40).filter(Attribute("id") < 45).filter(Attribute("id") < 45).filter(Attribute("id") < 45).filter(Attribute("id") < 45).sink(PrintSinkDescriptor::create());)",
                "SINK(7: {PrintSinkDescriptor()})\n  FILTER(13)\n    FILTER(12)\n      FILTER(11)\n        FILTER(10)\n          "
                "MAP(9)\n            SOURCE(8,windTurbines,LogicalSourceDescriptor(windTurbines, ))\nSINK(14: "
                "{PrintSinkDescriptor()})\n  FILTER(13)\n    FILTER(12)\n      FILTER(11)\n        FILTER(10)\n          "
                "MAP(9)\n            SOURCE(8,windTurbines,LogicalSourceDescriptor(windTurbines, ))\n"),
            HashSignatureBasedPartialQueryContainmentMergerTestEntry(
                "TestEquality",
                R"(Query::from("windTurbines").map(Attribute("value") = 40).filter(Attribute("id") < 45).filter(Attribute("id") < 45))"
                R"(.filter(Attribute("id") < 45).filter(Attribute("id") < 45).sink(PrintSinkDescriptor::create());)",
                R"(Query::from("windTurbines").map(Attribute("value") = 40).filter(Attribute("id") < 45).filter(Attribute("id") < 45).filter(Attribute("id") < 45).filter(Attribute("id") < 45).sink(PrintSinkDescriptor::create());)",
                "SINK(21: {PrintSinkDescriptor()})\n  FILTER(27)\n    FILTER(26)\n      FILTER(25)\n        FILTER(24)\n         "
                " MAP(23)\n            SOURCE(22,windTurbines,LogicalSourceDescriptor(windTurbines, ))\nSINK(28: "
                "{PrintSinkDescriptor()})\n  FILTER(27)\n    FILTER(26)\n      FILTER(25)\n        FILTER(24)\n          "
                "MAP(23)\n            SOURCE(22,windTurbines,LogicalSourceDescriptor(windTurbines, ))\n"),
            HashSignatureBasedPartialQueryContainmentMergerTestEntry(
                "TestEquality",
                R"(Query::from("windTurbines").window(TumblingWindow::of(EventTime(Attribute("ts")), Seconds(10))).apply(Sum(Attribute("value1"))).sink(PrintSinkDescriptor::create());)",
                R"(Query::from("windTurbines").window(TumblingWindow::of(EventTime(Attribute("ts")), Seconds(10))).apply(Sum(Attribute("value1"))).sink(PrintSinkDescriptor::create());)",
                "SINK(32: {PrintSinkDescriptor()})\n  WINDOW AGGREGATION(OP-31, Sum;)\n    WATERMARKASSIGNER(30)\n      "
                "SOURCE(29,windTurbines,LogicalSourceDescriptor(windTurbines, ))\nSINK(36: {PrintSinkDescriptor()})\n  WINDOW "
                "AGGREGATION(OP-31, Sum;)\n    WATERMARKASSIGNER(30)\n      "
                "SOURCE(29,windTurbines,LogicalSourceDescriptor(windTurbines, ))\n"),
            HashSignatureBasedPartialQueryContainmentMergerTestEntry(
                "TestEquality",
                R"(Query::from("windTurbines").joinWith(Query::from("solarPanels")).where(Attribute("id1")).equalsTo(Attribute("id")).window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))).sink(PrintSinkDescriptor::create());)",
                R"(Query::from("windTurbines").joinWith(Query::from("solarPanels")).where(Attribute("id1")).equalsTo(Attribute("id")).window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))).sink(PrintSinkDescriptor::create());)",
                "SINK(42: {PrintSinkDescriptor()})\n  Join(41)\n    WATERMARKASSIGNER(39)\n      "
                "SOURCE(37,windTurbines,LogicalSourceDescriptor(windTurbines, ))\n    WATERMARKASSIGNER(40)\n      "
                "SOURCE(38,solarPanels,LogicalSourceDescriptor(solarPanels, ))\nSINK(48: {PrintSinkDescriptor()})\n  Join(41)\n  "
                "  WATERMARKASSIGNER(39)\n      SOURCE(37,windTurbines,LogicalSourceDescriptor(windTurbines, ))\n    "
                "WATERMARKASSIGNER(40)\n      SOURCE(38,solarPanels,LogicalSourceDescriptor(solarPanels, ))\n"),
            HashSignatureBasedPartialQueryContainmentMergerTestEntry(
                "TestEquality",
                R"(Query::from("windTurbines").filter(Attribute("value") < 5).joinWith(Query::from("solarPanels")).where(Attribute("id1")).equalsTo(Attribute("id")).window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))).sink(PrintSinkDescriptor::create());)",
                R"(Query::from("windTurbines").joinWith(Query::from("solarPanels")).where(Attribute("id1")).equalsTo(Attribute("id")).window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))).filter(Attribute("value") < 5).sink(PrintSinkDescriptor::create());)",
                "SINK(55: {PrintSinkDescriptor()})\n  Join(54)\n    WATERMARKASSIGNER(52)\n      FILTER(50)\n        "
                "SOURCE(49,windTurbines,LogicalSourceDescriptor(windTurbines, ))\n    WATERMARKASSIGNER(59)\n      "
                "SOURCE(57,solarPanels,LogicalSourceDescriptor(solarPanels, ))\nSINK(62: {PrintSinkDescriptor()})\n  "
                "FILTER(61)\n    Join(60)\n      WATERMARKASSIGNER(58)\n        "
                "SOURCE(49,windTurbines,LogicalSourceDescriptor(windTurbines, ))\n      WATERMARKASSIGNER(59)\n        "
                "SOURCE(57,solarPanels,LogicalSourceDescriptor(solarPanels, ))\n"),
            HashSignatureBasedPartialQueryContainmentMergerTestEntry(
                "TestEquality",
                R"(Query::from("windTurbines").window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))).apply(Sum(Attribute("value"))).sink(PrintSinkDescriptor::create());)",
                R"(Query::from("windTurbines").window(SlidingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000), Milliseconds(1000))).apply(Sum(Attribute("value"))).sink(PrintSinkDescriptor::create());)",
                "SINK(66: {PrintSinkDescriptor()})\n  WINDOW AGGREGATION(OP-65, Sum;)\n    WATERMARKASSIGNER(68)\n      "
                "SOURCE(67,windTurbines,LogicalSourceDescriptor(windTurbines, ))\nSINK(70: {PrintSinkDescriptor()})\n  WINDOW "
                "AGGREGATION(OP-69, Sum;)\n    WATERMARKASSIGNER(68)\n      "
                "SOURCE(67,windTurbines,LogicalSourceDescriptor(windTurbines, ))\n"),
            HashSignatureBasedPartialQueryContainmentMergerTestEntry(
                "TestEquality",
                R"(Query::from("windTurbines").unionWith(Query::from("solarPanels")).sink(PrintSinkDescriptor::create());)",
                R"(Query::from("windTurbines").unionWith(Query::from("solarPanels")).sink(PrintSinkDescriptor::create());)",
                "SINK(74: {PrintSinkDescriptor()})\n  unionWith(77)\n    "
                "SOURCE(75,windTurbines,LogicalSourceDescriptor(windTurbines, ))\n    "
                "SOURCE(76,solarPanels,LogicalSourceDescriptor(solarPanels, ))\nSINK(78: {PrintSinkDescriptor()})\n  "
                "unionWith(77)\n    SOURCE(75,windTurbines,LogicalSourceDescriptor(windTurbines, ))\n    "
                "SOURCE(76,solarPanels,LogicalSourceDescriptor(solarPanels, ))\n")};
    }
    static auto createMixedContainmentCases() {
        return std::vector<HashSignatureBasedPartialQueryContainmentMergerTestEntry>{
            HashSignatureBasedPartialQueryContainmentMergerTestEntry(
                "TestMixedContainmentCases",
                R"(Query::from("windTurbines").map(Attribute("value") = 40).map(Attribute("value") = Attribute("value") + 10).sink(PrintSinkDescriptor::create());)",
                R"(Query::from("windTurbines").map(Attribute("value") = 40).sink(PrintSinkDescriptor::create());)",
                "SINK(82: {PrintSinkDescriptor()})\n  MAP(81)\n    MAP(84)\n      "
                "SOURCE(83,windTurbines,LogicalSourceDescriptor(windTurbines, ))\nSINK(85: {PrintSinkDescriptor()})\n  MAP(84)\n "
                "   SOURCE(83,windTurbines,LogicalSourceDescriptor(windTurbines, ))\n"),
            HashSignatureBasedPartialQueryContainmentMergerTestEntry(
                "TestMixedContainmentCases",
                R"(Query::from("windTurbines").filter(Attribute("value") < 40).window(TumblingWindow::of(EventTime(Attribute("ts")), Seconds(20))).apply(Sum(Attribute("value1"))).sink(PrintSinkDescriptor::create());)",
                R"(Query::from("windTurbines").window(TumblingWindow::of(EventTime(Attribute("ts")), Seconds(10))).apply(Sum(Attribute("value1"))).sink(PrintSinkDescriptor::create());)",
                "SINK(90: {PrintSinkDescriptor()})\n  WINDOW AGGREGATION(OP-89, Sum;)\n    WATERMARKASSIGNER(88)\n      "
                "FILTER(87)\n        SOURCE(86,windTurbines,LogicalSourceDescriptor(windTurbines, ))\nSINK(94: "
                "{PrintSinkDescriptor()})\n  WINDOW AGGREGATION(OP-93, Sum;)\n    WATERMARKASSIGNER(92)\n      "
                "SOURCE(86,windTurbines,LogicalSourceDescriptor(windTurbines, ))\n"),
            HashSignatureBasedPartialQueryContainmentMergerTestEntry(
                "TestMixedContainmentCases",
                R"(Query::from("windTurbines").filter(Attribute("value") < 5).joinWith(Query::from("solarPanels")).where(Attribute("id1")).equalsTo(Attribute("id")).window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))).sink(PrintSinkDescriptor::create());)",
                R"(Query::from("windTurbines").joinWith(Query::from("solarPanels")).where(Attribute("id1")).equalsTo(Attribute("id")).window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(20))).sink(PrintSinkDescriptor::create());)",
                "SINK(101: {PrintSinkDescriptor()})\n  Join(100)\n    WATERMARKASSIGNER(98)\n      FILTER(96)\n        "
                "SOURCE(95,windTurbines,LogicalSourceDescriptor(windTurbines, ))\n    WATERMARKASSIGNER(105)\n      "
                "SOURCE(103,solarPanels,LogicalSourceDescriptor(solarPanels, ))\nSINK(107: {PrintSinkDescriptor()})\n  "
                "Join(106)\n    WATERMARKASSIGNER(104)\n      SOURCE(95,windTurbines,LogicalSourceDescriptor(windTurbines, ))\n  "
                "  WATERMARKASSIGNER(105)\n      SOURCE(103,solarPanels,LogicalSourceDescriptor(solarPanels, ))\n"),
            HashSignatureBasedPartialQueryContainmentMergerTestEntry(
                "TestMixedContainmentCases",
                R"(Query::from("windTurbines").window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))).apply(Sum(Attribute("value"))).sink(PrintSinkDescriptor::create());)",
                R"(Query::from("windTurbines").window(SlidingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000), Milliseconds(10))).byKey(Attribute("id")).apply(Sum(Attribute("value"))).sink(PrintSinkDescriptor::create());)",
                "SINK(111: {PrintSinkDescriptor()})\n  WINDOW AGGREGATION(OP-110, Sum;)\n    WATERMARKASSIGNER(113)\n      "
                "SOURCE(112,windTurbines,LogicalSourceDescriptor(windTurbines, ))\nSINK(115: {PrintSinkDescriptor()})\n  WINDOW "
                "AGGREGATION(OP-114, Sum;)\n    WATERMARKASSIGNER(113)\n      "
                "SOURCE(112,windTurbines,LogicalSourceDescriptor(windTurbines, ))\n"),
            HashSignatureBasedPartialQueryContainmentMergerTestEntry(
                "TestMixedContainmentCases",
                R"(Query::from("windTurbines").window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))).apply(Sum(Attribute("value"))).window(TumblingWindow::of(EventTime(Attribute("start")), Milliseconds(10000))).apply(Min(Attribute("value"))).sink(PrintSinkDescriptor::create());)",
                R"(Query::from("windTurbines").window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))).apply(Sum(Attribute("value"))).sink(PrintSinkDescriptor::create());)",
                "SINK(121: {PrintSinkDescriptor()})\n  WINDOW AGGREGATION(OP-120, Min;)\n    WATERMARKASSIGNER(119)\n      "
                "WINDOW AGGREGATION(OP-118, Sum;)\n        WATERMARKASSIGNER(117)\n          "
                "SOURCE(116,windTurbines,LogicalSourceDescriptor(windTurbines, ))\nSINK(125: {PrintSinkDescriptor()})\n  WINDOW "
                "AGGREGATION(OP-118, Sum;)\n    WATERMARKASSIGNER(117)\n      "
                "SOURCE(116,windTurbines,LogicalSourceDescriptor(windTurbines, ))\n"),
            HashSignatureBasedPartialQueryContainmentMergerTestEntry(
                "TestMixedContainmentCases",
                R"(Query::from("windTurbines").window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))).apply(Sum(Attribute("value"))).window(TumblingWindow::of(EventTime(Attribute("start")), Milliseconds(10000))).apply(Min(Attribute("value"))).sink(PrintSinkDescriptor::create());)",
                R"(Query::from("windTurbines").window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(100))).apply(Sum(Attribute("value"))).window(TumblingWindow::of(EventTime(Attribute("start")), Milliseconds(1000))).apply(Max(Attribute("value"))).sink(PrintSinkDescriptor::create());)",
                "SINK(131: {PrintSinkDescriptor()})\n  WINDOW AGGREGATION(OP-130, Min;)\n    WATERMARKASSIGNER(129)\n      "
                "WINDOW AGGREGATION(OP-128, Sum;)\n        WATERMARKASSIGNER(133)\n          "
                "SOURCE(132,windTurbines,LogicalSourceDescriptor(windTurbines, ))\nSINK(137: {PrintSinkDescriptor()})\n  WINDOW "
                "AGGREGATION(OP-136, Max;)\n    WATERMARKASSIGNER(135)\n      WINDOW AGGREGATION(OP-134, Sum;)\n        "
                "WATERMARKASSIGNER(133)\n          SOURCE(132,windTurbines,LogicalSourceDescriptor(windTurbines, ))\n"),
            HashSignatureBasedPartialQueryContainmentMergerTestEntry(
                "TestMixedContainmentCases",
                R"(Query::from("windTurbines").unionWith(Query::from("solarPanels")).window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(10000))).apply(Avg(Attribute("value"))).sink(PrintSinkDescriptor::create());)",
                R"(Query::from("windTurbines").unionWith(Query::from("solarPanels")).window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(10000))).apply(Sum(Attribute("value"))).sink(PrintSinkDescriptor::create());)",
                "SINK(143: {PrintSinkDescriptor()})\n  WINDOW AGGREGATION(OP-142, Avg;)\n    WATERMARKASSIGNER(141)\n      "
                "unionWith(140)\n        SOURCE(138,windTurbines,LogicalSourceDescriptor(windTurbines, ))\n        "
                "SOURCE(139,solarPanels,LogicalSourceDescriptor(solarPanels, ))\nSINK(149: {PrintSinkDescriptor()})\n  WINDOW "
                "AGGREGATION(OP-148, Sum;)\n    WATERMARKASSIGNER(141)\n      unionWith(140)\n        "
                "SOURCE(138,windTurbines,LogicalSourceDescriptor(windTurbines, ))\n        "
                "SOURCE(139,solarPanels,LogicalSourceDescriptor(solarPanels, ))\n"),
            HashSignatureBasedPartialQueryContainmentMergerTestEntry(
                "TestMixedContainmentCases",
                R"(Query::from("windTurbines").unionWith(Query::from("solarPanels")).window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(10000))).apply(Sum(Attribute("value"))).filter(Attribute("value") == 1).sink(PrintSinkDescriptor::create());)",
                R"(Query::from("windTurbines").unionWith(Query::from("solarPanels")).window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(100))).apply(Sum(Attribute("value"))).sink(PrintSinkDescriptor::create());)",
                "SINK(156: {PrintSinkDescriptor()})\n  FILTER(155)\n    WINDOW AGGREGATION(OP-154, Sum;)\n      "
                "WATERMARKASSIGNER(153)\n        unionWith(152)\n          "
                "SOURCE(150,windTurbines,LogicalSourceDescriptor(windTurbines, ))\n          "
                "SOURCE(151,solarPanels,LogicalSourceDescriptor(solarPanels, ))\nSINK(162: {PrintSinkDescriptor()})\n  WINDOW "
                "AGGREGATION(OP-161, Sum;)\n    WATERMARKASSIGNER(153)\n      unionWith(152)\n        "
                "SOURCE(150,windTurbines,LogicalSourceDescriptor(windTurbines, ))\n        "
                "SOURCE(151,solarPanels,LogicalSourceDescriptor(solarPanels, ))\n"),
            HashSignatureBasedPartialQueryContainmentMergerTestEntry(
                "TestMixedContainmentCases",
                R"(Query::from("windTurbines").unionWith(Query::from("solarPanels")).window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(10000))).apply(Sum(Attribute("value"))).filter(Attribute("value") != 1).sink(PrintSinkDescriptor::create());)",
                R"(Query::from("windTurbines").filter(Attribute("value") == 1).unionWith(Query::from("solarPanels")).window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(10000))).apply(Sum(Attribute("value"))).sink(PrintSinkDescriptor::create());)",
                "SINK(169: {PrintSinkDescriptor()})\n  FILTER(168)\n    WINDOW AGGREGATION(OP-167, Sum;)\n      "
                "WATERMARKASSIGNER(166)\n        unionWith(165)\n          "
                "SOURCE(163,windTurbines,LogicalSourceDescriptor(windTurbines, ))\n          "
                "SOURCE(164,solarPanels,LogicalSourceDescriptor(solarPanels, ))\nSINK(176: {PrintSinkDescriptor()})\n  WINDOW "
                "AGGREGATION(OP-175, Sum;)\n    WATERMARKASSIGNER(174)\n      unionWith(173)\n        FILTER(171)\n          "
                "SOURCE(163,windTurbines,LogicalSourceDescriptor(windTurbines, ))\n        "
                "SOURCE(164,solarPanels,LogicalSourceDescriptor(solarPanels, ))\n"),
            HashSignatureBasedPartialQueryContainmentMergerTestEntry(
                "TestMixedContainmentCases",
                R"(Query::from("windTurbines").filter(Attribute("value1") < 3).unionWith(Query::from("solarPanels")).unionWith(Query::from("test")).filter(Attribute("value") == 1).sink(PrintSinkDescriptor::create());)",
                R"(Query::from("windTurbines").filter(Attribute("value1") < 3).unionWith(Query::from("solarPanels")).unionWith(Query::from("test")).sink(PrintSinkDescriptor::create());)",
                "SINK(184: {PrintSinkDescriptor()})\n  FILTER(183)\n    unionWith(182)\n      unionWith(180)\n        "
                "FILTER(178)\n          SOURCE(177,windTurbines,LogicalSourceDescriptor(windTurbines, ))\n        "
                "SOURCE(179,solarPanels,LogicalSourceDescriptor(solarPanels, ))\n      "
                "SOURCE(181,test,LogicalSourceDescriptor(test, ))\nSINK(191: {PrintSinkDescriptor()})\n  unionWith(190)\n    "
                "unionWith(188)\n      FILTER(186)\n        SOURCE(185,windTurbines,LogicalSourceDescriptor(windTurbines, ))\n   "
                "   SOURCE(187,solarPanels,LogicalSourceDescriptor(solarPanels, ))\n    "
                "SOURCE(189,test,LogicalSourceDescriptor(test, ))\n")};
    }
};

/**
 * @brief Test applying Z3SignatureBasedBottomUpQueryContainmentRuleTest on Global query plan
 */
TEST_P(HashSignatureBasedPartialQueryContainmentMergerRuleTest, DISABLED_testMergingContainmentQueries) {
    auto containmentCases = GetParam();
    for (const auto& containmentCase : containmentCases) {
        QueryPlanPtr queryPlanSQPQuery = syntacticQueryValidation->validate(containmentCase.leftQuery);
        QueryPlanPtr queryPlanNewQuery = syntacticQueryValidation->validate(containmentCase.rightQuery);
        SinkLogicalOperatorNodePtr sinkOperator1 = queryPlanSQPQuery->getSinkOperators()[0];
        QueryId queryId1 = PlanIdGenerator::getNextQueryId();
        queryPlanSQPQuery->setQueryId(queryId1);
        SinkLogicalOperatorNodePtr sinkOperator2 = queryPlanNewQuery->getSinkOperators()[0];
        QueryId queryId2 = PlanIdGenerator::getNextQueryId();
        queryPlanNewQuery->setQueryId(queryId2);

        auto typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);
        typeInferencePhase->execute(queryPlanSQPQuery);
        typeInferencePhase->execute(queryPlanNewQuery);

        z3::ContextPtr context = std::make_shared<z3::context>();
        auto z3InferencePhase = Optimizer::SignatureInferencePhase::create(
            context,
            Optimizer::QueryMergerRule::HashSignatureBasedPartialQueryContainmentMergerRule);
        z3InferencePhase->execute(queryPlanSQPQuery);
        z3InferencePhase->execute(queryPlanNewQuery);

        auto globalQueryPlan = GlobalQueryPlan::create();
        globalQueryPlan->addQueryPlan(queryPlanSQPQuery);
        globalQueryPlan->addQueryPlan(queryPlanNewQuery);

        //execute
        auto signatureBasedEqualQueryMergerRule = Optimizer::HashSignatureBasedPartialQueryContainmentMergerRule::create(context);
        signatureBasedEqualQueryMergerRule->apply(globalQueryPlan);

        //assert
        auto updatedSharedQMToDeploy = globalQueryPlan->getSharedQueryPlansToDeploy();
        EXPECT_TRUE(updatedSharedQMToDeploy.size() == 1);

        auto updatedSharedQueryPlan1 = updatedSharedQMToDeploy[0]->getQueryPlan();
        EXPECT_TRUE(updatedSharedQueryPlan1);

        //NES_INFO2(updatedSharedQueryPlan1->toString());

        //assert that the sink operators have same up-stream operator
        auto updatedRootOperators1 = updatedSharedQueryPlan1->getRootOperators();
        EXPECT_TRUE(updatedRootOperators1.size() == 2);

        // assert plans are equal
        EXPECT_EQ(containmentCase.mergedQueryPlan, updatedSharedQueryPlan1->toString());
    }
}

INSTANTIATE_TEST_CASE_P(
    testHashBasedMergingContainmentQueries,
    HashSignatureBasedPartialQueryContainmentMergerRuleTest,
    ::testing::Values(HashSignatureBasedPartialQueryContainmentMergerRuleTest::createEqualityCases(),
                      HashSignatureBasedPartialQueryContainmentMergerRuleTest::createMixedContainmentCases()),
    [](const testing::TestParamInfo<HashSignatureBasedPartialQueryContainmentMergerRuleTest::ParamType>& info) {
        std::string name = info.param.at(0).testType;
        return name;
    });
