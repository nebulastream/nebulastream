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
#include <BaseIntegrationTest.hpp>
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
#include <Operators/LogicalOperators/Watermarks/WatermarkAssignerLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Windows/WindowOperatorNode.hpp>
#include <Optimizer/Phases/SignatureInferencePhase.hpp>
#include <Optimizer/Phases/TypeInferencePhase.hpp>
#include <Optimizer/QueryMerger/Z3SignatureBasedBottomUpQueryContainmentRule.hpp>
#include <QueryValidation/SyntacticQueryValidation.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Plans/Global/Query/SharedQueryPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Plans/Utils/PlanIdGenerator.hpp>
#include <Services/QueryParsingService.hpp>
#include <Util/Logger/Logger.hpp>
#include <iostream>
#include <z3++.h>

using namespace NES;

class Z3SignatureBasedBottomUpQueryContainmentRuleTestEntry {

  public:
    Z3SignatureBasedBottomUpQueryContainmentRuleTestEntry(const std::string& testType,
                                                          const std::string& leftQuery,
                                                          const std::string& rightQuery,
                                                          const std::string& mergedQueryPlan)
        : testType(testType), leftQuery(leftQuery), rightQuery(rightQuery), mergedQueryPlan(mergedQueryPlan) {}

    std::string testType;
    std::string leftQuery;
    std::string rightQuery;
    std::string mergedQueryPlan;
};

class Z3SignatureBasedBottomUpQueryContainmentRuleTest
    : public Testing::BaseUnitTest,
      public testing::WithParamInterface<std::vector<Z3SignatureBasedBottomUpQueryContainmentRuleTestEntry>> {

  public:
    SchemaPtr schema;
    SchemaPtr schemaHouseholds;
    Catalogs::Source::SourceCatalogPtr sourceCatalog;
    std::shared_ptr<Catalogs::UDF::UDFCatalog> udfCatalog;
    Optimizer::SyntacticQueryValidationPtr syntacticQueryValidation;

    /* Will be called before all tests in this class are started. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("Z3SignatureBasedBottomUpQueryContainmentRuleTest.log", NES::LogLevel::LOG_TRACE);
        NES_INFO("Setup Z3SignatureBasedBottomUpQueryContainmentRuleTest test case.");
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
                               ->addField("value1", BasicType::FLOAT32)
                               ->addField("value2", BasicType::FLOAT64)
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
    void TearDown() override { NES_DEBUG("Tear down Z3SignatureBasedBottomUpQueryContainmentRuleTest test case."); }

    static auto createEqualityCases() {
        return std::vector<Z3SignatureBasedBottomUpQueryContainmentRuleTestEntry>{
            Z3SignatureBasedBottomUpQueryContainmentRuleTestEntry(
                "TestEquality",
                R"(Query::from("windTurbines").map(Attribute("value") = 40).filter(Attribute("id") < 45).filter(Attribute("id") < 45).filter(Attribute("id") < 45).filter(Attribute("id") < 45).sink(PrintSinkDescriptor::create());)",
                R"(Query::from("windTurbines").map(Attribute("value") = 40).filter(Attribute("id") < 45).filter(Attribute("id") < 45).filter(Attribute("id") < 45).filter(Attribute("id") < 45).sink(PrintSinkDescriptor::create());)",
                "SINK(7: {PrintSinkDescriptor()})\n"
                "  FILTER(6)\n"
                "    FILTER(5)\n"
                "      FILTER(4)\n"
                "        FILTER(3)\n"
                "          MAP(2)\n"
                "            SOURCE(1,windTurbines,LogicalSourceDescriptor(windTurbines, ))\n"
                "SINK(14: {PrintSinkDescriptor()})\n"
                "  FILTER(6)\n"
                "    FILTER(5)\n"
                "      FILTER(4)\n"
                "        FILTER(3)\n"
                "          MAP(2)\n"
                "            SOURCE(1,windTurbines,LogicalSourceDescriptor(windTurbines, ))\n"),
            Z3SignatureBasedBottomUpQueryContainmentRuleTestEntry(
                "TestEquality",
                R"(Query::from("windTurbines").map(Attribute("value") = 40).filter(Attribute("id") < 45).filter(Attribute("id") < 45).filter(Attribute("id") < 45).filter(Attribute("id") < 45).sink(PrintSinkDescriptor::create());)",
                R"(Query::from("windTurbines").map(Attribute("value") = 40).filter(Attribute("id") < 45).filter(Attribute("id") < 45).filter(Attribute("id") < 45).filter(Attribute("id") < 45).sink(PrintSinkDescriptor::create());)",
                "SINK(21: {PrintSinkDescriptor()})\n"
                "  FILTER(20)\n"
                "    FILTER(19)\n"
                "      FILTER(18)\n"
                "        FILTER(17)\n"
                "          MAP(16)\n"
                "            SOURCE(15,windTurbines,LogicalSourceDescriptor(windTurbines, ))\n"
                "SINK(28: {PrintSinkDescriptor()})\n"
                "  FILTER(20)\n"
                "    FILTER(19)\n"
                "      FILTER(18)\n"
                "        FILTER(17)\n"
                "          MAP(16)\n"
                "            SOURCE(15,windTurbines,LogicalSourceDescriptor(windTurbines, ))\n"),
            Z3SignatureBasedBottomUpQueryContainmentRuleTestEntry(
                "TestEquality",
                R"(Query::from("windTurbines").window(TumblingWindow::of(EventTime(Attribute("ts")), Seconds(10))).apply(Sum(Attribute("value1"))).sink(PrintSinkDescriptor::create());)",
                R"(Query::from("windTurbines").window(TumblingWindow::of(EventTime(Attribute("ts")), Seconds(10))).apply(Sum(Attribute("value1"))).sink(PrintSinkDescriptor::create());)",
                "SINK(32: {PrintSinkDescriptor()})\n"
                "  WINDOW AGGREGATION(OP-31, Sum;)\n"
                "    WATERMARKASSIGNER(30)\n"
                "      SOURCE(29,windTurbines,LogicalSourceDescriptor(windTurbines, ))\n"
                "SINK(36: {PrintSinkDescriptor()})\n"
                "  WINDOW AGGREGATION(OP-31, Sum;)\n"
                "    WATERMARKASSIGNER(30)\n"
                "      SOURCE(29,windTurbines,LogicalSourceDescriptor(windTurbines, ))\n"),
            Z3SignatureBasedBottomUpQueryContainmentRuleTestEntry(
                "TestEquality",
                R"(Query::from("windTurbines").joinWith(Query::from("solarPanels")).where(Attribute("id1")).equalsTo(Attribute("id")).window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))).sink(PrintSinkDescriptor::create());)",
                R"(Query::from("windTurbines").joinWith(Query::from("solarPanels")).where(Attribute("id1")).equalsTo(Attribute("id")).window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))).sink(PrintSinkDescriptor::create());)",
                "SINK(42: {PrintSinkDescriptor()})\n"
                "  Join(41)\n"
                "    WATERMARKASSIGNER(39)\n"
                "      SOURCE(37,windTurbines,LogicalSourceDescriptor(windTurbines, ))\n"
                "    WATERMARKASSIGNER(40)\n"
                "      SOURCE(38,solarPanels,LogicalSourceDescriptor(solarPanels, ))\n"
                "SINK(48: {PrintSinkDescriptor()})\n"
                "  Join(41)\n"
                "    WATERMARKASSIGNER(39)\n"
                "      SOURCE(37,windTurbines,LogicalSourceDescriptor(windTurbines, ))\n"
                "    WATERMARKASSIGNER(40)\n"
                "      SOURCE(38,solarPanels,LogicalSourceDescriptor(solarPanels, ))\n"),
            Z3SignatureBasedBottomUpQueryContainmentRuleTestEntry(
                "TestEquality",
                R"(Query::from("windTurbines").filter(Attribute("value") < 5).joinWith(Query::from("solarPanels")).where(Attribute("id1")).equalsTo(Attribute("id")).window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))).sink(PrintSinkDescriptor::create());)",
                R"(Query::from("windTurbines").joinWith(Query::from("solarPanels")).where(Attribute("id1")).equalsTo(Attribute("id")).window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))).filter(Attribute("value") < 5).sink(PrintSinkDescriptor::create());)",
                "SINK(55: {PrintSinkDescriptor()})\n  Join(54)\n    WATERMARKASSIGNER(52)\n      FILTER(50)\n        "
                "SOURCE(49,windTurbines,LogicalSourceDescriptor(windTurbines, ))\n    WATERMARKASSIGNER(53)\n      "
                "SOURCE(51,solarPanels,LogicalSourceDescriptor(solarPanels, ))\nSINK(62: {PrintSinkDescriptor()})\n  "
                "Join(54)\n    WATERMARKASSIGNER(52)\n      FILTER(50)\n        "
                "SOURCE(49,windTurbines,LogicalSourceDescriptor(windTurbines, ))\n    WATERMARKASSIGNER(53)\n      "
                "SOURCE(51,solarPanels,LogicalSourceDescriptor(solarPanels, ))\n"),
            Z3SignatureBasedBottomUpQueryContainmentRuleTestEntry(
                "TestEquality",
                R"(Query::from("windTurbines").joinWith(Query::from("solarPanels")).where(Attribute("id1")).equalsTo(Attribute("id")).window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))).sink(PrintSinkDescriptor::create());)",
                R"(Query::from("windTurbines").joinWith(Query::from("solarPanels")).where(Attribute("id1")).equalsTo(Attribute("id")).window(SlidingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000), Milliseconds(1000))).sink(PrintSinkDescriptor::create());)",
                "SINK(68: {PrintSinkDescriptor()})\n"
                "  Join(67)\n"
                "    WATERMARKASSIGNER(65)\n"
                "      SOURCE(63,windTurbines,LogicalSourceDescriptor(windTurbines, ))\n"
                "    WATERMARKASSIGNER(66)\n"
                "      SOURCE(64,solarPanels,LogicalSourceDescriptor(solarPanels, ))\n"
                "SINK(74: {PrintSinkDescriptor()})\n"
                "  Join(67)\n"
                "    WATERMARKASSIGNER(65)\n"
                "      SOURCE(63,windTurbines,LogicalSourceDescriptor(windTurbines, ))\n"
                "    WATERMARKASSIGNER(66)\n"
                "      SOURCE(64,solarPanels,LogicalSourceDescriptor(solarPanels, ))\n"),
            Z3SignatureBasedBottomUpQueryContainmentRuleTestEntry(
                "TestEquality",
                R"(Query::from("windTurbines").window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))).apply(Sum(Attribute("value"))).sink(PrintSinkDescriptor::create());)",
                R"(Query::from("windTurbines").window(SlidingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000), Milliseconds(1000))).apply(Sum(Attribute("value"))).sink(PrintSinkDescriptor::create());)",
                "SINK(78: {PrintSinkDescriptor()})\n"
                "  WINDOW AGGREGATION(OP-77, Sum;)\n"
                "    WATERMARKASSIGNER(76)\n"
                "      SOURCE(75,windTurbines,LogicalSourceDescriptor(windTurbines, ))\n"
                "SINK(82: {PrintSinkDescriptor()})\n"
                "  WINDOW AGGREGATION(OP-77, Sum;)\n"
                "    WATERMARKASSIGNER(76)\n"
                "      SOURCE(75,windTurbines,LogicalSourceDescriptor(windTurbines, ))\n"),
            Z3SignatureBasedBottomUpQueryContainmentRuleTestEntry(
                "TestEquality",
                R"(Query::from("windTurbines").window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))).apply(Count()).sink(PrintSinkDescriptor::create());)",
                R"(Query::from("windTurbines").window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))).apply(Count()).sink(PrintSinkDescriptor::create());)",
                "SINK(86: {PrintSinkDescriptor()})\n"
                "  WINDOW AGGREGATION(OP-85, Count;)\n"
                "    WATERMARKASSIGNER(84)\n"
                "      SOURCE(83,windTurbines,LogicalSourceDescriptor(windTurbines, ))\n"
                "SINK(90: {PrintSinkDescriptor()})\n"
                "  WINDOW AGGREGATION(OP-85, Count;)\n"
                "    WATERMARKASSIGNER(84)\n"
                "      SOURCE(83,windTurbines,LogicalSourceDescriptor(windTurbines, ))\n"),
            Z3SignatureBasedBottomUpQueryContainmentRuleTestEntry(
                "TestEquality",
                R"(Query::from("windTurbines").unionWith(Query::from("solarPanels")).sink(PrintSinkDescriptor::create());)",
                R"(Query::from("windTurbines").unionWith(Query::from("solarPanels")).sink(PrintSinkDescriptor::create());)",
                "SINK(94: {PrintSinkDescriptor()})\n"
                "  unionWith(93)\n"
                "    SOURCE(91,windTurbines,LogicalSourceDescriptor(windTurbines, ))\n"
                "    SOURCE(92,solarPanels,LogicalSourceDescriptor(solarPanels, ))\n"
                "SINK(98: {PrintSinkDescriptor()})\n"
                "  unionWith(93)\n"
                "    SOURCE(91,windTurbines,LogicalSourceDescriptor(windTurbines, ))\n"
                "    SOURCE(92,solarPanels,LogicalSourceDescriptor(solarPanels, ))\n"),
            Z3SignatureBasedBottomUpQueryContainmentRuleTestEntry("TestEquality",
                                                                  R"(Query::from("windTurbines").unionWith(Query::from("solarPanels")).filter(Attribute("value") > 4).project(Attribute("value"), Attribute("id1"), Attribute("value1"), Attribute("ts")).joinWith(Query::from("households").project(Attribute("value"), Attribute("id"), Attribute("value1"), Attribute("ts"))).where(Attribute("windTurbines$id1")).equalsTo(Attribute("households$id")).window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))).sink(PrintSinkDescriptor::create());)",
                                                                  R"(Query::from("windTurbines").unionWith(Query::from("solarPanels")).joinWith(Query::from("households")).where(Attribute("id1")).equalsTo(Attribute("id")).window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))).filter(Attribute("value") > 4).project(Attribute("windTurbines$value"), Attribute("windTurbines$id1"), Attribute("windTurbines$value1"), Attribute("windTurbines$ts"), Attribute("households$value"), Attribute("households$id"), Attribute("households$value1"), Attribute("households$ts")).sink(PrintSinkDescriptor::create());)",
                                                                  "SINK(109: {PrintSinkDescriptor()})\n"
                                                                  "  Join(108)\n"
                                                                  "    WATERMARKASSIGNER(106)\n"
                                                                  "      PROJECTION(103, schema=windTurbines$value:INTEGER "
                                                                  "windTurbines$id1:INTEGER windTurbines$value1:INTEGER "
                                                                  "windTurbines$ts:INTEGER )\n"
                                                                  "        FILTER(102)\n"
                                                                  "          unionWith(101)\n"
                                                                  "            "
                                                                  "SOURCE(99,windTurbines,LogicalSourceDescriptor("
                                                                  "windTurbines, ))\n"
                                                                  "            "
                                                                  "SOURCE(100,solarPanels,LogicalSourceDescriptor("
                                                                  "solarPanels, ))\n"
                                                                  "    WATERMARKASSIGNER(107)\n"
                                                                  "      PROJECTION(105, schema=households$value:INTEGER "
                                                                  "households$id:INTEGER households$value1:(Float) "
                                                                  "households$ts:INTEGER )\n"
                                                                  "        "
                                                                  "SOURCE(104,households,LogicalSourceDescriptor(households, "
                                                                  "))\n"
                                                                  "SINK(119: {PrintSinkDescriptor()})\n"
                                                                  "  Join(108)\n"
                                                                  "    WATERMARKASSIGNER(106)\n"
                                                                  "      PROJECTION(103, schema=windTurbines$value:INTEGER "
                                                                  "windTurbines$id1:INTEGER windTurbines$value1:INTEGER "
                                                                  "windTurbines$ts:INTEGER )\n"
                                                                  "        FILTER(102)\n"
                                                                  "          unionWith(101)\n"
                                                                  "            "
                                                                  "SOURCE(99,windTurbines,LogicalSourceDescriptor("
                                                                  "windTurbines, ))\n"
                                                                  "            "
                                                                  "SOURCE(100,solarPanels,LogicalSourceDescriptor("
                                                                  "solarPanels, ))\n"
                                                                  "    WATERMARKASSIGNER(107)\n"
                                                                  "      PROJECTION(105, schema=households$value:INTEGER "
                                                                  "households$id:INTEGER households$value1:(Float) "
                                                                  "households$ts:INTEGER )\n"
                                                                  "        "
                                                                  "SOURCE(104,households,LogicalSourceDescriptor(households, "
                                                                  "))\n"),
            Z3SignatureBasedBottomUpQueryContainmentRuleTestEntry(
                "TestEquality",
                R"(Query::from("windTurbines").unionWith(Query::from("solarPanels")).window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))).byKey(Attribute("id")).apply(Sum(Attribute("value"))).filter(Attribute("value") > 4).joinWith(Query::from("households").window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))).byKey(Attribute("id")).apply(Sum(Attribute("value")))).where(Attribute("windTurbines$id")).equalsTo(Attribute("households$id")).window(TumblingWindow::of(EventTime(Attribute("start")), Hours(1))).sink(PrintSinkDescriptor::create());)",
                R"(Query::from("windTurbines").unionWith(Query::from("solarPanels")).window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))).byKey(Attribute("id")).apply(Sum(Attribute("value"))).joinWith(Query::from("households").window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))).byKey(Attribute("id")).apply(Sum(Attribute("value")))).where(Attribute("id")).equalsTo(Attribute("id")).window(TumblingWindow::of(EventTime(Attribute("start")), Hours(1))).filter(Attribute("windTurbines$value") > 4).sink(PrintSinkDescriptor::create());)",
                "SINK(130: {PrintSinkDescriptor()})\n"
                "  Join(129)\n"
                "    FILTER(125)\n"
                "      WINDOW AGGREGATION(OP-124, Sum;)\n"
                "        WATERMARKASSIGNER(123)\n"
                "          unionWith(122)\n"
                "            SOURCE(120,windTurbines,LogicalSourceDescriptor(windTurbines, ))\n"
                "            SOURCE(121,solarPanels,LogicalSourceDescriptor(solarPanels, ))\n"
                "    WINDOW AGGREGATION(OP-128, Sum;)\n"
                "      WATERMARKASSIGNER(127)\n"
                "        SOURCE(126,households,LogicalSourceDescriptor(households, ))\n"
                "SINK(141: {PrintSinkDescriptor()})\n"
                "  Join(129)\n"
                "    FILTER(125)\n"
                "      WINDOW AGGREGATION(OP-124, Sum;)\n"
                "        WATERMARKASSIGNER(123)\n"
                "          unionWith(122)\n"
                "            SOURCE(120,windTurbines,LogicalSourceDescriptor(windTurbines, ))\n"
                "            SOURCE(121,solarPanels,LogicalSourceDescriptor(solarPanels, ))\n"
                "    WINDOW AGGREGATION(OP-128, Sum;)\n"
                "      WATERMARKASSIGNER(127)\n"
                "        SOURCE(126,households,LogicalSourceDescriptor(households, ))\n")};
    }
    static auto createMixedContainmentCases() {
        return std::
            vector<Z3SignatureBasedBottomUpQueryContainmentRuleTestEntry>{Z3SignatureBasedBottomUpQueryContainmentRuleTestEntry(
                                                                              "TestMixedContainmentCases",
                                                                              R"(Query::from("windTurbines").map(Attribute("value") = 40).map(Attribute("value") = Attribute("value") + 10).sink(PrintSinkDescriptor::create());)",
                                                                              R"(Query::from("windTurbines").map(Attribute("value") = 40).sink(PrintSinkDescriptor::create());)",
                                                                              "SINK(145: {PrintSinkDescriptor()})\n"
                                                                              "  MAP(144)\n"
                                                                              "    MAP(147)\n"
                                                                              "      "
                                                                              "SOURCE(146,windTurbines,LogicalSourceDescriptor("
                                                                              "windTurbines, ))\n"
                                                                              "SINK(148: {PrintSinkDescriptor()})\n"
                                                                              "  MAP(147)\n"
                                                                              "    "
                                                                              "SOURCE(146,windTurbines,LogicalSourceDescriptor("
                                                                              "windTurbines, ))\n"),
                                                                          Z3SignatureBasedBottomUpQueryContainmentRuleTestEntry(
                                                                              "TestMixedContainmentCases",
                                                                              R"(Query::from("windTurbines").filter(Attribute("value") < 40).window(TumblingWindow::of(EventTime(Attribute("ts")), Seconds(20))).apply(Sum(Attribute("value1"))).sink(PrintSinkDescriptor::create());)",
                                                                              R"(Query::from("windTurbines").window(TumblingWindow::of(EventTime(Attribute("ts")), Seconds(10))).apply(Sum(Attribute("value1"))).sink(PrintSinkDescriptor::create());)",
                                                                              "SINK(153: {PrintSinkDescriptor()})\n"
                                                                              "  WINDOW AGGREGATION(OP-152, Sum;)\n"
                                                                              "    WATERMARKASSIGNER(151)\n"
                                                                              "      FILTER(150)\n"
                                                                              "        "
                                                                              "SOURCE(154,windTurbines,LogicalSourceDescriptor("
                                                                              "windTurbines, ))\n"
                                                                              "SINK(157: {PrintSinkDescriptor()})\n"
                                                                              "  WINDOW AGGREGATION(OP-156, Sum;)\n"
                                                                              "    WATERMARKASSIGNER(155)\n"
                                                                              "      "
                                                                              "SOURCE(154,windTurbines,LogicalSourceDescriptor("
                                                                              "windTurbines, ))\n"),
                                                                          Z3SignatureBasedBottomUpQueryContainmentRuleTestEntry(
                                                                              "TestMixedContainmentCases",
                                                                              R"(Query::from("windTurbines").filter(Attribute("value") < 5).joinWith(Query::from("solarPanels")).where(Attribute("id1")).equalsTo(Attribute("id")).window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))).sink(PrintSinkDescriptor::create());)",
                                                                              R"(Query::from("windTurbines").joinWith(Query::from("solarPanels")).where(Attribute("id1")).equalsTo(Attribute("id")).window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(20))).sink(PrintSinkDescriptor::create());)",
                                                                              "SINK(164: {PrintSinkDescriptor()})\n"
                                                                              "  Join(163)\n"
                                                                              "    WATERMARKASSIGNER(161)\n"
                                                                              "      FILTER(159)\n"
                                                                              "        "
                                                                              "SOURCE(165,windTurbines,LogicalSourceDescriptor("
                                                                              "windTurbines, ))\n"
                                                                              "    WATERMARKASSIGNER(168)\n"
                                                                              "      "
                                                                              "SOURCE(166,solarPanels,LogicalSourceDescriptor("
                                                                              "solarPanels, ))\n"
                                                                              "SINK(170: {PrintSinkDescriptor()})\n"
                                                                              "  Join(169)\n"
                                                                              "    WATERMARKASSIGNER(167)\n"
                                                                              "      "
                                                                              "SOURCE(165,windTurbines,LogicalSourceDescriptor("
                                                                              "windTurbines, ))\n"
                                                                              "    WATERMARKASSIGNER(168)\n"
                                                                              "      "
                                                                              "SOURCE(166,solarPanels,LogicalSourceDescriptor("
                                                                              "solarPanels, ))\n"),
                                                                          Z3SignatureBasedBottomUpQueryContainmentRuleTestEntry(
                                                                              "TestMixedContainmentCases",
                                                                              R"(Query::from("windTurbines").window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))).apply(Sum(Attribute("value"))).sink(PrintSinkDescriptor::create());)",
                                                                              R"(Query::from("windTurbines").window(SlidingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000), Milliseconds(10))).byKey(Attribute("id")).apply(Sum(Attribute("value"))).sink(PrintSinkDescriptor::create());)",
                                                                              "SINK(174: {PrintSinkDescriptor()})\n"
                                                                              "  WINDOW AGGREGATION(OP-173, Sum;)\n"
                                                                              "    WATERMARKASSIGNER(176)\n"
                                                                              "      "
                                                                              "SOURCE(175,windTurbines,LogicalSourceDescriptor("
                                                                              "windTurbines, ))\n"
                                                                              "SINK(178: {PrintSinkDescriptor()})\n"
                                                                              "  WINDOW AGGREGATION(OP-177, Sum;)\n"
                                                                              "    WATERMARKASSIGNER(176)\n"
                                                                              "      "
                                                                              "SOURCE(175,windTurbines,LogicalSourceDescriptor("
                                                                              "windTurbines, ))\n"),
                                                                          Z3SignatureBasedBottomUpQueryContainmentRuleTestEntry(
                                                                              "TestMixedContainmentCases",
                                                                              R"(Query::from("windTurbines").window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))).apply(Sum(Attribute("value"))).window(TumblingWindow::of(EventTime(Attribute("start")), Milliseconds(10000))).apply(Min(Attribute("value"))).sink(PrintSinkDescriptor::create());)",
                                                                              R"(Query::from("windTurbines").window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))).apply(Sum(Attribute("value"))).sink(PrintSinkDescriptor::create());)",
                                                                              "SINK(184: {PrintSinkDescriptor()})\n"
                                                                              "  WINDOW AGGREGATION(OP-183, Min;)\n"
                                                                              "    WATERMARKASSIGNER(182)\n"
                                                                              "      WINDOW AGGREGATION(OP-187, Sum;)\n"
                                                                              "        WATERMARKASSIGNER(186)\n"
                                                                              "          "
                                                                              "SOURCE(185,windTurbines,LogicalSourceDescriptor("
                                                                              "windTurbines, ))\n"
                                                                              "SINK(188: {PrintSinkDescriptor()})\n"
                                                                              "  WINDOW AGGREGATION(OP-187, Sum;)\n"
                                                                              "    WATERMARKASSIGNER(186)\n"
                                                                              "      "
                                                                              "SOURCE(185,windTurbines,LogicalSourceDescriptor("
                                                                              "windTurbines, ))\n"),
                                                                          Z3SignatureBasedBottomUpQueryContainmentRuleTestEntry(
                                                                              "TestMixedContainmentCases",
                                                                              R"(Query::from("windTurbines").window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))).apply(Sum(Attribute("value"))).window(TumblingWindow::of(EventTime(Attribute("start")), Milliseconds(10000))).apply(Min(Attribute("value"))).sink(PrintSinkDescriptor::create());)",
                                                                              R"(Query::from("windTurbines").window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(100))).apply(Sum(Attribute("value"))).window(TumblingWindow::of(EventTime(Attribute("start")), Milliseconds(1000))).apply(Max(Attribute("value"))).sink(PrintSinkDescriptor::create());)",
                                                                              "SINK(194: {PrintSinkDescriptor()})\n"
                                                                              "  WINDOW AGGREGATION(OP-193, Min;)\n"
                                                                              "    WATERMARKASSIGNER(192)\n"
                                                                              "      WINDOW AGGREGATION(OP-191, Sum;)\n"
                                                                              "        WATERMARKASSIGNER(190)\n"
                                                                              "          WINDOW AGGREGATION(OP-197, Sum;)\n"
                                                                              "            WATERMARKASSIGNER(196)\n"
                                                                              "              "
                                                                              "SOURCE(195,windTurbines,LogicalSourceDescriptor("
                                                                              "windTurbines, ))\n"
                                                                              "SINK(200: {PrintSinkDescriptor()})\n"
                                                                              "  WINDOW AGGREGATION(OP-199, Max;)\n"
                                                                              "    WATERMARKASSIGNER(198)\n"
                                                                              "      WINDOW AGGREGATION(OP-197, Sum;)\n"
                                                                              "        WATERMARKASSIGNER(196)\n"
                                                                              "          "
                                                                              "SOURCE(195,windTurbines,LogicalSourceDescriptor("
                                                                              "windTurbines, ))\n"),
                                                                          Z3SignatureBasedBottomUpQueryContainmentRuleTestEntry(
                                                                              "TestMixedContainmentCases",
                                                                              R"(Query::from("windTurbines").unionWith(Query::from("solarPanels")).window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(10000))).apply(Avg(Attribute("value"))).sink(PrintSinkDescriptor::create());)",
                                                                              R"(Query::from("windTurbines").unionWith(Query::from("solarPanels")).window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(10000))).apply(Sum(Attribute("value"))).sink(PrintSinkDescriptor::create());)",
                                                                              "SINK(206: {PrintSinkDescriptor()})\n"
                                                                              "  WINDOW AGGREGATION(OP-205, Avg;)\n"
                                                                              "    WATERMARKASSIGNER(210)\n"
                                                                              "      unionWith(209)\n"
                                                                              "        "
                                                                              "SOURCE(207,windTurbines,LogicalSourceDescriptor("
                                                                              "windTurbines, ))\n"
                                                                              "        "
                                                                              "SOURCE(208,solarPanels,LogicalSourceDescriptor("
                                                                              "solarPanels, ))\n"
                                                                              "SINK(212: {PrintSinkDescriptor()})\n"
                                                                              "  WINDOW AGGREGATION(OP-211, Sum;)\n"
                                                                              "    WATERMARKASSIGNER(210)\n"
                                                                              "      unionWith(209)\n"
                                                                              "        "
                                                                              "SOURCE(207,windTurbines,LogicalSourceDescriptor("
                                                                              "windTurbines, ))\n"
                                                                              "        "
                                                                              "SOURCE(208,solarPanels,LogicalSourceDescriptor("
                                                                              "solarPanels, ))\n"),
                                                                          Z3SignatureBasedBottomUpQueryContainmentRuleTestEntry(
                                                                              "TestMixedContainmentCases",
                                                                              R"(Query::from("windTurbines").unionWith(Query::from("solarPanels")).window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(10000))).apply(Sum(Attribute("value"))).filter(Attribute("value") == 1).sink(PrintSinkDescriptor::create());)",
                                                                              R"(Query::from("windTurbines").unionWith(Query::from("solarPanels")).window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(100))).apply(Sum(Attribute("value"))).sink(PrintSinkDescriptor::create());)",
                                                                              "SINK(219: {PrintSinkDescriptor()})\n"
                                                                              "  FILTER(218)\n"
                                                                              "    WINDOW AGGREGATION(OP-217, Sum;)\n"
                                                                              "      WATERMARKASSIGNER(216)\n"
                                                                              "        WINDOW AGGREGATION(OP-224, Sum;)\n"
                                                                              "          WATERMARKASSIGNER(223)\n"
                                                                              "            unionWith(222)\n"
                                                                              "              "
                                                                              "SOURCE(220,windTurbines,LogicalSourceDescriptor("
                                                                              "windTurbines, ))\n"
                                                                              "              "
                                                                              "SOURCE(221,solarPanels,LogicalSourceDescriptor("
                                                                              "solarPanels, ))\n"
                                                                              "SINK(225: {PrintSinkDescriptor()})\n"
                                                                              "  WINDOW AGGREGATION(OP-224, Sum;)\n"
                                                                              "    WATERMARKASSIGNER(223)\n"
                                                                              "      unionWith(222)\n"
                                                                              "        "
                                                                              "SOURCE(220,windTurbines,LogicalSourceDescriptor("
                                                                              "windTurbines, ))\n"
                                                                              "        "
                                                                              "SOURCE(221,solarPanels,LogicalSourceDescriptor("
                                                                              "solarPanels, ))\n"),
                                                                          Z3SignatureBasedBottomUpQueryContainmentRuleTestEntry(
                                                                              "TestMixedContainmentCases",
                                                                              R"(Query::from("windTurbines").unionWith(Query::from("solarPanels")).window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(10000))).apply(Sum(Attribute("value"))).filter(Attribute("value") != 1).sink(PrintSinkDescriptor::create());)",
                                                                              R"(Query::from("windTurbines").filter(Attribute("value") == 1).unionWith(Query::from("solarPanels")).window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(10000))).apply(Sum(Attribute("value"))).sink(PrintSinkDescriptor::create());)",
                                                                              "SINK(232: {PrintSinkDescriptor()})\n"
                                                                              "  FILTER(231)\n"
                                                                              "    WINDOW AGGREGATION(OP-230, Sum;)\n"
                                                                              "      WATERMARKASSIGNER(229)\n"
                                                                              "        unionWith(228)\n"
                                                                              "          "
                                                                              "SOURCE(233,windTurbines,LogicalSourceDescriptor("
                                                                              "windTurbines, ))\n"
                                                                              "          "
                                                                              "SOURCE(235,solarPanels,LogicalSourceDescriptor("
                                                                              "solarPanels, ))\n"
                                                                              "SINK(239: {PrintSinkDescriptor()})\n"
                                                                              "  WINDOW AGGREGATION(OP-238, Sum;)\n"
                                                                              "    WATERMARKASSIGNER(237)\n"
                                                                              "      unionWith(236)\n"
                                                                              "        FILTER(234)\n"
                                                                              "          "
                                                                              "SOURCE(233,windTurbines,LogicalSourceDescriptor("
                                                                              "windTurbines, ))\n"
                                                                              "        "
                                                                              "SOURCE(235,solarPanels,LogicalSourceDescriptor("
                                                                              "solarPanels, ))\n"),
                                                                          Z3SignatureBasedBottomUpQueryContainmentRuleTestEntry(
                                                                              "TestMixedContainmentCases",
                                                                              R"(Query::from("windTurbines").filter(Attribute("value1") < 3).unionWith(Query::from("solarPanels")).unionWith(Query::from("test")).filter(Attribute("value") == 1).sink(PrintSinkDescriptor::create());)",
                                                                              R"(Query::from("windTurbines").filter(Attribute("value1") < 3).unionWith(Query::from("solarPanels")).unionWith(Query::from("test")).sink(PrintSinkDescriptor::create());)",
                                                                              "SINK(247: {PrintSinkDescriptor()})\n"
                                                                              "  FILTER(246)\n"
                                                                              "    unionWith(253)\n"
                                                                              "      unionWith(251)\n"
                                                                              "        FILTER(249)\n"
                                                                              "          "
                                                                              "SOURCE(248,windTurbines,LogicalSourceDescriptor("
                                                                              "windTurbines, ))\n"
                                                                              "        "
                                                                              "SOURCE(250,solarPanels,LogicalSourceDescriptor("
                                                                              "solarPanels, ))\n"
                                                                              "      "
                                                                              "SOURCE(252,test,LogicalSourceDescriptor(test, ))\n"
                                                                              "SINK(254: {PrintSinkDescriptor()})\n"
                                                                              "  unionWith(253)\n"
                                                                              "    unionWith(251)\n"
                                                                              "      FILTER(249)\n"
                                                                              "        "
                                                                              "SOURCE(248,windTurbines,LogicalSourceDescriptor("
                                                                              "windTurbines, ))\n"
                                                                              "      "
                                                                              "SOURCE(250,solarPanels,LogicalSourceDescriptor("
                                                                              "solarPanels, ))\n"
                                                                              "    SOURCE(252,test,LogicalSourceDescriptor(test, "
                                                                              "))\n"),
                                                                          Z3SignatureBasedBottomUpQueryContainmentRuleTestEntry(
                                                                              "TestMixedContainmentCases",
                                                                              R"(Query::from("windTurbines").unionWith(Query::from("solarPanels")).filter(Attribute("value1") < 3).unionWith(Query::from("test")).filter(Attribute("value") == 1).sink(PrintSinkDescriptor::create());)",
                                                                              R"(Query::from("windTurbines").unionWith(Query::from("solarPanels")).unionWith(Query::from("test")).filter(Attribute("value") == 1).sink(PrintSinkDescriptor::create());)",
                                                                              "SINK(262: {PrintSinkDescriptor()})\n"
                                                                              "  FILTER(261)\n"
                                                                              "    unionWith(260)\n"
                                                                              "      FILTER(258)\n"
                                                                              "        unionWith(265)\n"
                                                                              "          "
                                                                              "SOURCE(263,windTurbines,LogicalSourceDescriptor("
                                                                              "windTurbines, ))\n"
                                                                              "          "
                                                                              "SOURCE(264,solarPanels,LogicalSourceDescriptor("
                                                                              "solarPanels, ))\n"
                                                                              "      "
                                                                              "SOURCE(259,test,LogicalSourceDescriptor(test, ))\n"
                                                                              "SINK(269: {PrintSinkDescriptor()})\n"
                                                                              "  FILTER(268)\n"
                                                                              "    unionWith(267)\n"
                                                                              "      unionWith(265)\n"
                                                                              "        "
                                                                              "SOURCE(263,windTurbines,LogicalSourceDescriptor("
                                                                              "windTurbines, ))\n"
                                                                              "        "
                                                                              "SOURCE(264,solarPanels,LogicalSourceDescriptor("
                                                                              "solarPanels, ))\n"
                                                                              "      "
                                                                              "SOURCE(266,test,LogicalSourceDescriptor(test, "
                                                                              "))\n"),
                                                                          Z3SignatureBasedBottomUpQueryContainmentRuleTestEntry(
                                                                              "TestMixedContainmentCases",
                                                                              R"(Query::from("windTurbines").filter(Attribute("value") != 4).unionWith(Query::from("solarPanels")).joinWith(Query::from("households")).where(Attribute("id1")).equalsTo(Attribute("id")).window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))).sink(PrintSinkDescriptor::create());)",
                                                                              R"(Query::from("windTurbines").unionWith(Query::from("solarPanels")).joinWith(Query::from("households")).where(Attribute("id1")).equalsTo(Attribute("id")).window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))).filter(Attribute("value") > 4).sink(PrintSinkDescriptor::create());)",
                                                                              "SINK(278: {PrintSinkDescriptor()})\n"
                                                                              "  Join(277)\n"
                                                                              "    WATERMARKASSIGNER(275)\n"
                                                                              "      unionWith(273)\n"
                                                                              "        FILTER(271)\n"
                                                                              "          "
                                                                              "SOURCE(279,windTurbines,LogicalSourceDescriptor("
                                                                              "windTurbines, ))\n"
                                                                              "        "
                                                                              "SOURCE(280,solarPanels,LogicalSourceDescriptor("
                                                                              "solarPanels, ))\n"
                                                                              "    WATERMARKASSIGNER(284)\n"
                                                                              "      "
                                                                              "SOURCE(282,households,LogicalSourceDescriptor("
                                                                              "households, ))\n"
                                                                              "SINK(287: {PrintSinkDescriptor()})\n"
                                                                              "  FILTER(286)\n"
                                                                              "    Join(285)\n"
                                                                              "      WATERMARKASSIGNER(283)\n"
                                                                              "        unionWith(281)\n"
                                                                              "          "
                                                                              "SOURCE(279,windTurbines,LogicalSourceDescriptor("
                                                                              "windTurbines, ))\n"
                                                                              "          "
                                                                              "SOURCE(280,solarPanels,LogicalSourceDescriptor("
                                                                              "solarPanels, ))\n"
                                                                              "      WATERMARKASSIGNER(284)\n"
                                                                              "        "
                                                                              "SOURCE(282,households,LogicalSourceDescriptor("
                                                                              "households, ))\n"),
                                                                          Z3SignatureBasedBottomUpQueryContainmentRuleTestEntry(
                                                                              "TestMixedContainmentCases",
                                                                              R"(Query::from("windTurbines").unionWith(Query::from("solarPanels")).filter(Attribute("value") > 4).project(Attribute("value"), Attribute("id1"), Attribute("value1"), Attribute("ts")).joinWith(Query::from("households").project(Attribute("value"), Attribute("id"), Attribute("value1"), Attribute("ts"))).where(Attribute("windTurbines$id1")).equalsTo(Attribute("households$id")).window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))).sink(PrintSinkDescriptor::create());)",
                                                                              R"(Query::from("windTurbines").unionWith(Query::from("solarPanels")).joinWith(Query::from("households")).where(Attribute("id1")).equalsTo(Attribute("id")).window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))).filter(Attribute("value") > 4).project(Attribute("windTurbines$value"), Attribute("windTurbines$id1"), Attribute("windTurbines$value1"),Attribute("households$value"), Attribute("households$id"), Attribute("households$value1"),Attribute("windTurbines$ts"), Attribute("households$ts")).sink(PrintSinkDescriptor::create());)",
                                                                              "SINK(298: {PrintSinkDescriptor()})\n"
                                                                              "  Join(297)\n"
                                                                              "    WATERMARKASSIGNER(295)\n"
                                                                              "      PROJECTION(292, "
                                                                              "schema=windTurbines$value:INTEGER "
                                                                              "windTurbines$id1:INTEGER "
                                                                              "windTurbines$value1:INTEGER "
                                                                              "windTurbines$ts:INTEGER )\n"
                                                                              "        FILTER(291)\n"
                                                                              "          unionWith(301)\n"
                                                                              "            "
                                                                              "SOURCE(299,windTurbines,LogicalSourceDescriptor("
                                                                              "windTurbines, ))\n"
                                                                              "            "
                                                                              "SOURCE(300,solarPanels,LogicalSourceDescriptor("
                                                                              "solarPanels, ))\n"
                                                                              "    WATERMARKASSIGNER(296)\n"
                                                                              "      PROJECTION(294, "
                                                                              "schema=households$value:INTEGER "
                                                                              "households$id:INTEGER households$value1:(Float) "
                                                                              "households$ts:INTEGER )\n"
                                                                              "        "
                                                                              "SOURCE(302,households,LogicalSourceDescriptor("
                                                                              "households, ))\n"
                                                                              "SINK(308: {PrintSinkDescriptor()})\n"
                                                                              "  PROJECTION(307, "
                                                                              "schema=windTurbines$value:INTEGER "
                                                                              "windTurbines$id1:INTEGER "
                                                                              "windTurbines$value1:INTEGER "
                                                                              "households$value:INTEGER households$id:INTEGER "
                                                                              "households$value1:(Float) windTurbines$ts:INTEGER "
                                                                              "households$ts:INTEGER )\n"
                                                                              "    FILTER(306)\n"
                                                                              "      Join(305)\n"
                                                                              "        WATERMARKASSIGNER(303)\n"
                                                                              "          unionWith(301)\n"
                                                                              "            "
                                                                              "SOURCE(299,windTurbines,LogicalSourceDescriptor("
                                                                              "windTurbines, ))\n"
                                                                              "            "
                                                                              "SOURCE(300,solarPanels,LogicalSourceDescriptor("
                                                                              "solarPanels, ))\n"
                                                                              "        WATERMARKASSIGNER(304)\n"
                                                                              "          "
                                                                              "SOURCE(302,households,LogicalSourceDescriptor("
                                                                              "households, ))\n"),
                                                                          Z3SignatureBasedBottomUpQueryContainmentRuleTestEntry(
                                                                              "TestMixedContainmentCases",
                                                                              R"(Query::from("windTurbines").unionWith(Query::from("solarPanels")).filter(Attribute("value") > 4).joinWith(Query::from("households")).where(Attribute("windTurbines$id1")).equalsTo(Attribute("households$id")).window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(100))).sink(PrintSinkDescriptor::create());)",
                                                                              R"(Query::from("windTurbines").unionWith(Query::from("solarPanels")).joinWith(Query::from("households")).where(Attribute("id1")).equalsTo(Attribute("id")).window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))).filter(Attribute("value") > 4).sink(PrintSinkDescriptor::create());)",
                                                                              "SINK(317: {PrintSinkDescriptor()})\n"
                                                                              "  Join(316)\n"
                                                                              "    WATERMARKASSIGNER(314)\n"
                                                                              "      FILTER(312)\n"
                                                                              "        unionWith(320)\n"
                                                                              "          "
                                                                              "SOURCE(318,windTurbines,LogicalSourceDescriptor("
                                                                              "windTurbines, ))\n"
                                                                              "          "
                                                                              "SOURCE(319,solarPanels,LogicalSourceDescriptor("
                                                                              "solarPanels, ))\n"
                                                                              "    WATERMARKASSIGNER(323)\n"
                                                                              "      "
                                                                              "SOURCE(321,households,LogicalSourceDescriptor("
                                                                              "households, ))\n"
                                                                              "SINK(326: {PrintSinkDescriptor()})\n"
                                                                              "  FILTER(325)\n"
                                                                              "    Join(324)\n"
                                                                              "      WATERMARKASSIGNER(322)\n"
                                                                              "        unionWith(320)\n"
                                                                              "          "
                                                                              "SOURCE(318,windTurbines,LogicalSourceDescriptor("
                                                                              "windTurbines, ))\n"
                                                                              "          "
                                                                              "SOURCE(319,solarPanels,LogicalSourceDescriptor("
                                                                              "solarPanels, ))\n"
                                                                              "      WATERMARKASSIGNER(323)\n"
                                                                              "        "
                                                                              "SOURCE(321,households,LogicalSourceDescriptor("
                                                                              "households, ))\n"),
                                                                          //Limit of our algorithm, cannot detect equivalence among these windows
                                                                          Z3SignatureBasedBottomUpQueryContainmentRuleTestEntry("TestMixedContainmentCases",
                                                                                                                                R"(Query::from("windTurbines").unionWith(Query::from("solarPanels")).filter(Attribute("value") > 4).joinWith(Query::from("households")).where(Attribute("windTurbines$id")).equalsTo(Attribute("households$id")).window(TumblingWindow::of(EventTime(Attribute("ts")), Hours(1))).window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))).byKey(Attribute("id")).apply(Sum(Attribute("value"))).sink(PrintSinkDescriptor::create());)",
                                                                                                                                R"(Query::from("windTurbines").unionWith(Query::from("solarPanels")).window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))).byKey(Attribute("id")).apply(Sum(Attribute("value"))).joinWith(Query::from("households").window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))).byKey(Attribute("id")).apply(Sum(Attribute("value")))).where(Attribute("id")).equalsTo(Attribute("id")).window(TumblingWindow::of(EventTime(Attribute("start")), Hours(1))).filter(Attribute("windTurbines$value") > 4).sink(PrintSinkDescriptor::create());)",
                                                                                                                                "SINK(337: {PrintSinkDescriptor()})\n"
                                                                                                                                "  WINDOW AGGREGATION(OP-336, Sum;)\n"
                                                                                                                                "    WATERMARKASSIGNER(335)\n"
                                                                                                                                "      Join(334)\n"
                                                                                                                                "        WATERMARKASSIGNER(332)\n"
                                                                                                                                "          FILTER(330)\n"
                                                                                                                                "            unionWith(340)\n"
                                                                                                                                "              SOURCE(338,windTurbines,LogicalSourceDescriptor(windTurbines, ))\n"
                                                                                                                                "              SOURCE(339,solarPanels,LogicalSourceDescriptor(solarPanels, ))\n"
                                                                                                                                "        WATERMARKASSIGNER(344)\n"
                                                                                                                                "          SOURCE(343,households,LogicalSourceDescriptor(households, ))\n"
                                                                                                                                "SINK(348: {PrintSinkDescriptor()})\n"
                                                                                                                                "  FILTER(347)\n"
                                                                                                                                "    Join(346)\n"
                                                                                                                                "      WINDOW AGGREGATION(OP-342, Sum;)\n"
                                                                                                                                "        WATERMARKASSIGNER(341)\n"
                                                                                                                                "          unionWith(340)\n"
                                                                                                                                "            SOURCE(338,windTurbines,LogicalSourceDescriptor(windTurbines, ))\n"
                                                                                                                                "            SOURCE(339,solarPanels,LogicalSourceDescriptor(solarPanels, ))\n"
                                                                                                                                "      WINDOW AGGREGATION(OP-345, Sum;)\n"
                                                                                                                                "        WATERMARKASSIGNER(344)\n"
                                                                                                                                "          SOURCE(343,households,LogicalSourceDescriptor(households, ))\n"),
                                                                          Z3SignatureBasedBottomUpQueryContainmentRuleTestEntry(
                                                                              "TestMixedContainmentCases",
                                                                              R"(Query::from("windTurbines").unionWith(Query::from("solarPanels")).filter(Attribute("value") > 4).joinWith(Query::from("households")).where(Attribute("windTurbines$id1")).equalsTo(Attribute("households$id")).window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))).sink(PrintSinkDescriptor::create());)",
                                                                              R"(Query::from("windTurbines").unionWith(Query::from("solarPanels")).joinWith(Query::from("households")).where(Attribute("id1")).equalsTo(Attribute("id")).window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))).project(Attribute("windTurbines$value")).sink(PrintSinkDescriptor::create());)",
                                                                              "SINK(357: {PrintSinkDescriptor()})\n"
                                                                              "  Join(356)\n"
                                                                              "    WATERMARKASSIGNER(354)\n"
                                                                              "      FILTER(352)\n"
                                                                              "        unionWith(360)\n"
                                                                              "          "
                                                                              "SOURCE(358,windTurbines,LogicalSourceDescriptor("
                                                                              "windTurbines, ))\n"
                                                                              "          "
                                                                              "SOURCE(359,solarPanels,LogicalSourceDescriptor("
                                                                              "solarPanels, ))\n"
                                                                              "    WATERMARKASSIGNER(363)\n"
                                                                              "      "
                                                                              "SOURCE(361,households,LogicalSourceDescriptor("
                                                                              "households, ))\n"
                                                                              "SINK(366: {PrintSinkDescriptor()})\n"
                                                                              "  PROJECTION(365, "
                                                                              "schema=windTurbines$value:INTEGER )\n"
                                                                              "    Join(364)\n"
                                                                              "      WATERMARKASSIGNER(362)\n"
                                                                              "        unionWith(360)\n"
                                                                              "          "
                                                                              "SOURCE(358,windTurbines,LogicalSourceDescriptor("
                                                                              "windTurbines, ))\n"
                                                                              "          "
                                                                              "SOURCE(359,solarPanels,LogicalSourceDescriptor("
                                                                              "solarPanels, ))\n"
                                                                              "      WATERMARKASSIGNER(363)\n"
                                                                              "        "
                                                                              "SOURCE(361,households,LogicalSourceDescriptor("
                                                                              "households, ))\n"),
                                                                          Z3SignatureBasedBottomUpQueryContainmentRuleTestEntry(
                                                                              "TestMixedContainmentCases",
                                                                              R"(Query::from("windTurbines").filter(Attribute("value") < 10).unionWith(Query::from("solarPanels").filter(Attribute("value") < 10)).sink(PrintSinkDescriptor::create());)",
                                                                              R"(Query::from("windTurbines").filter(Attribute("value") < 20).unionWith(Query::from("solarPanels").filter(Attribute("value") < 20)).sink(PrintSinkDescriptor::create());)",
                                                                              "SINK(372: {PrintSinkDescriptor()})\n"
                                                                              "  unionWith(371)\n"
                                                                              "    FILTER(368)\n"
                                                                              "      FILTER(374)\n"
                                                                              "        "
                                                                              "SOURCE(373,windTurbines,LogicalSourceDescriptor("
                                                                              "windTurbines, ))\n"
                                                                              "    FILTER(370)\n"
                                                                              "      FILTER(376)\n"
                                                                              "        "
                                                                              "SOURCE(375,solarPanels,LogicalSourceDescriptor("
                                                                              "solarPanels, ))\n"
                                                                              "SINK(378: {PrintSinkDescriptor()})\n"
                                                                              "  unionWith(377)\n"
                                                                              "    FILTER(374)\n"
                                                                              "      "
                                                                              "SOURCE(373,windTurbines,LogicalSourceDescriptor("
                                                                              "windTurbines, ))\n"
                                                                              "    FILTER(376)\n"
                                                                              "      "
                                                                              "SOURCE(375,solarPanels,LogicalSourceDescriptor("
                                                                              "solarPanels, ))\n"),
                                                                          Z3SignatureBasedBottomUpQueryContainmentRuleTestEntry(
                                                                              "TestMixedContainmentCases",
                                                                              R"(Query::from("windTurbines").unionWith(Query::from("solarPanels")).filter(Attribute("value") > 4).joinWith(Query::from("households")).where(Attribute("windTurbines$id1")).equalsTo(Attribute("households$id")).window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(100))).sink(PrintSinkDescriptor::create());)",
                                                                              R"(Query::from("windTurbines").unionWith(Query::from("solarPanels")).joinWith(Query::from("households")).where(Attribute("id1")).equalsTo(Attribute("id")).window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))).filter(Attribute("windTurbines$value") > 4).sink(PrintSinkDescriptor::create());)",
                                                                              "SINK(387: {PrintSinkDescriptor()})\n"
                                                                              "  Join(386)\n"
                                                                              "    WATERMARKASSIGNER(384)\n"
                                                                              "      FILTER(382)\n"
                                                                              "        unionWith(390)\n"
                                                                              "          "
                                                                              "SOURCE(388,windTurbines,LogicalSourceDescriptor("
                                                                              "windTurbines, ))\n"
                                                                              "          "
                                                                              "SOURCE(389,solarPanels,LogicalSourceDescriptor("
                                                                              "solarPanels, ))\n"
                                                                              "    WATERMARKASSIGNER(393)\n"
                                                                              "      "
                                                                              "SOURCE(391,households,LogicalSourceDescriptor("
                                                                              "households, ))\n"
                                                                              "SINK(396: {PrintSinkDescriptor()})\n"
                                                                              "  FILTER(395)\n"
                                                                              "    Join(394)\n"
                                                                              "      WATERMARKASSIGNER(392)\n"
                                                                              "        unionWith(390)\n"
                                                                              "          "
                                                                              "SOURCE(388,windTurbines,LogicalSourceDescriptor("
                                                                              "windTurbines, ))\n"
                                                                              "          "
                                                                              "SOURCE(389,solarPanels,LogicalSourceDescriptor("
                                                                              "solarPanels, ))\n"
                                                                              "      WATERMARKASSIGNER(393)\n"
                                                                              "        "
                                                                              "SOURCE(391,households,LogicalSourceDescriptor("
                                                                              "households, ))\n"),
                                                                          //projection conditions differ too much for containment to be picked up in the complete query
                                                                          //bottom up approach would detect it, though
                                                                          Z3SignatureBasedBottomUpQueryContainmentRuleTestEntry(
                                                                              "TestMixedContainmentCases",
                                                                              R"(Query::from("windTurbines").unionWith(Query::from("solarPanels")).joinWith(Query::from("households")).where(Attribute("windTurbines$id1")).equalsTo(Attribute("households$id")).window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))).sink(PrintSinkDescriptor::create());)",
                                                                              R"(Query::from("windTurbines").unionWith(Query::from("solarPanels")).joinWith(Query::from("households")).where(Attribute("id1")).equalsTo(Attribute("id")).window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))).window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))).byKey(Attribute("id")).apply(Sum(Attribute("value"))).sink(PrintSinkDescriptor::create());)",
                                                                              "SINK(404: {PrintSinkDescriptor()})\n"
                                                                              "  Join(411)\n"
                                                                              "    WATERMARKASSIGNER(409)\n"
                                                                              "      unionWith(407)\n"
                                                                              "        "
                                                                              "SOURCE(405,windTurbines,LogicalSourceDescriptor("
                                                                              "windTurbines, ))\n"
                                                                              "        "
                                                                              "SOURCE(406,solarPanels,LogicalSourceDescriptor("
                                                                              "solarPanels, ))\n"
                                                                              "    WATERMARKASSIGNER(410)\n"
                                                                              "      "
                                                                              "SOURCE(408,households,LogicalSourceDescriptor("
                                                                              "households, ))\n"
                                                                              "SINK(414: {PrintSinkDescriptor()})\n"
                                                                              "  WINDOW AGGREGATION(OP-413, Sum;)\n"
                                                                              "    WATERMARKASSIGNER(412)\n"
                                                                              "      Join(411)\n"
                                                                              "        WATERMARKASSIGNER(409)\n"
                                                                              "          unionWith(407)\n"
                                                                              "            "
                                                                              "SOURCE(405,windTurbines,LogicalSourceDescriptor("
                                                                              "windTurbines, ))\n"
                                                                              "            "
                                                                              "SOURCE(406,solarPanels,LogicalSourceDescriptor("
                                                                              "solarPanels, ))\n"
                                                                              "        WATERMARKASSIGNER(410)\n"
                                                                              "          "
                                                                              "SOURCE(408,households,LogicalSourceDescriptor("
                                                                              "households, ))\n"),
                                                                          Z3SignatureBasedBottomUpQueryContainmentRuleTestEntry(
                                                                              "TestMixedContainmentCases",
                                                                              R"(Query::from("windTurbines").unionWith(Query::from("solarPanels")).sink(PrintSinkDescriptor::create());)",
                                                                              R"(Query::from("windTurbines").unionWith(Query::from("solarPanels")).window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))).byKey(Attribute("id")).apply(Sum(Attribute("value"))).sink(PrintSinkDescriptor::create());)",
                                                                              "SINK(418: {PrintSinkDescriptor()})\n"
                                                                              "  unionWith(417)\n"
                                                                              "    "
                                                                              "SOURCE(415,windTurbines,LogicalSourceDescriptor("
                                                                              "windTurbines, ))\n"
                                                                              "    "
                                                                              "SOURCE(416,solarPanels,LogicalSourceDescriptor("
                                                                              "solarPanels, ))\n"
                                                                              "SINK(424: {PrintSinkDescriptor()})\n"
                                                                              "  WINDOW AGGREGATION(OP-423, Sum;)\n"
                                                                              "    WATERMARKASSIGNER(422)\n"
                                                                              "      unionWith(417)\n"
                                                                              "        "
                                                                              "SOURCE(415,windTurbines,LogicalSourceDescriptor("
                                                                              "windTurbines, ))\n"
                                                                              "        "
                                                                              "SOURCE(416,solarPanels,LogicalSourceDescriptor("
                                                                              "solarPanels, ))\n"),
                                                                          Z3SignatureBasedBottomUpQueryContainmentRuleTestEntry(
                                                                              "TestMixedContainmentCases",
                                                                              R"(Query::from("windTurbines").unionWith(Query::from("solarPanels")).window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(10))).byKey(Attribute("id")).apply(Sum(Attribute("value"))).sink(PrintSinkDescriptor::create());)",
                                                                              R"(Query::from("windTurbines").unionWith(Query::from("solarPanels")).window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(10))).byKey(Attribute("id")).apply(Sum(Attribute("value"))).window(TumblingWindow::of(EventTime(Attribute("start")), Milliseconds(1000))).byKey(Attribute("id")).apply(Min(Attribute("value"))).sink(PrintSinkDescriptor::create());)",
                                                                              "SINK(430: {PrintSinkDescriptor()})\n"
                                                                              "  WINDOW AGGREGATION(OP-429, Sum;)\n"
                                                                              "    WATERMARKASSIGNER(428)\n"
                                                                              "      unionWith(427)\n"
                                                                              "        "
                                                                              "SOURCE(425,windTurbines,LogicalSourceDescriptor("
                                                                              "windTurbines, ))\n"
                                                                              "        "
                                                                              "SOURCE(426,solarPanels,LogicalSourceDescriptor("
                                                                              "solarPanels, ))\n"
                                                                              "SINK(438: {PrintSinkDescriptor()})\n"
                                                                              "  WINDOW AGGREGATION(OP-437, Min;)\n"
                                                                              "    WATERMARKASSIGNER(436)\n"
                                                                              "      WINDOW AGGREGATION(OP-429, Sum;)\n"
                                                                              "        WATERMARKASSIGNER(428)\n"
                                                                              "          unionWith(427)\n"
                                                                              "            "
                                                                              "SOURCE(425,windTurbines,LogicalSourceDescriptor("
                                                                              "windTurbines, ))\n"
                                                                              "            "
                                                                              "SOURCE(426,solarPanels,LogicalSourceDescriptor("
                                                                              "solarPanels, ))\n"),
                                                                          Z3SignatureBasedBottomUpQueryContainmentRuleTestEntry(
                                                                              "TestWindowContainment",
                                                                              R"(Query::from("windTurbines").unionWith(Query::from("solarPanels")).window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))).byKey(Attribute("id")).apply(Sum(Attribute("value"))).sink(PrintSinkDescriptor::create());)",
                                                                              R"(Query::from("windTurbines").unionWith(Query::from("solarPanels")).window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(100))).byKey(Attribute("id")).apply(Sum(Attribute("value")), Min(Attribute("value1")), Max(Attribute("value2"))).sink(PrintSinkDescriptor::create());)",
                                                                              "SINK(444: {PrintSinkDescriptor()})\n"
                                                                              "  WINDOW AGGREGATION(OP-443, Sum;)\n"
                                                                              "    WATERMARKASSIGNER(442)\n"
                                                                              "      WINDOW AGGREGATION(OP-449, Sum;Min;Max;)\n"
                                                                              "        WATERMARKASSIGNER(448)\n"
                                                                              "          unionWith(447)\n"
                                                                              "            "
                                                                              "SOURCE(445,windTurbines,LogicalSourceDescriptor("
                                                                              "windTurbines, ))\n"
                                                                              "            "
                                                                              "SOURCE(446,solarPanels,LogicalSourceDescriptor("
                                                                              "solarPanels, ))\n"
                                                                              "SINK(450: {PrintSinkDescriptor()})\n"
                                                                              "  WINDOW AGGREGATION(OP-449, Sum;Min;Max;)\n"
                                                                              "    WATERMARKASSIGNER(448)\n"
                                                                              "      unionWith(447)\n"
                                                                              "        "
                                                                              "SOURCE(445,windTurbines,LogicalSourceDescriptor("
                                                                              "windTurbines, ))\n"
                                                                              "        "
                                                                              "SOURCE(446,solarPanels,LogicalSourceDescriptor("
                                                                              "solarPanels, ))\n"),
                                                                          Z3SignatureBasedBottomUpQueryContainmentRuleTestEntry(
                                                                              "TestFilterContainment",
                                                                              R"(Query::from("windTurbines").filter(Attribute("value") == 5).filter(Attribute("value1") == 8).sink(PrintSinkDescriptor::create());)",
                                                                              R"(Query::from("windTurbines").filter(Attribute("value") >= 4).filter(Attribute("value1") > 3).sink(PrintSinkDescriptor::create());)",
                                                                              "SINK(454: {PrintSinkDescriptor()})\n"
                                                                              "  FILTER(453)\n"
                                                                              "    FILTER(452)\n"
                                                                              "      FILTER(456)\n"
                                                                              "        "
                                                                              "SOURCE(455,windTurbines,LogicalSourceDescriptor("
                                                                              "windTurbines, ))\n"
                                                                              "SINK(458: {PrintSinkDescriptor()})\n"
                                                                              "  FILTER(457)\n"
                                                                              "    FILTER(456)\n"
                                                                              "      "
                                                                              "SOURCE(455,windTurbines,LogicalSourceDescriptor("
                                                                              "windTurbines, ))\n")};
    }

    static auto createProjectionContainmentCases() {
        return std::vector<Z3SignatureBasedBottomUpQueryContainmentRuleTestEntry>{
            Z3SignatureBasedBottomUpQueryContainmentRuleTestEntry(
                "TestProjectionContainment",
                R"(Query::from("windTurbines").map(Attribute("value") = 40).project(Attribute("value").as("newValue")).sink(PrintSinkDescriptor::create());)",
                R"(Query::from("windTurbines").map(Attribute("value") = 40).sink(PrintSinkDescriptor::create());)",
                "SINK(462: {PrintSinkDescriptor()})\n"
                "  PROJECTION(461, schema=windTurbines$newValue:INTEGER )\n"
                "    MAP(464)\n"
                "      SOURCE(463,windTurbines,LogicalSourceDescriptor(windTurbines, ))\n"
                "SINK(465: {PrintSinkDescriptor()})\n"
                "  MAP(464)\n"
                "    SOURCE(463,windTurbines,LogicalSourceDescriptor(windTurbines, ))\n"),
            Z3SignatureBasedBottomUpQueryContainmentRuleTestEntry(
                "TestProjectionContainment",
                R"(Query::from("windTurbines").unionWith(Query::from("solarPanels")).filter(Attribute("value") > 4).project(Attribute("value"), Attribute("id1"), Attribute("value1"), Attribute("ts")).joinWith(Query::from("households").project(Attribute("value"), Attribute("id"), Attribute("value1"), Attribute("ts"))).where(Attribute("windTurbines$id1")).equalsTo(Attribute("households$id")).window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))).sink(PrintSinkDescriptor::create());)",
                R"(Query::from("windTurbines").unionWith(Query::from("solarPanels")).joinWith(Query::from("households")).where(Attribute("id1")).equalsTo(Attribute("id")).window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))).filter(Attribute("value") > 4).project(Attribute("windTurbines$value"), Attribute("windTurbines$id1"), Attribute("households$value"), Attribute("households$id"), Attribute("windTurbines$ts"), Attribute("households$ts")).sink(PrintSinkDescriptor::create());)",
                "SINK(476: {PrintSinkDescriptor()})\n"
                "  Join(475)\n"
                "    WATERMARKASSIGNER(473)\n"
                "      PROJECTION(470, schema=windTurbines$value:INTEGER windTurbines$id1:INTEGER windTurbines$value1:INTEGER "
                "windTurbines$ts:INTEGER )\n"
                "        FILTER(469)\n"
                "          unionWith(479)\n"
                "            SOURCE(477,windTurbines,LogicalSourceDescriptor(windTurbines, ))\n"
                "            SOURCE(478,solarPanels,LogicalSourceDescriptor(solarPanels, ))\n"
                "    WATERMARKASSIGNER(474)\n"
                "      PROJECTION(472, schema=households$value:INTEGER households$id:INTEGER households$value1:(Float) "
                "households$ts:INTEGER )\n"
                "        SOURCE(480,households,LogicalSourceDescriptor(households, ))\n"
                "SINK(486: {PrintSinkDescriptor()})\n"
                "  PROJECTION(485, schema=windTurbines$value:INTEGER windTurbines$id1:INTEGER households$value:INTEGER "
                "households$id:INTEGER windTurbines$ts:INTEGER households$ts:INTEGER )\n"
                "    FILTER(484)\n"
                "      Join(483)\n"
                "        WATERMARKASSIGNER(481)\n"
                "          unionWith(479)\n"
                "            SOURCE(477,windTurbines,LogicalSourceDescriptor(windTurbines, ))\n"
                "            SOURCE(478,solarPanels,LogicalSourceDescriptor(solarPanels, ))\n"
                "        WATERMARKASSIGNER(482)\n"
                "          SOURCE(480,households,LogicalSourceDescriptor(households, ))\n"),
            Z3SignatureBasedBottomUpQueryContainmentRuleTestEntry(
                "TestProjectionContainment",
                R"(Query::from("windTurbines").unionWith(Query::from("solarPanels")).joinWith(Query::from("households")).where(Attribute("windTurbines$id1")).equalsTo(Attribute("households$id")).window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))).sink(PrintSinkDescriptor::create());)",
                R"(Query::from("windTurbines").unionWith(Query::from("solarPanels")).joinWith(Query::from("households")).where(Attribute("id1")).equalsTo(Attribute("id")).window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))).project(Attribute("windTurbines$value")).sink(PrintSinkDescriptor::create());)",
                "SINK(494: {PrintSinkDescriptor()})\n"
                "  Join(493)\n"
                "    WATERMARKASSIGNER(491)\n"
                "      unionWith(489)\n"
                "        SOURCE(487,windTurbines,LogicalSourceDescriptor(windTurbines, ))\n"
                "        SOURCE(488,solarPanels,LogicalSourceDescriptor(solarPanels, ))\n"
                "    WATERMARKASSIGNER(492)\n"
                "      SOURCE(490,households,LogicalSourceDescriptor(households, ))\n"
                "SINK(503: {PrintSinkDescriptor()})\n"
                "  PROJECTION(502, schema=windTurbines$value:INTEGER )\n"
                "    Join(493)\n"
                "      WATERMARKASSIGNER(491)\n"
                "        unionWith(489)\n"
                "          SOURCE(487,windTurbines,LogicalSourceDescriptor(windTurbines, ))\n"
                "          SOURCE(488,solarPanels,LogicalSourceDescriptor(solarPanels, ))\n"
                "      WATERMARKASSIGNER(492)\n"
                "        SOURCE(490,households,LogicalSourceDescriptor(households, ))\n")};
    }
};

/**
 * @brief Test applying Z3SignatureBasedBottomUpQueryContainmentRuleTest on Global query plan
 */
TEST_P(Z3SignatureBasedBottomUpQueryContainmentRuleTest, DISABLED_testMergingContainmentQueries) {
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
        auto z3InferencePhase =
            Optimizer::SignatureInferencePhase::create(context,
                                                       Optimizer::QueryMergerRule::Z3SignatureBasedBottomUpQueryContainmentRule);
        z3InferencePhase->execute(queryPlanSQPQuery);
        z3InferencePhase->execute(queryPlanNewQuery);

        auto globalQueryPlan = GlobalQueryPlan::create();
        globalQueryPlan->addQueryPlan(queryPlanSQPQuery);
        globalQueryPlan->addQueryPlan(queryPlanNewQuery);

        //execute
        auto signatureBasedEqualQueryMergerRule = Optimizer::Z3SignatureBasedBottomUpQueryContainmentRule::create(context, true);
        signatureBasedEqualQueryMergerRule->apply(globalQueryPlan);

        //assert
        auto updatedSharedQMToDeploy = globalQueryPlan->getSharedQueryPlansToDeploy();
        EXPECT_TRUE(updatedSharedQMToDeploy.size() == 1);

        auto updatedSharedQueryPlan1 = updatedSharedQMToDeploy[0]->getQueryPlan();
        EXPECT_TRUE(updatedSharedQueryPlan1);

        NES_INFO("{}", updatedSharedQueryPlan1->toString());

        //assert that the sink operators have same up-stream operator
        auto updatedRootOperators1 = updatedSharedQueryPlan1->getRootOperators();
        EXPECT_TRUE(updatedRootOperators1.size() == 2);

        // assert plans are equal
        EXPECT_EQ(containmentCase.mergedQueryPlan, updatedSharedQueryPlan1->toString());
    }
}

INSTANTIATE_TEST_CASE_P(testMergingContainmentQueries,
                        Z3SignatureBasedBottomUpQueryContainmentRuleTest,
                        ::testing::Values(Z3SignatureBasedBottomUpQueryContainmentRuleTest::createEqualityCases(),
                                          Z3SignatureBasedBottomUpQueryContainmentRuleTest::createMixedContainmentCases(),
                                          Z3SignatureBasedBottomUpQueryContainmentRuleTest::createProjectionContainmentCases()),
                        [](const testing::TestParamInfo<Z3SignatureBasedBottomUpQueryContainmentRuleTest::ParamType>& info) {
                            std::string name = info.param.at(0).testType;
                            return name;
                        });
