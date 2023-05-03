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
#include <Optimizer/QueryMerger/Z3SignatureBasedBottomUpQueryContainmentRule.hpp>
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
    : public Testing::TestWithErrorHandling<testing::Test>,
      public testing::WithParamInterface<std::vector<Z3SignatureBasedBottomUpQueryContainmentRuleTestEntry>> {

  public:
    SchemaPtr schema;
    SchemaPtr schemaHouseholds;
    Catalogs::Source::SourceCatalogPtr sourceCatalog;
    std::shared_ptr<Catalogs::UDF::UDFCatalog> udfCatalog;
    Optimizer::SyntacticQueryValidationPtr syntacticQueryValidation;

    /* Will be called before all tests in this class are started. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("Z3SignatureBasedBottomUpQueryContainmentRuleTest.log", NES::LogLevel::LOG_DEBUG);
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
        sourceCatalog->addLogicalSource("households", schemaHouseholds);
        udfCatalog = Catalogs::UDF::UDFCatalog::create();
        auto cppCompiler = Compiler::CPPCompiler::create();
        auto jitCompiler = Compiler::JITCompilerBuilder().registerLanguageCompiler(cppCompiler).build();
        syntacticQueryValidation = Optimizer::SyntacticQueryValidation::create(QueryParsingService::create(jitCompiler));
    }

    /* Will be called after a test is executed. */
    void TearDown() override { NES_DEBUG("Tear down Z3SignatureBasedBottomUpQueryContainmentRuleTest test case."); }

    static auto createEqualityCases() {
        return std::
            vector<Z3SignatureBasedBottomUpQueryContainmentRuleTestEntry>{
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
                    R"(Query::from("windTurbines").map(Attribute("value") = 40).filter(Attribute("id") < 45).filter(Attribute("id") < 45))"
                    R"(.filter(Attribute("id") < 45).filter(Attribute("id") < 45).sink(PrintSinkDescriptor::create());)",
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
    static auto createNoContainmentCases() {
        return std::vector<Z3SignatureBasedBottomUpQueryContainmentRuleTestEntry>{
            Z3SignatureBasedBottomUpQueryContainmentRuleTestEntry(
                "TestNoContainment",
                R"(Query::from("windTurbines").map(Attribute("value") = 40).map(Attribute("value") = Attribute("value") + 10).sink(PrintSinkDescriptor::create());)",
                R"(Query::from("windTurbines").map(Attribute("value") = 40).sink(PrintSinkDescriptor::create());)",
                "SINK(145: {PrintSinkDescriptor()})\n"
                "  MAP(144)\n"
                "    MAP(147)\n"
                "      SOURCE(146,windTurbines,LogicalSourceDescriptor(windTurbines, ))\n"
                "SINK(148: {PrintSinkDescriptor()})\n"
                "  MAP(147)\n"
                "    SOURCE(146,windTurbines,LogicalSourceDescriptor(windTurbines, ))\n"),
            Z3SignatureBasedBottomUpQueryContainmentRuleTestEntry(
                "TestNoContainment",
                R"(Query::from("windTurbines").filter(Attribute("value") < 40).window(TumblingWindow::of(EventTime(Attribute("ts")), Seconds(20))).apply(Sum(Attribute("value1"))).sink(PrintSinkDescriptor::create());)",
                R"(Query::from("windTurbines").window(TumblingWindow::of(EventTime(Attribute("ts")), Seconds(10))).apply(Sum(Attribute("value1"))).sink(PrintSinkDescriptor::create());)",
                "SINK(153: {PrintSinkDescriptor()})\n"
                "  WINDOW AGGREGATION(OP-152, Sum;)\n"
                "    WATERMARKASSIGNER(151)\n"
                "      FILTER(150)\n"
                "        SOURCE(154,windTurbines,LogicalSourceDescriptor(windTurbines, ))\n"
                "SINK(157: {PrintSinkDescriptor()})\n"
                "  WINDOW AGGREGATION(OP-156, Sum;)\n"
                "    WATERMARKASSIGNER(155)\n"
                "      SOURCE(154,windTurbines,LogicalSourceDescriptor(windTurbines, ))\n"),
            Z3SignatureBasedBottomUpQueryContainmentRuleTestEntry(
                "TestNoContainment",
                R"(Query::from("windTurbines").joinWith(Query::from("solarPanels")).where(Attribute("id1")).equalsTo(Attribute("id")).window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))).sink(PrintSinkDescriptor::create());)",
                R"(Query::from("windTurbines").joinWith(Query::from("solarPanels")).where(Attribute("id1")).equalsTo(Attribute("id")).window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(20))).filter(Attribute("value") < 10).sink(PrintSinkDescriptor::create());)",
                "SINK(163: {PrintSinkDescriptor()})\n"
                "  Join(162)\n"
                "    WATERMARKASSIGNER(167)\n"
                "      SOURCE(165,solarPanels,LogicalSourceDescriptor(solarPanels, ))\n"
                "    WATERMARKASSIGNER(166)\n"
                "      SOURCE(164,windTurbines,LogicalSourceDescriptor(windTurbines, ))\n"
                "SINK(170: {PrintSinkDescriptor()})\n"
                "  FILTER(169)\n"
                "    Join(168)\n"
                "      WATERMARKASSIGNER(166)\n"
                "        SOURCE(164,windTurbines,LogicalSourceDescriptor(windTurbines, ))\n"
                "      WATERMARKASSIGNER(167)\n"
                "        SOURCE(165,solarPanels,LogicalSourceDescriptor(solarPanels, ))\n"),
            Z3SignatureBasedBottomUpQueryContainmentRuleTestEntry(
                "TestNoContainment",
                R"(Query::from("windTurbines").filter(Attribute("value") < 5).joinWith(Query::from("solarPanels")).where(Attribute("id1")).equalsTo(Attribute("id")).window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))).sink(PrintSinkDescriptor::create());)",
                R"(Query::from("windTurbines").joinWith(Query::from("solarPanels")).where(Attribute("id1")).equalsTo(Attribute("id")).window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(20))).sink(PrintSinkDescriptor::create());)",
                "SINK(177: {PrintSinkDescriptor()})\n"
                "  Join(176)\n"
                "    WATERMARKASSIGNER(174)\n"
                "      FILTER(172)\n"
                "        SOURCE(178,windTurbines,LogicalSourceDescriptor(windTurbines, ))\n"
                "    WATERMARKASSIGNER(181)\n"
                "      SOURCE(179,solarPanels,LogicalSourceDescriptor(solarPanels, ))\n"
                "SINK(183: {PrintSinkDescriptor()})\n"
                "  Join(182)\n"
                "    WATERMARKASSIGNER(180)\n"
                "      SOURCE(178,windTurbines,LogicalSourceDescriptor(windTurbines, ))\n"
                "    WATERMARKASSIGNER(181)\n"
                "      SOURCE(179,solarPanels,LogicalSourceDescriptor(solarPanels, ))\n"),
            Z3SignatureBasedBottomUpQueryContainmentRuleTestEntry(
                "TestNoContainment",
                R"(Query::from("windTurbines").window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))).apply(Sum(Attribute("value"))).sink(PrintSinkDescriptor::create());)",
                R"(Query::from("windTurbines").window(SlidingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000), Milliseconds(10))).byKey(Attribute("id")).apply(Sum(Attribute("value"))).sink(PrintSinkDescriptor::create());)",
                "SINK(187: {PrintSinkDescriptor()})\n"
                "  WINDOW AGGREGATION(OP-186, Sum;)\n"
                "    WATERMARKASSIGNER(189)\n"
                "      SOURCE(188,windTurbines,LogicalSourceDescriptor(windTurbines, ))\n"
                "SINK(191: {PrintSinkDescriptor()})\n"
                "  WINDOW AGGREGATION(OP-190, Sum;)\n"
                "    WATERMARKASSIGNER(189)\n"
                "      SOURCE(188,windTurbines,LogicalSourceDescriptor(windTurbines, ))\n"),
            Z3SignatureBasedBottomUpQueryContainmentRuleTestEntry(
                "TestNoContainment",
                R"(Query::from("windTurbines").window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))).apply(Sum(Attribute("value"))).window(TumblingWindow::of(EventTime(Attribute("start")), Milliseconds(10000))).apply(Min(Attribute("value"))).sink(PrintSinkDescriptor::create());)",
                R"(Query::from("windTurbines").window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))).apply(Sum(Attribute("value"))).sink(PrintSinkDescriptor::create());)",
                "SINK(197: {PrintSinkDescriptor()})\n"
                "  WINDOW AGGREGATION(OP-196, Min;)\n"
                "    WATERMARKASSIGNER(195)\n"
                "      WINDOW AGGREGATION(OP-200, Sum;)\n"
                "        WATERMARKASSIGNER(199)\n"
                "          SOURCE(198,windTurbines,LogicalSourceDescriptor(windTurbines, ))\n"
                "SINK(201: {PrintSinkDescriptor()})\n"
                "  WINDOW AGGREGATION(OP-200, Sum;)\n"
                "    WATERMARKASSIGNER(199)\n"
                "      SOURCE(198,windTurbines,LogicalSourceDescriptor(windTurbines, ))\n"),
            Z3SignatureBasedBottomUpQueryContainmentRuleTestEntry(
                "TestNoContainment",
                R"(Query::from("windTurbines").window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))).apply(Sum(Attribute("value"))).window(TumblingWindow::of(EventTime(Attribute("start")), Milliseconds(10000))).apply(Min(Attribute("value"))).sink(PrintSinkDescriptor::create());)",
                R"(Query::from("windTurbines").window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(100))).apply(Sum(Attribute("value"))).window(TumblingWindow::of(EventTime(Attribute("start")), Milliseconds(1000))).apply(Max(Attribute("value"))).sink(PrintSinkDescriptor::create());)",
                "SINK(207: {PrintSinkDescriptor()})\n"
                "  WINDOW AGGREGATION(OP-206, Min;)\n"
                "    WATERMARKASSIGNER(205)\n"
                "      WINDOW AGGREGATION(OP-204, Sum;)\n"
                "        WATERMARKASSIGNER(203)\n"
                "          WINDOW AGGREGATION(OP-210, Sum;)\n"
                "            WATERMARKASSIGNER(209)\n"
                "              SOURCE(208,windTurbines,LogicalSourceDescriptor(windTurbines, ))\n"
                "SINK(213: {PrintSinkDescriptor()})\n"
                "  WINDOW AGGREGATION(OP-212, Max;)\n"
                "    WATERMARKASSIGNER(211)\n"
                "      WINDOW AGGREGATION(OP-210, Sum;)\n"
                "        WATERMARKASSIGNER(209)\n"
                "          SOURCE(208,windTurbines,LogicalSourceDescriptor(windTurbines, ))\n"),
            Z3SignatureBasedBottomUpQueryContainmentRuleTestEntry(
                "TestNoContainment",
                R"(Query::from("windTurbines").unionWith(Query::from("solarPanels")).window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(10000))).apply(Avg(Attribute("value"))).sink(PrintSinkDescriptor::create());)",
                R"(Query::from("windTurbines").unionWith(Query::from("solarPanels")).window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(10000))).apply(Sum(Attribute("value"))).sink(PrintSinkDescriptor::create());)",
                "SINK(219: {PrintSinkDescriptor()})\n"
                "  WINDOW AGGREGATION(OP-218, Avg;)\n"
                "    WATERMARKASSIGNER(223)\n"
                "      unionWith(222)\n"
                "        SOURCE(220,windTurbines,LogicalSourceDescriptor(windTurbines, ))\n"
                "        SOURCE(221,solarPanels,LogicalSourceDescriptor(solarPanels, ))\n"
                "SINK(225: {PrintSinkDescriptor()})\n"
                "  WINDOW AGGREGATION(OP-224, Sum;)\n"
                "    WATERMARKASSIGNER(223)\n"
                "      unionWith(222)\n"
                "        SOURCE(220,windTurbines,LogicalSourceDescriptor(windTurbines, ))\n"
                "        SOURCE(221,solarPanels,LogicalSourceDescriptor(solarPanels, ))\n"),
            Z3SignatureBasedBottomUpQueryContainmentRuleTestEntry(
                "TestNoContainment",
                R"(Query::from("windTurbines").unionWith(Query::from("solarPanels")).window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(10000))).apply(Sum(Attribute("value"))).filter(Attribute("value") == 1).sink(PrintSinkDescriptor::create());)",
                R"(Query::from("windTurbines").unionWith(Query::from("solarPanels")).window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(100))).apply(Sum(Attribute("value"))).sink(PrintSinkDescriptor::create());)",
                "SINK(232: {PrintSinkDescriptor()})\n"
                "  FILTER(231)\n"
                "    WINDOW AGGREGATION(OP-230, Sum;)\n"
                "      WATERMARKASSIGNER(229)\n"
                "        WINDOW AGGREGATION(OP-237, Sum;)\n"
                "          WATERMARKASSIGNER(236)\n"
                "            unionWith(235)\n"
                "              SOURCE(233,windTurbines,LogicalSourceDescriptor(windTurbines, ))\n"
                "              SOURCE(234,solarPanels,LogicalSourceDescriptor(solarPanels, ))\n"
                "SINK(238: {PrintSinkDescriptor()})\n"
                "  WINDOW AGGREGATION(OP-237, Sum;)\n"
                "    WATERMARKASSIGNER(236)\n"
                "      unionWith(235)\n"
                "        SOURCE(233,windTurbines,LogicalSourceDescriptor(windTurbines, ))\n"
                "        SOURCE(234,solarPanels,LogicalSourceDescriptor(solarPanels, ))\n"),
            //todo: queries like this are a problem currently as the wrong operations are merged. And the filter operation is lost.
            //todo: this happens, when there is a filter before a join or a unionWith. The containment relationship happens when the union or join is present but the actually contained operation is lost.
            Z3SignatureBasedBottomUpQueryContainmentRuleTestEntry(
                "TestNoContainment",
                R"(Query::from("windTurbines").unionWith(Query::from("solarPanels")).window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(10000))).apply(Sum(Attribute("value"))).filter(Attribute("value") != 1).sink(PrintSinkDescriptor::create());)",
                R"(Query::from("windTurbines").filter(Attribute("value") == 1).unionWith(Query::from("solarPanels")).window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(10000))).apply(Sum(Attribute("value"))).sink(PrintSinkDescriptor::create());)",
                "")/*,
            Z3SignatureBasedBottomUpQueryContainmentRuleTestEntry(
                "TestNoContainment",
                R"(Query::from("windTurbines").filter(Attribute("value") == 5).unionWith(Query::from("solarPanels")).joinWith(Query::from("households")).where(Attribute("id1")).equalsTo(Attribute("id")).window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))).sink(PrintSinkDescriptor::create());)",
                R"(Query::from("windTurbines").joinWith(Query::from("households")).where(Attribute("id1")).equalsTo(Attribute("id")).window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))).filter(Attribute("value") != 4).sink(PrintSinkDescriptor::create());)",
                ""),
            Z3SignatureBasedBottomUpQueryContainmentRuleTestEntry("TestNoContainment", R"(Query::from("windTurbines").filter(Attribute("value") != 4).unionWith(Query::from("solarPanels")).joinWith(Query::from("households")).where(Attribute("id1")).equalsTo(Attribute("id")).window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))).sink(PrintSinkDescriptor::create());)", R"(Query::from("windTurbines").unionWith(Query::from("solarPanels")).joinWith(Query::from("households")).where(Attribute("id1")).equalsTo(Attribute("id")).window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))).filter(Attribute("value") > 4).sink(PrintSinkDescriptor::create());)", ""),
            Z3SignatureBasedBottomUpQueryContainmentRuleTestEntry("TestNoContainment", R"(Query::from("windTurbines").unionWith(Query::from("solarPanels")).filter(Attribute("value") > 4).project(Attribute("value"), Attribute("id1"), Attribute("value1"), Attribute("ts")).joinWith(Query::from("households").project(Attribute("value"), Attribute("id"), Attribute("value1"), Attribute("ts"))).where(Attribute("windTurbines$id1")).equalsTo(Attribute("households$id")).window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))).sink(PrintSinkDescriptor::create());)", R"(Query::from("windTurbines").unionWith(Query::from("solarPanels")).joinWith(Query::from("households")).where(Attribute("id1")).equalsTo(Attribute("id")).window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))).filter(Attribute("value") > 4).project(Attribute("windTurbines$value"), Attribute("windTurbines$id1"), Attribute("windTurbines$value1"),Attribute("households$value"), Attribute("households$id"), Attribute("households$value1"),Attribute("windTurbines$ts"), Attribute("households$ts")).sink(PrintSinkDescriptor::create());)", ""),
            Z3SignatureBasedBottomUpQueryContainmentRuleTestEntry(
                "TestNoContainment",
                R"(Query::from("windTurbines").unionWith(Query::from("solarPanels")).filter(Attribute("value") > 4).joinWith(Query::from("households")).where(Attribute("windTurbines$id1")).equalsTo(Attribute("households$id")).window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(100))).sink(PrintSinkDescriptor::create());)",
                R"(Query::from("windTurbines").unionWith(Query::from("solarPanels")).joinWith(Query::from("households")).where(Attribute("id1")).equalsTo(Attribute("id")).window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))).filter(Attribute("value") > 4).sink(PrintSinkDescriptor::create());)",
                ""),
            //Limit of our algorithm, cannot detect equivalence among these windows
            Z3SignatureBasedBottomUpQueryContainmentRuleTestEntry(
                "TestNoContainment",
                R"(Query::from("windTurbines").unionWith(Query::from("solarPanels")).filter(Attribute("value") > 4).joinWith(Query::from("households")).where(Attribute("windTurbines$id")).equalsTo(Attribute("households$id")).window(TumblingWindow::of(EventTime(Attribute("ts")), Hours(1))).window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))).byKey(Attribute("id")).apply(Sum(Attribute("value"))).sink(PrintSinkDescriptor::create());)",
                R"(Query::from("windTurbines").unionWith(Query::from("solarPanels")).window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))).byKey(Attribute("id")).apply(Sum(Attribute("value"))).joinWith(Query::from("households").window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))).byKey(Attribute("id")).apply(Sum(Attribute("value")))).where(Attribute("id")).equalsTo(Attribute("id")).window(TumblingWindow::of(EventTime(Attribute("start")), Hours(1))).filter(Attribute("windTurbines$value") > 4).sink(PrintSinkDescriptor::create());)",
                ""),
            Z3SignatureBasedBottomUpQueryContainmentRuleTestEntry(
                "TestNoContainment",
                R"(Query::from("windTurbines").unionWith(Query::from("solarPanels")).filter(Attribute("value") > 4).joinWith(Query::from("households")).where(Attribute("windTurbines$id1")).equalsTo(Attribute("households$id")).window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))).sink(PrintSinkDescriptor::create());)",
                R"(Query::from("windTurbines").unionWith(Query::from("solarPanels")).joinWith(Query::from("households")).where(Attribute("id1")).equalsTo(Attribute("id")).window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))).project(Attribute("windTurbines$value")).sink(PrintSinkDescriptor::create());)",
                ""),
            Z3SignatureBasedBottomUpQueryContainmentRuleTestEntry("TestNoContainment", R"(Query::from("windTurbines").unionWith(Query::from("solarPanels")).joinWith(Query::from("households")).where(Attribute("windTurbines$id1")).equalsTo(Attribute("households$id")).window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(10000))).sink(PrintSinkDescriptor::create());)", R"(Query::from("windTurbines").unionWith(Query::from("solarPanels")).joinWith(Query::from("households")).where(Attribute("id1")).equalsTo(Attribute("id")).window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))).project(Attribute("windTurbines$value")).sink(PrintSinkDescriptor::create());)", ""),
            Z3SignatureBasedBottomUpQueryContainmentRuleTestEntry(
                "TestNoContainment",
                R"(Query::from("windTurbines").joinWith(Query::from("solarPanels")).where(Attribute("id1")).equalsTo(Attribute("id")).window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(10))).sink(PrintSinkDescriptor::create());)",
                R"(Query::from("windTurbines").joinWith(Query::from("solarPanels")).where(Attribute("id1")).equalsTo(Attribute("id")).window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))).sink(PrintSinkDescriptor::create());)",
                ""),
            Z3SignatureBasedBottomUpQueryContainmentRuleTestEntry(
                "TestNoContainment",
                R"(Query::from("windTurbines").joinWith(Query::from("solarPanels")).where(Attribute("id1")).equalsTo(Attribute("id")).window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))).sink(PrintSinkDescriptor::create());)",
                R"(Query::from("windTurbines").joinWith(Query::from("solarPanels")).where(Attribute("id1")).equalsTo(Attribute("id")).window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(20))).sink(PrintSinkDescriptor::create());)",
                ""),
            Z3SignatureBasedBottomUpQueryContainmentRuleTestEntry("TestNoContainment", R"(Query::from("windTurbines").unionWith(Query::from("solarPanels")).filter(Attribute("value") > 4).joinWith(Query::from("households")).where(Attribute("windTurbines$id1")).equalsTo(Attribute("households$id")).window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(100))).sink(PrintSinkDescriptor::create());)", R"(Query::from("windTurbines").unionWith(Query::from("solarPanels")).joinWith(Query::from("households")).where(Attribute("id1")).equalsTo(Attribute("id")).window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))).filter(Attribute("windTurbines$value") > 4).sink(PrintSinkDescriptor::create());)", ""),
            //projection conditions differ too much for containment to be picked up in the complete query
            //bottom up approach would detect it, though
            Z3SignatureBasedBottomUpQueryContainmentRuleTestEntry(
                "TestNoContainment",
                R"(Query::from("windTurbines").unionWith(Query::from("solarPanels")).joinWith(Query::from("households")).where(Attribute("windTurbines$id1")).equalsTo(Attribute("households$id")).window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))).sink(PrintSinkDescriptor::create());)",
                R"(Query::from("windTurbines").unionWith(Query::from("solarPanels")).joinWith(Query::from("households")).where(Attribute("id1")).equalsTo(Attribute("id")).window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))).window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))).byKey(Attribute("id")).apply(Sum(Attribute("value"))).sink(PrintSinkDescriptor::create());)",
                ""),
            //another limit of our algorithm. If one query does not have a window, we cannot detect containment
            Z3SignatureBasedBottomUpQueryContainmentRuleTestEntry(
                "TestNoContainment",
                R"(Query::from("windTurbines").unionWith(Query::from("solarPanels")).sink(PrintSinkDescriptor::create());)",
                R"(Query::from("windTurbines").unionWith(Query::from("solarPanels")).window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))).byKey(Attribute("id")).apply(Sum(Attribute("value"))).sink(PrintSinkDescriptor::create());)",
                ""),
            Z3SignatureBasedBottomUpQueryContainmentRuleTestEntry("TestNoContainment",
                                                                  R"(Query::from("windTurbines").unionWith(Query::from("solarPanels")).window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(10))).byKey(Attribute("id")).apply(Sum(Attribute("value"))).sink(PrintSinkDescriptor::create());)",
                                                                  R"(Query::from("windTurbines").unionWith(Query::from("solarPanels")).window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(10))).byKey(Attribute("id")).apply(Sum(Attribute("value"))).window(TumblingWindow::of(EventTime(Attribute("start")), Milliseconds(1000))).byKey(Attribute("id")).apply(Min(Attribute("value"))).sink(PrintSinkDescriptor::create());)",
                                                                  "")*/};
    }

    static auto createFilterContainmentCases() {
        return std::vector<Z3SignatureBasedBottomUpQueryContainmentRuleTestEntry>{
            Z3SignatureBasedBottomUpQueryContainmentRuleTestEntry(
                "TestFilterContainment",
                R"(Query::from("windTurbines").map(Attribute("value") = 40).filter(Attribute("id") < 45).sink(PrintSinkDescriptor::create());)",
                R"(Query::from("windTurbines").map(Attribute("value") = 40).filter(Attribute("id") < 60).sink(PrintSinkDescriptor::create());)",
                ""),
            Z3SignatureBasedBottomUpQueryContainmentRuleTestEntry(
                "TestFilterContainment",
                R"(Query::from("windTurbines").map(Attribute("value") = 40).filter(Attribute("id") < 60).sink(PrintSinkDescriptor::create());)",
                R"(Query::from("windTurbines").map(Attribute("value") = 40).filter(Attribute("id") < 45).sink(PrintSinkDescriptor::create());)",
                ""),
            Z3SignatureBasedBottomUpQueryContainmentRuleTestEntry(
                "TestFilterContainment",
                R"(Query::from("windTurbines").joinWith(Query::from("solarPanels")).where(Attribute("id1")).equalsTo(Attribute("id")).window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))).sink(PrintSinkDescriptor::create());)",
                R"(Query::from("windTurbines").joinWith(Query::from("solarPanels")).where(Attribute("id1")).equalsTo(Attribute("id")).window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))).filter(Attribute("value") < 5).sink(PrintSinkDescriptor::create());)",
                ""),
            Z3SignatureBasedBottomUpQueryContainmentRuleTestEntry(
                "TestFilterContainment",
                R"(Query::from("windTurbines").filter(Attribute("value") < 5).joinWith(Query::from("solarPanels")).where(Attribute("id1")).equalsTo(Attribute("id")).window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))).sink(PrintSinkDescriptor::create());)",
                R"(Query::from("windTurbines").joinWith(Query::from("solarPanels")).where(Attribute("id1")).equalsTo(Attribute("id")).window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))).sink(PrintSinkDescriptor::create());)",
                ""),
            Z3SignatureBasedBottomUpQueryContainmentRuleTestEntry(
                "TestFilterContainment",
                R"(Query::from("windTurbines").filter(Attribute("value") == 5).joinWith(Query::from("households")).where(Attribute("id1")).equalsTo(Attribute("id")).window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))).sink(PrintSinkDescriptor::create());)",
                R"(Query::from("windTurbines").joinWith(Query::from("households")).where(Attribute("id1")).equalsTo(Attribute("id")).window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))).filter(Attribute("value") != 4).sink(PrintSinkDescriptor::create());)",
                ""),
            Z3SignatureBasedBottomUpQueryContainmentRuleTestEntry(
                "TestFilterContainment",
                R"(Query::from("windTurbines").filter(Attribute("value") == 5).unionWith(Query::from("solarPanels")).sink(PrintSinkDescriptor::create());)",
                R"(Query::from("windTurbines").unionWith(Query::from("solarPanels")).filter(Attribute("value") >= 4).sink(PrintSinkDescriptor::create());)",
                ""),
            Z3SignatureBasedBottomUpQueryContainmentRuleTestEntry(
                "TestFilterContainment",
                R"(Query::from("windTurbines").unionWith(Query::from("solarPanels")).filter(Attribute("value") != 4).joinWith(Query::from("households")).where(Attribute("id1")).equalsTo(Attribute("id")).window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))).sink(PrintSinkDescriptor::create());)",
                R"(Query::from("windTurbines").unionWith(Query::from("solarPanels")).joinWith(Query::from("households")).where(Attribute("id1")).equalsTo(Attribute("id")).window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))).filter(Attribute("value") > 4).sink(PrintSinkDescriptor::create());)",
                "")};
    }

    static auto createProjectionContainmentCases() {
        return std::vector<Z3SignatureBasedBottomUpQueryContainmentRuleTestEntry>{
            Z3SignatureBasedBottomUpQueryContainmentRuleTestEntry(
                "TestNoContainment",
                R"(Query::from("windTurbines").map(Attribute("value") = 40).project(Attribute("value").as("newValue")).sink(PrintSinkDescriptor::create());)",
                R"(Query::from("windTurbines").map(Attribute("value") = 40).sink(PrintSinkDescriptor::create());)",
                "SINK(145: {PrintSinkDescriptor()})\n"
                "  PROJECTION(144, schema=windTurbines$newValue:INTEGER )\n"
                "    MAP(147)\n"
                "      SOURCE(146,windTurbines,LogicalSourceDescriptor(windTurbines, ))\n"
                "SINK(148: {PrintSinkDescriptor()})\n"
                "  MAP(147)\n"
                "    SOURCE(146,windTurbines,LogicalSourceDescriptor(windTurbines, ))\n"),
            Z3SignatureBasedBottomUpQueryContainmentRuleTestEntry(
                "TestProjectionContainment",
                R"(Query::from("windTurbines").map(Attribute("value") = 40).project(Attribute("value")).sink(PrintSinkDescriptor::create());)",
                R"(Query::from("windTurbines").map(Attribute("value") = 40).sink(PrintSinkDescriptor::create());)",
                ""),
            Z3SignatureBasedBottomUpQueryContainmentRuleTestEntry(
                "TestProjectionContainment",
                R"(Query::from("windTurbines").map(Attribute("value") = 40).sink(PrintSinkDescriptor::create());)",
                R"(Query::from("windTurbines").map(Attribute("value") = 40).project(Attribute("value")).sink(PrintSinkDescriptor::create());)",
                ""),
            Z3SignatureBasedBottomUpQueryContainmentRuleTestEntry(
                "TestProjectionContainment",
                R"(Query::from("windTurbines").unionWith(Query::from("solarPanels")).filter(Attribute("value") > 4).project(Attribute("value"), Attribute("id1"), Attribute("value1"), Attribute("ts")).joinWith(Query::from("households").project(Attribute("value"), Attribute("id"), Attribute("value1"), Attribute("ts"))).where(Attribute("windTurbines$id1")).equalsTo(Attribute("households$id")).window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))).sink(PrintSinkDescriptor::create());)",
                R"(Query::from("windTurbines").unionWith(Query::from("solarPanels")).joinWith(Query::from("households")).where(Attribute("id1")).equalsTo(Attribute("id")).window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))).filter(Attribute("value") > 4).project(Attribute("windTurbines$value"), Attribute("windTurbines$id1"), Attribute("households$value"), Attribute("households$id"), Attribute("windTurbines$ts"), Attribute("households$ts")).sink(PrintSinkDescriptor::create());)",
                ""),
            Z3SignatureBasedBottomUpQueryContainmentRuleTestEntry(
                "TestProjectionContainment",
                R"(Query::from("windTurbines").unionWith(Query::from("solarPanels")).joinWith(Query::from("households")).where(Attribute("windTurbines$id1")).equalsTo(Attribute("households$id")).window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))).sink(PrintSinkDescriptor::create());)",
                R"(Query::from("windTurbines").unionWith(Query::from("solarPanels")).joinWith(Query::from("households")).where(Attribute("id1")).equalsTo(Attribute("id")).window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))).project(Attribute("windTurbines$value")).sink(PrintSinkDescriptor::create());)",
                "")};
    }

    static auto createWindowContainmentCases() {
        return std::vector<Z3SignatureBasedBottomUpQueryContainmentRuleTestEntry>{
            Z3SignatureBasedBottomUpQueryContainmentRuleTestEntry(
                "TestWindowContainment",
                R"(Query::from("windTurbines").window(TumblingWindow::of(EventTime(Attribute("ts")), Seconds(20))).apply(Sum(Attribute("value1"))).sink(PrintSinkDescriptor::create());)",
                R"(Query::from("windTurbines").window(TumblingWindow::of(EventTime(Attribute("ts")), Seconds(10))).apply(Sum(Attribute("value1"))).sink(PrintSinkDescriptor::create());)",
                ""),
            Z3SignatureBasedBottomUpQueryContainmentRuleTestEntry(
                "TestWindowContainment",
                R"(Query::from("windTurbines").window(TumblingWindow::of(EventTime(Attribute("ts")), Seconds(10))).apply(Sum(Attribute("value1"))).sink(PrintSinkDescriptor::create());)",
                R"(Query::from("windTurbines").window(TumblingWindow::of(EventTime(Attribute("ts")), Seconds(20))).apply(Sum(Attribute("value1"))).sink(PrintSinkDescriptor::create());)",
                ""),
            Z3SignatureBasedBottomUpQueryContainmentRuleTestEntry(
                "TestWindowContainment",
                R"(Query::from("windTurbines").window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))).apply(Sum(Attribute("value")), Min(Attribute("value"))->as(Attribute("newValue"))).sink(PrintSinkDescriptor::create());)",
                R"(Query::from("windTurbines").window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))).apply(Sum(Attribute("value"))).sink(PrintSinkDescriptor::create());)",
                ""),
            Z3SignatureBasedBottomUpQueryContainmentRuleTestEntry(
                "TestWindowContainment",
                R"(Query::from("windTurbines").window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))).apply(Sum(Attribute("value")), Min(Attribute("value1"))).sink(PrintSinkDescriptor::create());)",
                R"(Query::from("windTurbines").window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))).apply(Sum(Attribute("value"))).sink(PrintSinkDescriptor::create());)",
                ""),
            Z3SignatureBasedBottomUpQueryContainmentRuleTestEntry(
                "TestWindowContainment",
                R"(Query::from("windTurbines").window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))).apply(Sum(Attribute("value"))).sink(PrintSinkDescriptor::create());)",
                R"(Query::from("windTurbines").window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))).apply(Sum(Attribute("value")), Min(Attribute("value1"))).sink(PrintSinkDescriptor::create());)",
                ""),
            Z3SignatureBasedBottomUpQueryContainmentRuleTestEntry(
                "TestWindowContainment",
                R"(Query::from("windTurbines").window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))).apply(Sum(Attribute("value"))).window(TumblingWindow::of(EventTime(Attribute("start")), Milliseconds(10000))).apply(Min(Attribute("value"))).sink(PrintSinkDescriptor::create());)",
                R"(Query::from("windTurbines").window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))).apply(Sum(Attribute("value"))).window(TumblingWindow::of(EventTime(Attribute("start")), Milliseconds(100))).apply(Min(Attribute("value"))).sink(PrintSinkDescriptor::create());)",
                ""),
            Z3SignatureBasedBottomUpQueryContainmentRuleTestEntry(
                "TestWindowContainment",
                R"(Query::from("windTurbines").window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))).apply(Sum(Attribute("value")), Min(Attribute("value1")), Max(Attribute("value1"))).sink(PrintSinkDescriptor::create());)",
                R"(Query::from("windTurbines").window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))).apply(Sum(Attribute("value"))).sink(PrintSinkDescriptor::create());)",
                ""),
            Z3SignatureBasedBottomUpQueryContainmentRuleTestEntry(
                "TestWindowContainment",
                R"(Query::from("windTurbines").window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))).apply(Sum(Attribute("value"))).sink(PrintSinkDescriptor::create());)",
                R"(Query::from("windTurbines").window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))).apply(Sum(Attribute("value")), Min(Attribute("value1")), Max(Attribute("value1"))).sink(PrintSinkDescriptor::create());)",
                ""),
            Z3SignatureBasedBottomUpQueryContainmentRuleTestEntry(
                "TestWindowContainment",
                R"(Query::from("windTurbines").unionWith(Query::from("solarPanels")).window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))).byKey(Attribute("id")).apply(Sum(Attribute("value"))).sink(PrintSinkDescriptor::create());)",
                R"(Query::from("windTurbines").unionWith(Query::from("solarPanels")).window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(100))).byKey(Attribute("id")).apply(Sum(Attribute("value")), Min(Attribute("value1")), Max(Attribute("value2"))).sink(PrintSinkDescriptor::create());)",
                "SINK(73: {PrintSinkDescriptor()})\n"
                "  WINDOW AGGREGATION(OP-72, Sum;)\n"
                "    WATERMARKASSIGNER(71)\n"
                "      WINDOW AGGREGATION(OP-78, Sum;Min;Max;)\n"
                "        WATERMARKASSIGNER(77)\n"
                "          unionWith(76)\n"
                "            SOURCE(74,windTurbines,LogicalSourceDescriptor(windTurbines, ))\n"
                "            SOURCE(75,solarPanels,LogicalSourceDescriptor(solarPanels, ))\n"
                "SINK(79: {PrintSinkDescriptor()})\n"
                "  WINDOW AGGREGATION(OP-78, Sum;Min;Max;)\n"
                "    WATERMARKASSIGNER(77)\n"
                "      unionWith(76)\n"
                "        SOURCE(74,windTurbines,LogicalSourceDescriptor(windTurbines, ))\n"
                "        SOURCE(75,solarPanels,LogicalSourceDescriptor(solarPanels, ))\n")};
    }
};

/**
 * @brief Test applying Z3SignatureBasedBottomUpQueryContainmentRuleTest on Global query plan
 */
TEST_P(Z3SignatureBasedBottomUpQueryContainmentRuleTest, testMergingContainmentQueries) {
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
        auto signatureBasedEqualQueryMergerRule = Optimizer::Z3SignatureBasedBottomUpQueryContainmentRule::create(context);
        signatureBasedEqualQueryMergerRule->apply(globalQueryPlan);

        //assert
        auto updatedSharedQMToDeploy = globalQueryPlan->getSharedQueryPlansToDeploy();
        EXPECT_TRUE(updatedSharedQMToDeploy.size() == 1);

        auto updatedSharedQueryPlan1 = updatedSharedQMToDeploy[0]->getQueryPlan();
        EXPECT_TRUE(updatedSharedQueryPlan1);

        NES_INFO(updatedSharedQueryPlan1->toString());

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
                                          Z3SignatureBasedBottomUpQueryContainmentRuleTest::createNoContainmentCases()
                                          /*Z3SignatureBasedBottomUpQueryContainmentRuleTest::createProjectionContainmentCases(),
                                          Z3SignatureBasedBottomUpQueryContainmentRuleTest::createFilterContainmentCases(),
                                          Z3SignatureBasedBottomUpQueryContainmentRuleTest::createWindowContainmentCases()*/),
                        [](const testing::TestParamInfo<Z3SignatureBasedBottomUpQueryContainmentRuleTest::ParamType>& info) {
                            std::string name = info.param.at(0).testType;
                            return name;
                        });
