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

#include <API/Query.hpp>
#include <API/QueryAPI.hpp>
#include <BaseIntegrationTest.hpp>
#include <Catalogs/Source/SourceCatalog.hpp>
#include <Catalogs/UDF/UDFCatalog.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Compiler/CPPCompiler/CPPCompiler.hpp>
#include <Compiler/JITCompilerBuilder.hpp>
#include <Operators/LogicalOperators/MapLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sinks/PrintSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Optimizer/Phases/SignatureInferencePhase.hpp>
#include <Optimizer/Phases/TypeInferencePhase.hpp>
#include <Optimizer/QuerySignatures/SignatureContainmentUtil.hpp>
#include <Optimizer/QueryValidation/SyntacticQueryValidation.hpp>
#include <Services/QueryParsingService.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/magicenum/magic_enum.hpp>
#include <gtest/gtest.h>
#include <iostream>
#include <memory>
#include <tuple>
#include <vector>
#include <z3++.h>

using namespace NES;

class QueryContainmentTestEntry {

  public:
    QueryContainmentTestEntry(const std::string& testType,
                              const std::string& leftQuery,
                              const std::string& rightQuery,
                              Optimizer::ContainmentType containmentType)
        : testType(testType), leftQuery(leftQuery), rightQuery(rightQuery), containmentType(containmentType) {}

    std::string testType;
    std::string leftQuery;
    std::string rightQuery;
    Optimizer::ContainmentType containmentType;
};

class QueryContainmentIdentificationTest : public Testing::BaseUnitTest,
                                           public testing::WithParamInterface<std::vector<QueryContainmentTestEntry>> {

  public:
    SchemaPtr schema;
    SchemaPtr schemaHouseholds;
    Catalogs::Source::SourceCatalogPtr sourceCatalog;
    std::shared_ptr<Catalogs::UDF::UDFCatalog> udfCatalog;
    Optimizer::SyntacticQueryValidationPtr syntacticQueryValidation;

    /* Will be called before all tests in this class are started. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("QueryContainmentIdentificationTest.log", NES::LogLevel::LOG_TRACE);
        NES_INFO("Setup QueryContainmentIdentificationTest test case.");
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
    void TearDown() override {
        NES_DEBUG("QueryContainmentIdentificationTest: Tear down QueryContainmentIdentificationTest test case.");
    }

    static auto createEqualityCases() {
        return std::vector<QueryContainmentTestEntry>{
            QueryContainmentTestEntry(
                "TestEquality",
                R"(Query::from("windTurbines").map(Attribute("value") = 40).filter(Attribute("id") < 45).filter(Attribute("id") < 45).filter(Attribute("id") < 45).filter(Attribute("id") < 45).sink(PrintSinkDescriptor::create());)",
                R"(Query::from("windTurbines").map(Attribute("value") = 40).filter(Attribute("id") < 45).filter(Attribute("id") < 45).filter(Attribute("id") < 45).filter(Attribute("id") < 45).sink(PrintSinkDescriptor::create());)",
                Optimizer::ContainmentType::EQUALITY),
            QueryContainmentTestEntry(
                "TestEquality",
                R"(Query::from("windTurbines").map(Attribute("value") = 40).filter(Attribute("id") < 45).filter(Attribute("id") < 45))"
                R"(.filter(Attribute("id") < 45).filter(Attribute("id") < 45).sink(PrintSinkDescriptor::create());)",
                R"(Query::from("windTurbines").map(Attribute("value") = 40).filter(Attribute("id") < 45).filter(Attribute("id") < 45).filter(Attribute("id") < 45).filter(Attribute("id") < 45).sink(PrintSinkDescriptor::create());)",
                Optimizer::ContainmentType::EQUALITY),
            QueryContainmentTestEntry(
                "TestEquality",
                R"(Query::from("windTurbines").window(TumblingWindow::of(EventTime(Attribute("ts")), Seconds(10))).apply(Sum(Attribute("value1"))).sink(PrintSinkDescriptor::create());)",
                R"(Query::from("windTurbines").window(TumblingWindow::of(EventTime(Attribute("ts")), Seconds(10))).apply(Sum(Attribute("value1"))).sink(PrintSinkDescriptor::create());)",
                Optimizer::ContainmentType::EQUALITY),
            QueryContainmentTestEntry(
                "TestEquality",
                R"(Query::from("windTurbines").joinWith(Query::from("solarPanels")).where(Attribute("id1")).equalsTo(Attribute("id")).window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))).sink(PrintSinkDescriptor::create());)",
                R"(Query::from("windTurbines").joinWith(Query::from("solarPanels")).where(Attribute("id1")).equalsTo(Attribute("id")).window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))).sink(PrintSinkDescriptor::create());)",
                Optimizer::ContainmentType::EQUALITY),
            QueryContainmentTestEntry(
                "TestEquality",
                R"(Query::from("windTurbines").filter(Attribute("value") < 5).joinWith(Query::from("solarPanels")).where(Attribute("id1")).equalsTo(Attribute("id")).window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))).sink(PrintSinkDescriptor::create());)",
                R"(Query::from("windTurbines").joinWith(Query::from("solarPanels")).where(Attribute("id1")).equalsTo(Attribute("id")).window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))).filter(Attribute("value") < 5).sink(PrintSinkDescriptor::create());)",
                Optimizer::ContainmentType::EQUALITY),
            QueryContainmentTestEntry(
                "TestEquality",
                R"(Query::from("windTurbines").joinWith(Query::from("solarPanels")).where(Attribute("id1")).equalsTo(Attribute("id")).window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))).sink(PrintSinkDescriptor::create());)",
                R"(Query::from("windTurbines").joinWith(Query::from("solarPanels")).where(Attribute("id1")).equalsTo(Attribute("id")).window(SlidingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000), Milliseconds(1000))).sink(PrintSinkDescriptor::create());)",
                Optimizer::ContainmentType::EQUALITY),
            QueryContainmentTestEntry(
                "TestEquality",
                R"(Query::from("windTurbines").window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))).apply(Sum(Attribute("value"))).sink(PrintSinkDescriptor::create());)",
                R"(Query::from("windTurbines").window(SlidingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000), Milliseconds(1000))).apply(Sum(Attribute("value"))).sink(PrintSinkDescriptor::create());)",
                Optimizer::ContainmentType::EQUALITY),
            QueryContainmentTestEntry(
                "TestEquality",
                R"(Query::from("windTurbines").window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))).apply(Count()).sink(PrintSinkDescriptor::create());)",
                R"(Query::from("windTurbines").window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))).apply(Count()).sink(PrintSinkDescriptor::create());)",
                Optimizer::ContainmentType::EQUALITY),
            QueryContainmentTestEntry(
                "TestEquality",
                R"(Query::from("windTurbines").unionWith(Query::from("solarPanels")).sink(PrintSinkDescriptor::create());)",
                R"(Query::from("windTurbines").unionWith(Query::from("solarPanels")).sink(PrintSinkDescriptor::create());)",
                Optimizer::ContainmentType::EQUALITY),
            QueryContainmentTestEntry("TestEquality", R"(Query::from("windTurbines").unionWith(Query::from("solarPanels")).filter(Attribute("value") > 4).project(Attribute("value"), Attribute("id1"), Attribute("value1"), Attribute("ts")).joinWith(Query::from("households").project(Attribute("value"), Attribute("id"), Attribute("value1"), Attribute("ts"))).where(Attribute("windTurbines$id1")).equalsTo(Attribute("households$id")).window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))).sink(PrintSinkDescriptor::create());)", R"(Query::from("windTurbines").unionWith(Query::from("solarPanels")).joinWith(Query::from("households")).where(Attribute("id1")).equalsTo(Attribute("id")).window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))).filter(Attribute("value") > 4).project(Attribute("windTurbines$value"), Attribute("windTurbines$id1"), Attribute("windTurbines$value1"), Attribute("windTurbines$ts"), Attribute("households$value"), Attribute("households$id"), Attribute("households$value1"), Attribute("households$ts")).sink(PrintSinkDescriptor::create());)", Optimizer::ContainmentType::EQUALITY),
            QueryContainmentTestEntry("TestEquality",
                                      R"(Query::from("windTurbines").unionWith(Query::from("solarPanels")).window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))).byKey(Attribute("id")).apply(Sum(Attribute("value"))).filter(Attribute("value") > 4).joinWith(Query::from("households").window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))).byKey(Attribute("id")).apply(Sum(Attribute("value")))).where(Attribute("windTurbines$id")).equalsTo(Attribute("households$id")).window(TumblingWindow::of(EventTime(Attribute("start")), Hours(1))).sink(PrintSinkDescriptor::create());)",
                                      R"(Query::from("windTurbines").unionWith(Query::from("solarPanels")).window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))).byKey(Attribute("id")).apply(Sum(Attribute("value"))).joinWith(Query::from("households").window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))).byKey(Attribute("id")).apply(Sum(Attribute("value")))).where(Attribute("id")).equalsTo(Attribute("id")).window(TumblingWindow::of(EventTime(Attribute("start")), Hours(1))).filter(Attribute("windTurbines$value") > 4).sink(PrintSinkDescriptor::create());)",
                                      Optimizer::ContainmentType::EQUALITY),
            QueryContainmentTestEntry(
                "TestEquality",
                R"(Query::from("windTurbines").unionWith(Query::from("solarPanels")).window(TumblingWindow::of(EventTime(Attribute("ts")), Hours(1))).byKey(Attribute("id")).apply(Sum(Attribute("value"))).joinWith(Query::from("households").project(Attribute("value3"), Attribute("id"), Attribute("ts").as("start"))).where(Attribute("id")).equalsTo(Attribute("id")).window(TumblingWindow::of(EventTime(Attribute("start")), Hours(1))).map(Attribute("DifferenceProducedConsumedPower") = Attribute("value") - Attribute("value3")).project(Attribute("DifferenceProducedConsumedPower"), Attribute("value"), Attribute("value3")).filter(Attribute("DifferenceProducedConsumedPower") < 100).sink(PrintSinkDescriptor::create());)",
                R"(Query::from("windTurbines").unionWith(Query::from("solarPanels")).window(TumblingWindow::of(EventTime(Attribute("ts")), Hours(1))).byKey(Attribute("id")).apply(Sum(Attribute("value"))).joinWith(Query::from("households").project(Attribute("value3"), Attribute("id"), Attribute("ts").as("start"))).where(Attribute("id")).equalsTo(Attribute("id")).window(TumblingWindow::of(EventTime(Attribute("start")), Hours(1))).map(Attribute("DifferenceProducedConsumedPower") = Attribute("value") - Attribute("value3")).project(Attribute("DifferenceProducedConsumedPower"), Attribute("value"), Attribute("value3")).filter(Attribute("DifferenceProducedConsumedPower") < 100).sink(PrintSinkDescriptor::create());)",
                Optimizer::ContainmentType::EQUALITY),
        };
    }

    static auto createNoContainmentCases() {
        return std::vector<
            QueryContainmentTestEntry>{QueryContainmentTestEntry(
                                           "TestNoContainment",
                                           R"(Query::from("windTurbines").map(Attribute("value") = 40).project(Attribute("value").as("newValue")).sink(PrintSinkDescriptor::create());)",
                                           R"(Query::from("windTurbines").map(Attribute("value") = 40).sink(PrintSinkDescriptor::create());)",
                                           Optimizer::ContainmentType::NO_CONTAINMENT),
                                       QueryContainmentTestEntry(
                                           "TestNoContainment",
                                           R"(Query::from("windTurbines").map(Attribute("value") = 40).filter(Attribute("id") < 45).filter(Attribute("id") < 45).filter(Attribute("id") < 45).filter(Attribute("id") < 45).sink(PrintSinkDescriptor::create());)",
                                           R"(Query::from("solarPanels").map(Attribute("value") = 40).filter(Attribute("id") < 45).filter(Attribute("id") < 45).filter(Attribute("id") < 45).filter(Attribute("id") < 45).sink(PrintSinkDescriptor::create());)",
                                           Optimizer::ContainmentType::NO_CONTAINMENT),
                                       QueryContainmentTestEntry(
                                           "TestNoContainment",
                                           R"(Query::from("windTurbines").map(Attribute("value") = 40).map(Attribute("value") = Attribute("value") + 10).sink(PrintSinkDescriptor::create());)",
                                           R"(Query::from("windTurbines").map(Attribute("value") = 40).sink(PrintSinkDescriptor::create());)",
                                           Optimizer::ContainmentType::NO_CONTAINMENT),
                                       QueryContainmentTestEntry(
                                           "TestNoContainment",
                                           R"(Query::from("windTurbines").filter(Attribute("value") < 40).window(TumblingWindow::of(EventTime(Attribute("ts")), Seconds(20))).apply(Sum(Attribute("value1"))).sink(PrintSinkDescriptor::create());)",
                                           R"(Query::from("windTurbines"))"
                                           R"(.window(TumblingWindow::of(EventTime(Attribute("ts")), Seconds(10))).apply(Sum(Attribute("value1"))).sink(PrintSinkDescriptor::create());)",
                                           Optimizer::ContainmentType::NO_CONTAINMENT),
                                       QueryContainmentTestEntry(
                                           "TestNoContainment",
                                           R"(Query::from("windTurbines").joinWith(Query::from("solarPanels")).where(Attribute("id1")).equalsTo(Attribute("id")).window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))).sink(PrintSinkDescriptor::create());)",
                                           R"(Query::from("windTurbines").joinWith(Query::from("solarPanels")).where(Attribute("id1")).equalsTo(Attribute("id")).window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(20))).filter(Attribute("value") < 10).sink(PrintSinkDescriptor::create());)",
                                           Optimizer::ContainmentType::NO_CONTAINMENT),
                                       QueryContainmentTestEntry(
                                           "TestNoContainment",
                                           R"(Query::from("windTurbines").filter(Attribute("value") < 5).joinWith(Query::from("solarPanels")).where(Attribute("id1")).equalsTo(Attribute("id")).window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))).sink(PrintSinkDescriptor::create());)",
                                           R"(Query::from("windTurbines").joinWith(Query::from("solarPanels")).where(Attribute("id1")).equalsTo(Attribute("id")).window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(20))).sink(PrintSinkDescriptor::create());)",
                                           Optimizer::ContainmentType::NO_CONTAINMENT),
                                       QueryContainmentTestEntry(
                                           "TestNoContainment",
                                           R"(Query::from("windTurbines").window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))).apply(Sum(Attribute("value"))).sink(PrintSinkDescriptor::create());)",
                                           R"(Query::from("windTurbines").window(SlidingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000), Milliseconds(10))).byKey(Attribute("id")).apply(Sum(Attribute("value"))).sink(PrintSinkDescriptor::create());)",
                                           Optimizer::ContainmentType::NO_CONTAINMENT),
                                       QueryContainmentTestEntry(
                                           "TestNoContainment",
                                           R"(Query::from("windTurbines").window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))).apply(Sum(Attribute("value"))).window(TumblingWindow::of(EventTime(Attribute("start")), Milliseconds(10000))).apply(Min(Attribute("value"))).sink(PrintSinkDescriptor::create());)",
                                           R"(Query::from("windTurbines").window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))).apply(Sum(Attribute("value"))).sink(PrintSinkDescriptor::create());)",
                                           Optimizer::ContainmentType::NO_CONTAINMENT),
                                       QueryContainmentTestEntry("TestNoContainment", R"(Query::from("windTurbines").window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))).apply(Sum(Attribute("value"))).window(TumblingWindow::of(EventTime(Attribute("start")), Milliseconds(10000))).apply(Min(Attribute("value"))).sink(PrintSinkDescriptor::create());)", R"(Query::from("windTurbines").window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(100))).apply(Sum(Attribute("value"))).window(TumblingWindow::of(EventTime(Attribute("start")), Milliseconds(1000))).apply(Max(Attribute("value"))).sink(PrintSinkDescriptor::create());)", Optimizer::ContainmentType::NO_CONTAINMENT),
                                       QueryContainmentTestEntry(
                                           "TestNoContainment",
                                           R"(Query::from("windTurbines").unionWith(Query::from("solarPanels")).window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(10000))).apply(Avg(Attribute("value"))).sink(PrintSinkDescriptor::create());)",
                                           R"(Query::from("windTurbines").unionWith(Query::from("solarPanels")).window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(10000))).apply(Sum(Attribute("value"))).sink(PrintSinkDescriptor::create());)",
                                           Optimizer::ContainmentType::NO_CONTAINMENT),
                                       QueryContainmentTestEntry(
                                           "TestNoContainment",
                                           R"(Query::from("windTurbines").unionWith(Query::from("solarPanels")).window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(10000))).apply(Avg(Attribute("value"))).sink(PrintSinkDescriptor::create());)",
                                           R"(Query::from("windTurbines").unionWith(Query::from("solarPanels")).window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(100))).apply(Sum(Attribute("value"))).sink(PrintSinkDescriptor::create());)",
                                           Optimizer::ContainmentType::NO_CONTAINMENT),
                                       QueryContainmentTestEntry(
                                           "TestNoContainment",
                                           R"(Query::from("windTurbines").unionWith(Query::from("solarPanels")).window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(10000))).byKey(Attribute("id")).apply(Sum(Attribute("value"))).sink(PrintSinkDescriptor::create());)",
                                           R"(Query::from("windTurbines").unionWith(Query::from("solarPanels")).window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(10000))).apply(Sum(Attribute("value"))).sink(PrintSinkDescriptor::create());)",
                                           Optimizer::ContainmentType::NO_CONTAINMENT),
                                       QueryContainmentTestEntry(
                                           "TestNoContainment",
                                           R"(Query::from("windTurbines").unionWith(Query::from("solarPanels")).window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(10000))).apply(Sum(Attribute("value"))).filter(Attribute("value") == 1).sink(PrintSinkDescriptor::create());)",
                                           R"(Query::from("windTurbines").unionWith(Query::from("solarPanels")).window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(100))).apply(Sum(Attribute("value"))).sink(PrintSinkDescriptor::create());)",
                                           Optimizer::ContainmentType::NO_CONTAINMENT),
                                       QueryContainmentTestEntry(
                                           "TestNoContainment",
                                           R"(Query::from("windTurbines").unionWith(Query::from("solarPanels")).window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(10000))).apply(Sum(Attribute("value"))).filter(Attribute("value") != 1).sink(PrintSinkDescriptor::create());)",
                                           R"(Query::from("windTurbines").filter(Attribute("value") == 1).unionWith(Query::from("solarPanels")).window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(10000))).apply(Sum(Attribute("value"))).sink(PrintSinkDescriptor::create());)",
                                           Optimizer::ContainmentType::NO_CONTAINMENT),
                                       QueryContainmentTestEntry(
                                           "TestNoContainment",
                                           R"(Query::from("windTurbines").filter(Attribute("value") == 5).unionWith(Query::from("solarPanels")).joinWith(Query::from("households")).where(Attribute("id1")).equalsTo(Attribute("id")).window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))).sink(PrintSinkDescriptor::create());)",
                                           R"(Query::from("windTurbines").joinWith(Query::from("households")).where(Attribute("id1")).equalsTo(Attribute("id")).window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))).filter(Attribute("value") != 4).sink(PrintSinkDescriptor::create());)",
                                           Optimizer::ContainmentType::NO_CONTAINMENT),
                                       QueryContainmentTestEntry("TestNoContainment", R"(Query::from("windTurbines").filter(Attribute("value") != 4).unionWith(Query::from("solarPanels")).joinWith(Query::from("households")).where(Attribute("id1")).equalsTo(Attribute("id")).window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))).sink(PrintSinkDescriptor::create());)", R"(Query::from("windTurbines").unionWith(Query::from("solarPanels")).joinWith(Query::from("households")).where(Attribute("id1")).equalsTo(Attribute("id")).window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))).filter(Attribute("value") > 4).sink(PrintSinkDescriptor::create());)", Optimizer::ContainmentType::NO_CONTAINMENT),
                                       QueryContainmentTestEntry(
                                           "TestNoContainment",
                                           R"(Query::from("windTurbines").unionWith(Query::from("solarPanels")).filter(Attribute("value") > 4).joinWith(Query::from("households")).where(Attribute("windTurbines$id1")).equalsTo(Attribute("households$id")).window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(100))).sink(PrintSinkDescriptor::create());)",
                                           R"(Query::from("windTurbines").unionWith(Query::from("solarPanels")).joinWith(Query::from("households")).where(Attribute("id1")).equalsTo(Attribute("id")).window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))).filter(Attribute("value") > 4).sink(PrintSinkDescriptor::create());)",
                                           Optimizer::ContainmentType::NO_CONTAINMENT),
                                       //Limit of our algorithm, cannot detect equivalence among these windows
                                       QueryContainmentTestEntry(
                                           "TestNoContainment",
                                           R"(Query::from("windTurbines").unionWith(Query::from("solarPanels")).filter(Attribute("value") > 4).joinWith(Query::from("households")).where(Attribute("windTurbines$id")).equalsTo(Attribute("households$id")).window(TumblingWindow::of(EventTime(Attribute("ts")), Hours(1))).window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))).byKey(Attribute("id")).apply(Sum(Attribute("value"))).sink(PrintSinkDescriptor::create());)",
                                           R"(Query::from("windTurbines").unionWith(Query::from("solarPanels")).window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))).byKey(Attribute("id")).apply(Sum(Attribute("value"))).joinWith(Query::from("households").window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))).byKey(Attribute("id")).apply(Sum(Attribute("value")))).where(Attribute("id")).equalsTo(Attribute("id")).window(TumblingWindow::of(EventTime(Attribute("start")), Hours(1))).filter(Attribute("windTurbines$value") > 4).sink(PrintSinkDescriptor::create());)",
                                           Optimizer::ContainmentType::NO_CONTAINMENT),
                                       QueryContainmentTestEntry("TestNoContainment", R"(Query::from("windTurbines").unionWith(Query::from("solarPanels")).filter(Attribute("value") > 4).joinWith(Query::from("households")).where(Attribute("windTurbines$id1")).equalsTo(Attribute("households$id")).window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))).sink(PrintSinkDescriptor::create());)", R"(Query::from("windTurbines").unionWith(Query::from("solarPanels")).joinWith(Query::from("households")).where(Attribute("id1")).equalsTo(Attribute("id")).window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))).project(Attribute("windTurbines$value")).sink(PrintSinkDescriptor::create());)", Optimizer::ContainmentType::NO_CONTAINMENT),
                                       QueryContainmentTestEntry(
                                           "TestNoContainment",
                                           R"(Query::from("windTurbines").unionWith(Query::from("solarPanels")).joinWith(Query::from("households")).where(Attribute("windTurbines$id1")).equalsTo(Attribute("households$id")).window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(10000))).sink(PrintSinkDescriptor::create());)",
                                           R"(Query::from("windTurbines").unionWith(Query::from("solarPanels")).joinWith(Query::from("households")).where(Attribute("id1")).equalsTo(Attribute("id")).window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))).project(Attribute("windTurbines$value")).sink(PrintSinkDescriptor::create());)",
                                           Optimizer::ContainmentType::NO_CONTAINMENT),
                                       QueryContainmentTestEntry(
                                           "TestNoContainment",
                                           R"(Query::from("windTurbines").joinWith(Query::from("solarPanels")).where(Attribute("id1")).equalsTo(Attribute("id")).window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(10))).sink(PrintSinkDescriptor::create());)",
                                           R"(Query::from("windTurbines").joinWith(Query::from("solarPanels")).where(Attribute("id1")).equalsTo(Attribute("id")).window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))).sink(PrintSinkDescriptor::create());)",
                                           Optimizer::ContainmentType::NO_CONTAINMENT),
                                       QueryContainmentTestEntry(
                                           "TestNoContainment",
                                           R"(Query::from("windTurbines").joinWith(Query::from("solarPanels")).where(Attribute("id1")).equalsTo(Attribute("id")).window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))).sink(PrintSinkDescriptor::create());)",
                                           R"(Query::from("windTurbines").joinWith(Query::from("solarPanels")).where(Attribute("id1")).equalsTo(Attribute("id")).window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(20))).sink(PrintSinkDescriptor::create());)",
                                           Optimizer::ContainmentType::NO_CONTAINMENT),
                                       QueryContainmentTestEntry(
                                           "TestNoContainment",
                                           R"(Query::from("windTurbines").unionWith(Query::from("solarPanels")).filter(Attribute("value") > 4).joinWith(Query::from("households")).where(Attribute("windTurbines$id1")).equalsTo(Attribute("households$id")).window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(100))).sink(PrintSinkDescriptor::create());)",
                                           R"(Query::from("windTurbines").unionWith(Query::from("solarPanels")).joinWith(Query::from("households")).where(Attribute("id1")).equalsTo(Attribute("id")).window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))).filter(Attribute("windTurbines$value") > 4).sink(PrintSinkDescriptor::create());)",
                                           Optimizer::ContainmentType::NO_CONTAINMENT),
                                       //projection conditions differ too much for containment to be picked up in the complete query
                                       //bottom up approach would detect it, though
                                       QueryContainmentTestEntry(
                                           "TestNoContainment",
                                           R"(Query::from("windTurbines").unionWith(Query::from("solarPanels")).joinWith(Query::from("households")).where(Attribute("windTurbines$id1")).equalsTo(Attribute("households$id")).window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))).sink(PrintSinkDescriptor::create());)",
                                           R"(Query::from("windTurbines").unionWith(Query::from("solarPanels")).joinWith(Query::from("households")).where(Attribute("id1")).equalsTo(Attribute("id")).window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))).window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))).byKey(Attribute("id")).apply(Sum(Attribute("value"))).sink(PrintSinkDescriptor::create());)",
                                           Optimizer::ContainmentType::NO_CONTAINMENT),
                                       //another limit of our algorithm. If one query does not have a window, we cannot detect containment
                                       QueryContainmentTestEntry(
                                           "TestNoContainment",
                                           R"(Query::from("windTurbines").unionWith(Query::from("solarPanels")).sink(PrintSinkDescriptor::create());)",
                                           R"(Query::from("windTurbines").unionWith(Query::from("solarPanels")).window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))).byKey(Attribute("id")).apply(Sum(Attribute("value"))).sink(PrintSinkDescriptor::create());)",
                                           Optimizer::ContainmentType::NO_CONTAINMENT),
                                       QueryContainmentTestEntry(
                                           "TestNoContainment",
                                           R"(Query::from("windTurbines").unionWith(Query::from("solarPanels")).window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(10))).byKey(Attribute("id")).apply(Sum(Attribute("value"))).sink(PrintSinkDescriptor::create());)",
                                           R"(Query::from("windTurbines").unionWith(Query::from("solarPanels")).window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(10))).byKey(Attribute("id")).apply(Sum(Attribute("value"))).window(TumblingWindow::of(EventTime(Attribute("start")), Milliseconds(1000))).byKey(Attribute("id")).apply(Min(Attribute("value"))).sink(PrintSinkDescriptor::create());)",
                                           Optimizer::ContainmentType::NO_CONTAINMENT)};
    }

    static auto createFilterContainmentCases() {
        return std::vector<QueryContainmentTestEntry>{
            QueryContainmentTestEntry(
                "TestFilterContainment",
                R"(Query::from("windTurbines").map(Attribute("value") = 40).filter(Attribute("id") < 45).sink(PrintSinkDescriptor::create());)",
                R"(Query::from("windTurbines").map(Attribute("value") = 40).filter(Attribute("id") < 60).sink(PrintSinkDescriptor::create());)",
                Optimizer::ContainmentType::LEFT_SIG_CONTAINED),
            QueryContainmentTestEntry(
                "TestFilterContainment",
                R"(Query::from("windTurbines").map(Attribute("value") = 40).filter(Attribute("id") < 60).sink(PrintSinkDescriptor::create());)",
                R"(Query::from("windTurbines").map(Attribute("value") = 40).filter(Attribute("id") < 45).sink(PrintSinkDescriptor::create());)",
                Optimizer::ContainmentType::RIGHT_SIG_CONTAINED),
            QueryContainmentTestEntry(
                "TestFilterContainment",
                R"(Query::from("windTurbines").joinWith(Query::from("solarPanels")).where(Attribute("id1")).equalsTo(Attribute("id")).window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))).sink(PrintSinkDescriptor::create());)",
                R"(Query::from("windTurbines").joinWith(Query::from("solarPanels")).where(Attribute("id1")).equalsTo(Attribute("id")).window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))).filter(Attribute("value") < 5).sink(PrintSinkDescriptor::create());)",
                Optimizer::ContainmentType::RIGHT_SIG_CONTAINED),
            QueryContainmentTestEntry(
                "TestFilterContainment",
                R"(Query::from("windTurbines").filter(Attribute("value") < 5).joinWith(Query::from("solarPanels")).where(Attribute("id1")).equalsTo(Attribute("id")).window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))).sink(PrintSinkDescriptor::create());)",
                R"(Query::from("windTurbines").joinWith(Query::from("solarPanels")).where(Attribute("id1")).equalsTo(Attribute("id")).window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))).sink(PrintSinkDescriptor::create());)",
                Optimizer::ContainmentType::LEFT_SIG_CONTAINED),
            QueryContainmentTestEntry(
                "TestFilterContainment",
                R"(Query::from("windTurbines").filter(Attribute("value") == 5).joinWith(Query::from("households")).where(Attribute("id1")).equalsTo(Attribute("id")).window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))).sink(PrintSinkDescriptor::create());)",
                R"(Query::from("windTurbines").joinWith(Query::from("households")).where(Attribute("id1")).equalsTo(Attribute("id")).window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))).filter(Attribute("value") != 4).sink(PrintSinkDescriptor::create());)",
                Optimizer::ContainmentType::LEFT_SIG_CONTAINED),
            QueryContainmentTestEntry(
                "TestFilterContainment",
                R"(Query::from("windTurbines").filter(Attribute("value") == 5).unionWith(Query::from("solarPanels")).unionWith(Query::from("test")).sink(PrintSinkDescriptor::create());)",
                R"(Query::from("windTurbines").filter(Attribute("value") == 5).unionWith(Query::from("solarPanels")).unionWith(Query::from("test")).filter(Attribute("value") >= 6).sink(PrintSinkDescriptor::create());)",
                Optimizer::ContainmentType::RIGHT_SIG_CONTAINED),
            QueryContainmentTestEntry(
                "TestFilterContainment",
                R"(Query::from("windTurbines").unionWith(Query::from("solarPanels")).filter(Attribute("value") != 4).joinWith(Query::from("households")).where(Attribute("id1")).equalsTo(Attribute("id")).window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))).sink(PrintSinkDescriptor::create());)",
                R"(Query::from("windTurbines").unionWith(Query::from("solarPanels")).joinWith(Query::from("households")).where(Attribute("id1")).equalsTo(Attribute("id")).window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))).filter(Attribute("value") > 4).sink(PrintSinkDescriptor::create());)",
                Optimizer::ContainmentType::RIGHT_SIG_CONTAINED)};
    }

    static auto createProjectionContainmentCases() {
        return std::vector<QueryContainmentTestEntry>{
            QueryContainmentTestEntry(
                "TestProjectionContainment",
                R"(Query::from("windTurbines").project(Attribute("value")).sink(PrintSinkDescriptor::create());)",
                R"(Query::from("windTurbines").sink(PrintSinkDescriptor::create());)",
                Optimizer::ContainmentType::LEFT_SIG_CONTAINED),
            QueryContainmentTestEntry(
                "TestProjectionContainment",
                R"(Query::from("windTurbines").map(Attribute("value") = 10).map(Attribute("id") = 10).map(Attribute("ts") = 10).project(Attribute("value"), Attribute("id")).sink(PrintSinkDescriptor::create());)",
                R"(Query::from("windTurbines").map(Attribute("value") = 10).map(Attribute("id") = 10).map(Attribute("ts") = 10).project(Attribute("value"), Attribute("id"), Attribute("ts")).sink(PrintSinkDescriptor::create());)",
                Optimizer::ContainmentType::LEFT_SIG_CONTAINED),
            QueryContainmentTestEntry(
                "TestProjectionContainment",
                R"(Query::from("windTurbines").map(Attribute("value") = 40).project(Attribute("value")).sink(PrintSinkDescriptor::create());)",
                R"(Query::from("windTurbines").map(Attribute("value") = 40).sink(PrintSinkDescriptor::create());)",
                Optimizer::ContainmentType::LEFT_SIG_CONTAINED),
            QueryContainmentTestEntry(
                "TestProjectionContainment",
                R"(Query::from("windTurbines").map(Attribute("value") = 40).sink(PrintSinkDescriptor::create());)",
                R"(Query::from("windTurbines").map(Attribute("value") = 40).project(Attribute("value")).sink(PrintSinkDescriptor::create());)",
                Optimizer::ContainmentType::RIGHT_SIG_CONTAINED),
            QueryContainmentTestEntry(
                "TestProjectionContainment",
                R"(Query::from("windTurbines").unionWith(Query::from("solarPanels")).filter(Attribute("value") > 4).project(Attribute("value"), Attribute("id1"), Attribute("value1"), Attribute("ts")).joinWith(Query::from("households").project(Attribute("value"), Attribute("id"), Attribute("value1"), Attribute("ts"))).where(Attribute("windTurbines$id1")).equalsTo(Attribute("households$id")).window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))).sink(PrintSinkDescriptor::create());)",
                R"(Query::from("windTurbines").unionWith(Query::from("solarPanels")).joinWith(Query::from("households")).where(Attribute("id1")).equalsTo(Attribute("id")).window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))).filter(Attribute("value") > 4).project(Attribute("windTurbines$value"), Attribute("windTurbines$id1"), Attribute("households$value"), Attribute("households$id"), Attribute("windTurbines$ts"), Attribute("households$ts")).sink(PrintSinkDescriptor::create());)",
                Optimizer::ContainmentType::RIGHT_SIG_CONTAINED),
            QueryContainmentTestEntry(
                "TestProjectionContainment",
                R"(Query::from("windTurbines").unionWith(Query::from("solarPanels")).joinWith(Query::from("households")).where(Attribute("windTurbines$id1")).equalsTo(Attribute("households$id")).window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))).sink(PrintSinkDescriptor::create());)",
                R"(Query::from("windTurbines").unionWith(Query::from("solarPanels")).joinWith(Query::from("households")).where(Attribute("id1")).equalsTo(Attribute("id")).window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))).project(Attribute("windTurbines$value")).sink(PrintSinkDescriptor::create());)",
                Optimizer::ContainmentType::RIGHT_SIG_CONTAINED),
            QueryContainmentTestEntry("TestProjectionContainment",
                                      R"(Query::from("windTurbines").unionWith(Query::from("solarPanels")).filter(Attribute("value") > 4).project(Attribute("value"), Attribute("id1"), Attribute("value1"), Attribute("ts")).joinWith(Query::from("households").project(Attribute("value"), Attribute("id"), Attribute("value1"), Attribute("ts"))).where(Attribute("windTurbines$id1")).equalsTo(Attribute("households$id")).window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))).sink(PrintSinkDescriptor::create());)",
                                      R"(Query::from("windTurbines").unionWith(Query::from("solarPanels")).joinWith(Query::from("households")).where(Attribute("id1")).equalsTo(Attribute("id")).window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))).filter(Attribute("value") > 4).project(Attribute("windTurbines$value"), Attribute("windTurbines$id1"), Attribute("windTurbines$value1"),Attribute("households$value"), Attribute("households$id"), Attribute("households$value1"),Attribute("windTurbines$ts"), Attribute("households$ts")).sink(PrintSinkDescriptor::create());)",
                                      Optimizer::ContainmentType::RIGHT_SIG_CONTAINED),
        };
    }

    static auto createWindowContainmentCases() {
        return std::
            vector<QueryContainmentTestEntry>{
                QueryContainmentTestEntry(
                    "TestWindowContainment",
                    R"(Query::from("windTurbines").window(TumblingWindow::of(EventTime(Attribute("ts")), Seconds(20))).apply(Sum(Attribute("value1"))).sink(PrintSinkDescriptor::create());)",
                    R"(Query::from("windTurbines").window(TumblingWindow::of(EventTime(Attribute("ts")), Seconds(10))).apply(Sum(Attribute("value1"))).sink(PrintSinkDescriptor::create());)",
                    Optimizer::ContainmentType::LEFT_SIG_CONTAINED),
                QueryContainmentTestEntry(
                    "TestWindowContainment",
                    R"(Query::from("windTurbines").window(TumblingWindow::of(EventTime(Attribute("ts")), Seconds(10))).apply(Sum(Attribute("value1"))).sink(PrintSinkDescriptor::create());)",
                    R"(Query::from("windTurbines").window(TumblingWindow::of(EventTime(Attribute("ts")), Seconds(20))).apply(Sum(Attribute("value1"))).sink(PrintSinkDescriptor::create());)",
                    Optimizer::ContainmentType::RIGHT_SIG_CONTAINED),
                QueryContainmentTestEntry(
                    "TestWindowContainment",
                    R"(Query::from("windTurbines").window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))).apply(Sum(Attribute("value")), Min(Attribute("value"))->as(Attribute("newValue"))).sink(PrintSinkDescriptor::create());)",
                    R"(Query::from("windTurbines").window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))).apply(Sum(Attribute("value"))).sink(PrintSinkDescriptor::create());)",
                    Optimizer::ContainmentType::RIGHT_SIG_CONTAINED),
                QueryContainmentTestEntry(
                    "TestWindowContainment",
                    R"(Query::from("windTurbines").window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))).apply(Sum(Attribute("value")), Min(Attribute("value1"))).sink(PrintSinkDescriptor::create());)",
                    R"(Query::from("windTurbines").window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))).apply(Sum(Attribute("value"))).sink(PrintSinkDescriptor::create());)",
                    Optimizer::ContainmentType::RIGHT_SIG_CONTAINED),
                QueryContainmentTestEntry(
                    "TestWindowContainment",
                    R"(Query::from("windTurbines").window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))).apply(Sum(Attribute("value"))).sink(PrintSinkDescriptor::create());)",
                    R"(Query::from("windTurbines").window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))).apply(Sum(Attribute("value")), Min(Attribute("value1"))).sink(PrintSinkDescriptor::create());)",
                    Optimizer::ContainmentType::LEFT_SIG_CONTAINED),
                QueryContainmentTestEntry(
                    "TestWindowContainment",
                    R"(Query::from("windTurbines").window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))).apply(Sum(Attribute("value"))).window(TumblingWindow::of(EventTime(Attribute("start")), Milliseconds(10000))).apply(Min(Attribute("value"))).sink(PrintSinkDescriptor::create());)",
                    R"(Query::from("windTurbines").window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))).apply(Sum(Attribute("value"))).window(TumblingWindow::of(EventTime(Attribute("start")), Milliseconds(100))).apply(Min(Attribute("value"))).sink(PrintSinkDescriptor::create());)",
                    Optimizer::ContainmentType::LEFT_SIG_CONTAINED),
                QueryContainmentTestEntry(
                    "TestWindowContainment",
                    R"(Query::from("windTurbines").window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))).apply(Sum(Attribute("value")), Min(Attribute("value1")), Max(Attribute("value1"))).sink(PrintSinkDescriptor::create());)",
                    R"(Query::from("windTurbines").window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))).apply(Sum(Attribute("value"))).sink(PrintSinkDescriptor::create());)",
                    Optimizer::ContainmentType::RIGHT_SIG_CONTAINED),
                QueryContainmentTestEntry(
                    "TestWindowContainment",
                    R"(Query::from("windTurbines").window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))).apply(Sum(Attribute("value"))).sink(PrintSinkDescriptor::create());)",
                    R"(Query::from("windTurbines").window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))).apply(Sum(Attribute("value")), Min(Attribute("value1")), Max(Attribute("value1"))).sink(PrintSinkDescriptor::create());)",
                    Optimizer::ContainmentType::LEFT_SIG_CONTAINED),
                QueryContainmentTestEntry(
                    "TestWindowContainment",
                    R"(Query::from("windTurbines").unionWith(Query::from("solarPanels")).window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))).byKey(Attribute("id")).apply(Sum(Attribute("value"))).sink(PrintSinkDescriptor::create());)",
                    R"(Query::from("windTurbines").unionWith(Query::from("solarPanels")).window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(100))).byKey(Attribute("id")).apply(Sum(Attribute("value")), Min(Attribute("value1")), Max(Attribute("value2"))).sink(PrintSinkDescriptor::create());)",
                    Optimizer::ContainmentType::LEFT_SIG_CONTAINED),
                QueryContainmentTestEntry("TestWindowContainment", R"(Query::from("windTurbines").unionWith(Query::from("solarPanels")).window(SlidingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000), Milliseconds(10))).byKey(Attribute("id")).apply(Sum(Attribute("value"))).sink(PrintSinkDescriptor::create());)", R"(Query::from("windTurbines").unionWith(Query::from("solarPanels")).window(SlidingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000), Milliseconds(20))).byKey(Attribute("id")).apply(Sum(Attribute("value"))).sink(PrintSinkDescriptor::create());)", Optimizer::ContainmentType::NO_CONTAINMENT)};
    }
};

/**
 * @brief tests if the correct containment relationship is returned by the signature containment util
 */
TEST_P(QueryContainmentIdentificationTest, testContainmentIdentification) {

    auto containmentCases = GetParam();

    uint16_t counter = 0;
    for (auto containmentCase : containmentCases) {
        QueryPlanPtr queryPlanSQPQuery = syntacticQueryValidation->validate(containmentCase.leftQuery);
        QueryPlanPtr queryPlanNewQuery = syntacticQueryValidation->validate(containmentCase.rightQuery);
        //type inference face
        auto typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);
        typeInferencePhase->execute(queryPlanSQPQuery);
        typeInferencePhase->execute(queryPlanNewQuery);
        //obtain context and create signatures
        z3::ContextPtr context = std::make_shared<z3::context>();
        auto signatureInferencePhase =
            Optimizer::SignatureInferencePhase::create(context,
                                                       Optimizer::QueryMergerRule::Z3SignatureBasedBottomUpQueryContainmentRule);
        signatureInferencePhase->execute(queryPlanSQPQuery);
        signatureInferencePhase->execute(queryPlanNewQuery);
        SinkLogicalOperatorNodePtr sinkOperatorSQPQuery = queryPlanSQPQuery->getSinkOperators()[0];
        SinkLogicalOperatorNodePtr sinkOperatorNewQuery = queryPlanSQPQuery->getSinkOperators()[0];
        auto signatureContainmentUtil = Optimizer::SignatureContainmentUtil::create(context);
        std::map<OperatorNodePtr, OperatorNodePtr> targetToHostSinkOperatorMap;
        auto sqpSink = queryPlanSQPQuery->getSinkOperators()[0];
        auto newSink = queryPlanNewQuery->getSinkOperators()[0];
        //Check if the host and target sink operator signatures have a containment relationship
        Optimizer::ContainmentType containment =
            signatureContainmentUtil->checkContainmentForBottomUpMerging(sqpSink->getZ3Signature(), newSink->getZ3Signature());
        NES_TRACE("Z3SignatureBasedContainmentBasedCompleteQueryMergerRule: containment: {}", magic_enum::enum_name(containment));
        NES_TRACE("Query pairing number: {}", counter);
        ASSERT_EQ(containment, containmentCase.containmentType);
        counter++;
    }
}

INSTANTIATE_TEST_CASE_P(testContainment,
                        QueryContainmentIdentificationTest,
                        ::testing::Values(QueryContainmentIdentificationTest::createEqualityCases(),
                                          QueryContainmentIdentificationTest::createNoContainmentCases(),
                                          QueryContainmentIdentificationTest::createProjectionContainmentCases(),
                                          QueryContainmentIdentificationTest::createFilterContainmentCases(),
                                          QueryContainmentIdentificationTest::createWindowContainmentCases()),
                        [](const testing::TestParamInfo<QueryContainmentIdentificationTest::ParamType>& info) {
                            std::string name = info.param.at(0).testType;
                            return name;
                        });