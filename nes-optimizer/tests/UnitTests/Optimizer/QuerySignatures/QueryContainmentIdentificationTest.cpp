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
#include <Operators/Expressions/FieldAccessExpressionNode.hpp>
#include <Operators/LogicalOperators/LogicalMapOperator.hpp>
#include <Operators/LogicalOperators/Sinks/LogicalSinkOperator.hpp>
#include <Operators/LogicalOperators/Sinks/PrintSinkDescriptor.hpp>
#include <Optimizer/Phases/SignatureInferencePhase.hpp>
#include <Optimizer/Phases/TypeInferencePhase.hpp>
#include <Optimizer/QuerySignatures/SignatureContainmentCheck.hpp>
#include <Plans/Query/QueryPlan.hpp>
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
                              const Query& leftQuery,
                              const Query& rightQuery,
                              Optimizer::ContainmentRelationship containmentRelationship)
        : testType(testType), leftQuery(leftQuery), rightQuery(rightQuery), containmentRelationship(containmentRelationship) {}

    std::string testType;
    Query leftQuery;
    Query rightQuery;
    Optimizer::ContainmentRelationship containmentRelationship;
};

class QueryContainmentIdentificationTest : public Testing::BaseUnitTest,
                                           public testing::WithParamInterface<std::vector<QueryContainmentTestEntry>> {

  public:
    SchemaPtr schema;
    SchemaPtr schemaHouseholds;
    Catalogs::Source::SourceCatalogPtr sourceCatalog;
    std::shared_ptr<Catalogs::UDF::UDFCatalog> udfCatalog;

    /* Will be called before all tests in this class are started. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("QueryContainmentIdentificationTest.log", NES::LogLevel::LOG_DEBUG);
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

        sourceCatalog = std::make_shared<Catalogs::Source::SourceCatalog>();
        sourceCatalog->addLogicalSource("windTurbines", schema);
        sourceCatalog->addLogicalSource("solarPanels", schema);
        sourceCatalog->addLogicalSource("test", schema);
        sourceCatalog->addLogicalSource("households", schemaHouseholds);
        udfCatalog = Catalogs::UDF::UDFCatalog::create();
    }

    /* Will be called after a test is executed. */
    void TearDown() override {
        NES_DEBUG("QueryContainmentIdentificationTest: Tear down QueryContainmentIdentificationTest test case.");
    }

    static auto createEqualityCases() {
        return std::vector<QueryContainmentTestEntry>{
            QueryContainmentTestEntry("TestEquality",
                                      Query::from("windTurbines")
                                          .map(Attribute("value") = 40)
                                          .filter(Attribute("id") < 45)
                                          .filter(Attribute("id") < 45)
                                          .filter(Attribute("id") < 45)
                                          .filter(Attribute("id") < 45)
                                          .sink(PrintSinkDescriptor::create()),
                                      Query::from("windTurbines")
                                          .map(Attribute("value") = 40)
                                          .filter(Attribute("id") < 45)
                                          .filter(Attribute("id") < 45)
                                          .filter(Attribute("id") < 45)
                                          .filter(Attribute("id") < 45)
                                          .sink(PrintSinkDescriptor::create()),
                                      Optimizer::ContainmentRelationship::EQUALITY),
            QueryContainmentTestEntry("TestEquality",
                                      Query::from("windTurbines")
                                          .map(Attribute("value") = 40)
                                          .filter(Attribute("id") < 45)
                                          .filter(Attribute("id") < 45)
                                          .filter(Attribute("id") < 45)
                                          .filter(Attribute("id") < 45)
                                          .sink(PrintSinkDescriptor::create()),
                                      Query::from("windTurbines")
                                          .map(Attribute("value") = 40)
                                          .filter(Attribute("id") < 45)
                                          .filter(Attribute("id") < 45)
                                          .filter(Attribute("id") < 45)
                                          .filter(Attribute("id") < 45)
                                          .sink(PrintSinkDescriptor::create()),
                                      Optimizer::ContainmentRelationship::EQUALITY),
            QueryContainmentTestEntry("TestEquality",
                                      Query::from("windTurbines")
                                          .window(TumblingWindow::of(EventTime(Attribute("ts")), Seconds(10)))
                                          .apply(Sum(Attribute("value1")))
                                          .sink(PrintSinkDescriptor::create()),
                                      Query::from("windTurbines")
                                          .window(TumblingWindow::of(EventTime(Attribute("ts")), Seconds(10)))
                                          .apply(Sum(Attribute("value1")))
                                          .sink(PrintSinkDescriptor::create()),
                                      Optimizer::ContainmentRelationship::EQUALITY),
            QueryContainmentTestEntry("TestEquality",
                                      Query::from("windTurbines")
                                          .joinWith(Query::from("solarPanels"))
                                          .where(Attribute("id1"))
                                          .equalsTo(Attribute("id"))
                                          .window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000)))
                                          .sink(PrintSinkDescriptor::create()),
                                      Query::from("windTurbines")
                                          .joinWith(Query::from("solarPanels"))
                                          .where(Attribute("id1"))
                                          .equalsTo(Attribute("id"))
                                          .window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000)))
                                          .sink(PrintSinkDescriptor::create()),
                                      Optimizer::ContainmentRelationship::EQUALITY),
            QueryContainmentTestEntry("TestEquality",
                                      Query::from("windTurbines")
                                          .filter(Attribute("value") < 5)
                                          .joinWith(Query::from("solarPanels"))
                                          .where(Attribute("id1"))
                                          .equalsTo(Attribute("id"))
                                          .window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000)))
                                          .sink(PrintSinkDescriptor::create()),
                                      Query::from("windTurbines")
                                          .joinWith(Query::from("solarPanels"))
                                          .where(Attribute("id1"))
                                          .equalsTo(Attribute("id"))
                                          .window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000)))
                                          .filter(Attribute("value") < 5)
                                          .sink(PrintSinkDescriptor::create()),
                                      Optimizer::ContainmentRelationship::EQUALITY),
            QueryContainmentTestEntry(
                "TestEquality",
                Query::from("windTurbines")
                    .joinWith(Query::from("solarPanels"))
                    .where(Attribute("id1"))
                    .equalsTo(Attribute("id"))
                    .window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000)))
                    .sink(PrintSinkDescriptor::create()),
                Query::from("windTurbines")
                    .joinWith(Query::from("solarPanels"))
                    .where(Attribute("id1"))
                    .equalsTo(Attribute("id"))
                    .window(SlidingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000), Milliseconds(1000)))
                    .sink(PrintSinkDescriptor::create()),
                Optimizer::ContainmentRelationship::EQUALITY),
            QueryContainmentTestEntry(
                "TestEquality",
                Query::from("windTurbines")
                    .window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000)))
                    .apply(Sum(Attribute("value")))
                    .sink(PrintSinkDescriptor::create()),
                Query::from("windTurbines")
                    .window(SlidingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000), Milliseconds(1000)))
                    .apply(Sum(Attribute("value")))
                    .sink(PrintSinkDescriptor::create()),
                Optimizer::ContainmentRelationship::EQUALITY),
            QueryContainmentTestEntry("TestEquality",
                                      Query::from("windTurbines")
                                          .window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000)))
                                          .apply(Count())
                                          .sink(PrintSinkDescriptor::create()),
                                      Query::from("windTurbines")
                                          .window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000)))
                                          .apply(Count())
                                          .sink(PrintSinkDescriptor::create()),
                                      Optimizer::ContainmentRelationship::EQUALITY),
            QueryContainmentTestEntry(
                "TestEquality",
                Query::from("windTurbines").unionWith(Query::from("solarPanels")).sink(PrintSinkDescriptor::create()),
                Query::from("windTurbines").unionWith(Query::from("solarPanels")).sink(PrintSinkDescriptor::create()),
                Optimizer::ContainmentRelationship::EQUALITY),
            QueryContainmentTestEntry(
                "TestEquality",
                Query::from("windTurbines")
                    .unionWith(Query::from("solarPanels"))
                    .filter(Attribute("value") > 4)
                    .project(Attribute("value"), Attribute("id1"), Attribute("value1"), Attribute("ts"))
                    .joinWith(Query::from("households")
                                  .project(Attribute("value"), Attribute("id"), Attribute("value1"), Attribute("ts")))
                    .where(Attribute("windTurbines$id1"))
                    .equalsTo(Attribute("households$id"))
                    .window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000)))
                    .sink(PrintSinkDescriptor::create()),
                Query::from("windTurbines")
                    .unionWith(Query::from("solarPanels"))
                    .joinWith(Query::from("households"))
                    .where(Attribute("id1"))
                    .equalsTo(Attribute("id"))
                    .window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000)))
                    .filter(Attribute("value") > 4)
                    .project(Attribute("windTurbines$value"),
                             Attribute("windTurbines$id1"),
                             Attribute("windTurbines$value1"),
                             Attribute("windTurbines$ts"),
                             Attribute("households$value"),
                             Attribute("households$id"),
                             Attribute("households$value1"),
                             Attribute("households$ts"))
                    .sink(PrintSinkDescriptor::create()),
                Optimizer::ContainmentRelationship::EQUALITY),
            QueryContainmentTestEntry(
                "TestEquality",
                Query::from("windTurbines")
                    .unionWith(Query::from("solarPanels"))
                    .window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000)))
                    .byKey(Attribute("id"))
                    .apply(Sum(Attribute("value")))
                    .filter(Attribute("value") > 4)
                    .joinWith(Query::from("households")
                                  .window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000)))
                                  .byKey(Attribute("id"))
                                  .apply(Sum(Attribute("value"))))
                    .where(Attribute("windTurbines$id"))
                    .equalsTo(Attribute("households$id"))
                    .window(TumblingWindow::of(EventTime(Attribute("start")), Hours(1)))
                    .sink(PrintSinkDescriptor::create()),
                Query::from("windTurbines")
                    .unionWith(Query::from("solarPanels"))
                    .window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000)))
                    .byKey(Attribute("id"))
                    .apply(Sum(Attribute("value")))
                    .joinWith(Query::from("households")
                                  .window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000)))
                                  .byKey(Attribute("id"))
                                  .apply(Sum(Attribute("value"))))
                    .where(Attribute("id"))
                    .equalsTo(Attribute("id"))
                    .window(TumblingWindow::of(EventTime(Attribute("start")), Hours(1)))
                    .filter(Attribute("windTurbines$value") > 4)
                    .sink(PrintSinkDescriptor::create()),
                Optimizer::ContainmentRelationship::EQUALITY),
            QueryContainmentTestEntry(
                "TestEquality",
                Query::from("windTurbines")
                    .unionWith(Query::from("solarPanels"))
                    .window(TumblingWindow::of(EventTime(Attribute("ts")), Hours(1)))
                    .byKey(Attribute("id"))
                    .apply(Sum(Attribute("value")))
                    .joinWith(
                        Query::from("households").project(Attribute("value3"), Attribute("id"), Attribute("ts").as("start")))
                    .where(Attribute("id"))
                    .equalsTo(Attribute("id"))
                    .window(TumblingWindow::of(EventTime(Attribute("start")), Hours(1)))
                    .map(Attribute("DifferenceProducedConsumedPower") = Attribute("value") - Attribute("value3"))
                    .project(Attribute("DifferenceProducedConsumedPower"), Attribute("value"), Attribute("value3"))
                    .filter(Attribute("DifferenceProducedConsumedPower") < 100)
                    .sink(PrintSinkDescriptor::create()),
                Query::from("windTurbines")
                    .unionWith(Query::from("solarPanels"))
                    .window(TumblingWindow::of(EventTime(Attribute("ts")), Hours(1)))
                    .byKey(Attribute("id"))
                    .apply(Sum(Attribute("value")))
                    .joinWith(
                        Query::from("households").project(Attribute("value3"), Attribute("id"), Attribute("ts").as("start")))
                    .where(Attribute("id"))
                    .equalsTo(Attribute("id"))
                    .window(TumblingWindow::of(EventTime(Attribute("start")), Hours(1)))
                    .map(Attribute("DifferenceProducedConsumedPower") = Attribute("value") - Attribute("value3"))
                    .project(Attribute("DifferenceProducedConsumedPower"), Attribute("value"), Attribute("value3"))
                    .filter(Attribute("DifferenceProducedConsumedPower") < 100)
                    .sink(PrintSinkDescriptor::create()),
                Optimizer::ContainmentRelationship::EQUALITY),
        };
    }

    static auto createNoContainmentCases() {
        return std::vector<QueryContainmentTestEntry>{
            QueryContainmentTestEntry(
                "TestNoContainment",
                Query::from("windTurbines")
                    .map(Attribute("value") = 40)
                    .project(Attribute("value").as("newValue"))
                    .sink(PrintSinkDescriptor::create()),
                Query::from("windTurbines").map(Attribute("value") = 40).sink(PrintSinkDescriptor::create()),
                Optimizer::ContainmentRelationship::NO_CONTAINMENT),
            QueryContainmentTestEntry("TestNoContainment",
                                      Query::from("windTurbines")
                                          .map(Attribute("value") = 40)
                                          .filter(Attribute("id") < 45)
                                          .filter(Attribute("id") < 45)
                                          .filter(Attribute("id") < 45)
                                          .filter(Attribute("id") < 45)
                                          .sink(PrintSinkDescriptor::create()),
                                      Query::from("solarPanels")
                                          .map(Attribute("value") = 40)
                                          .filter(Attribute("id") < 45)
                                          .filter(Attribute("id") < 45)
                                          .filter(Attribute("id") < 45)
                                          .filter(Attribute("id") < 45)
                                          .sink(PrintSinkDescriptor::create()),
                                      Optimizer::ContainmentRelationship::NO_CONTAINMENT),
            QueryContainmentTestEntry(
                "TestNoContainment",
                Query::from("windTurbines")
                    .map(Attribute("value") = 40)
                    .map(Attribute("value") = Attribute("value") + 10)
                    .sink(PrintSinkDescriptor::create()),
                Query::from("windTurbines").map(Attribute("value") = 40).sink(PrintSinkDescriptor::create()),
                Optimizer::ContainmentRelationship::NO_CONTAINMENT),
            QueryContainmentTestEntry("TestNoContainment",
                                      Query::from("windTurbines")
                                          .filter(Attribute("value") < 40)
                                          .window(TumblingWindow::of(EventTime(Attribute("ts")), Seconds(20)))
                                          .apply(Sum(Attribute("value1")))
                                          .sink(PrintSinkDescriptor::create()),
                                      Query::from("windTurbines")
                                          .window(TumblingWindow::of(EventTime(Attribute("ts")), Seconds(10)))
                                          .apply(Sum(Attribute("value1")))
                                          .sink(PrintSinkDescriptor::create()),
                                      Optimizer::ContainmentRelationship::NO_CONTAINMENT),
            QueryContainmentTestEntry("TestNoContainment",
                                      Query::from("windTurbines")
                                          .joinWith(Query::from("solarPanels"))
                                          .where(Attribute("id1"))
                                          .equalsTo(Attribute("id"))
                                          .window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000)))
                                          .sink(PrintSinkDescriptor::create()),
                                      Query::from("windTurbines")
                                          .joinWith(Query::from("solarPanels"))
                                          .where(Attribute("id1"))
                                          .equalsTo(Attribute("id"))
                                          .window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(20)))
                                          .filter(Attribute("value") < 10)
                                          .sink(PrintSinkDescriptor::create()),
                                      Optimizer::ContainmentRelationship::NO_CONTAINMENT),
            QueryContainmentTestEntry("TestNoContainment",
                                      Query::from("windTurbines")
                                          .filter(Attribute("value") < 5)
                                          .joinWith(Query::from("solarPanels"))
                                          .where(Attribute("id1"))
                                          .equalsTo(Attribute("id"))
                                          .window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000)))
                                          .sink(PrintSinkDescriptor::create()),
                                      Query::from("windTurbines")
                                          .joinWith(Query::from("solarPanels"))
                                          .where(Attribute("id1"))
                                          .equalsTo(Attribute("id"))
                                          .window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(20)))
                                          .sink(PrintSinkDescriptor::create()),
                                      Optimizer::ContainmentRelationship::NO_CONTAINMENT),
            QueryContainmentTestEntry(
                "TestNoContainment",
                Query::from("windTurbines")
                    .window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000)))
                    .apply(Sum(Attribute("value")))
                    .sink(PrintSinkDescriptor::create()),
                Query::from("windTurbines")
                    .window(SlidingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000), Milliseconds(10)))
                    .byKey(Attribute("id"))
                    .apply(Sum(Attribute("value")))
                    .sink(PrintSinkDescriptor::create()),
                Optimizer::ContainmentRelationship::NO_CONTAINMENT),
            QueryContainmentTestEntry("TestNoContainment",
                                      Query::from("windTurbines")
                                          .window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000)))
                                          .apply(Sum(Attribute("value")))
                                          .window(TumblingWindow::of(EventTime(Attribute("start")), Milliseconds(10000)))
                                          .apply(Min(Attribute("value")))
                                          .sink(PrintSinkDescriptor::create()),
                                      Query::from("windTurbines")
                                          .window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000)))
                                          .apply(Sum(Attribute("value")))
                                          .sink(PrintSinkDescriptor::create()),
                                      Optimizer::ContainmentRelationship::NO_CONTAINMENT),
            QueryContainmentTestEntry("TestNoContainment",
                                      Query::from("windTurbines")
                                          .window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000)))
                                          .apply(Sum(Attribute("value")))
                                          .window(TumblingWindow::of(EventTime(Attribute("start")), Milliseconds(10000)))
                                          .apply(Min(Attribute("value")))
                                          .sink(PrintSinkDescriptor::create()),
                                      Query::from("windTurbines")
                                          .window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(100)))
                                          .apply(Sum(Attribute("value")))
                                          .window(TumblingWindow::of(EventTime(Attribute("start")), Milliseconds(1000)))
                                          .apply(Max(Attribute("value")))
                                          .sink(PrintSinkDescriptor::create()),
                                      Optimizer::ContainmentRelationship::NO_CONTAINMENT),
            QueryContainmentTestEntry("TestNoContainment",
                                      Query::from("windTurbines")
                                          .unionWith(Query::from("solarPanels"))
                                          .window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(10000)))
                                          .apply(Avg(Attribute("value")))
                                          .sink(PrintSinkDescriptor::create()),
                                      Query::from("windTurbines")
                                          .unionWith(Query::from("solarPanels"))
                                          .window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(10000)))
                                          .apply(Sum(Attribute("value")))
                                          .sink(PrintSinkDescriptor::create()),
                                      Optimizer::ContainmentRelationship::NO_CONTAINMENT),
            QueryContainmentTestEntry("TestNoContainment",
                                      Query::from("windTurbines")
                                          .unionWith(Query::from("solarPanels"))
                                          .window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(10000)))
                                          .apply(Avg(Attribute("value")))
                                          .sink(PrintSinkDescriptor::create()),
                                      Query::from("windTurbines")
                                          .unionWith(Query::from("solarPanels"))
                                          .window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(100)))
                                          .apply(Sum(Attribute("value")))
                                          .sink(PrintSinkDescriptor::create()),
                                      Optimizer::ContainmentRelationship::NO_CONTAINMENT),
            QueryContainmentTestEntry("TestNoContainment",
                                      Query::from("windTurbines")
                                          .unionWith(Query::from("solarPanels"))
                                          .window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(10000)))
                                          .byKey(Attribute("id"))
                                          .apply(Sum(Attribute("value")))
                                          .sink(PrintSinkDescriptor::create()),
                                      Query::from("windTurbines")
                                          .unionWith(Query::from("solarPanels"))
                                          .window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(10000)))
                                          .apply(Sum(Attribute("value")))
                                          .sink(PrintSinkDescriptor::create()),
                                      Optimizer::ContainmentRelationship::NO_CONTAINMENT),
            QueryContainmentTestEntry("TestNoContainment",
                                      Query::from("windTurbines")
                                          .unionWith(Query::from("solarPanels"))
                                          .window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(10000)))
                                          .apply(Sum(Attribute("value")))
                                          .filter(Attribute("value") == 1)
                                          .sink(PrintSinkDescriptor::create()),
                                      Query::from("windTurbines")
                                          .unionWith(Query::from("solarPanels"))
                                          .window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(100)))
                                          .apply(Sum(Attribute("value")))
                                          .sink(PrintSinkDescriptor::create()),
                                      Optimizer::ContainmentRelationship::NO_CONTAINMENT),
            QueryContainmentTestEntry("TestNoContainment",
                                      Query::from("windTurbines")
                                          .unionWith(Query::from("solarPanels"))
                                          .window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(10000)))
                                          .apply(Sum(Attribute("value")))
                                          .filter(Attribute("value") != 1)
                                          .sink(PrintSinkDescriptor::create()),
                                      Query::from("windTurbines")
                                          .filter(Attribute("value") == 1)
                                          .unionWith(Query::from("solarPanels"))
                                          .window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(10000)))
                                          .apply(Sum(Attribute("value")))
                                          .sink(PrintSinkDescriptor::create()),
                                      Optimizer::ContainmentRelationship::NO_CONTAINMENT),
            QueryContainmentTestEntry("TestNoContainment",
                                      Query::from("windTurbines")
                                          .filter(Attribute("value") == 5)
                                          .unionWith(Query::from("solarPanels"))
                                          .joinWith(Query::from("households"))
                                          .where(Attribute("id1"))
                                          .equalsTo(Attribute("id"))
                                          .window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000)))
                                          .sink(PrintSinkDescriptor::create()),
                                      Query::from("windTurbines")
                                          .joinWith(Query::from("households"))
                                          .where(Attribute("id1"))
                                          .equalsTo(Attribute("id"))
                                          .window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000)))
                                          .filter(Attribute("value") != 4)
                                          .sink(PrintSinkDescriptor::create()),
                                      Optimizer::ContainmentRelationship::NO_CONTAINMENT),
            QueryContainmentTestEntry("TestNoContainment",
                                      Query::from("windTurbines")
                                          .filter(Attribute("value") != 4)
                                          .unionWith(Query::from("solarPanels"))
                                          .joinWith(Query::from("households"))
                                          .where(Attribute("id1"))
                                          .equalsTo(Attribute("id"))
                                          .window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000)))
                                          .sink(PrintSinkDescriptor::create()),
                                      Query::from("windTurbines")
                                          .unionWith(Query::from("solarPanels"))
                                          .joinWith(Query::from("households"))
                                          .where(Attribute("id1"))
                                          .equalsTo(Attribute("id"))
                                          .window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000)))
                                          .filter(Attribute("value") > 4)
                                          .sink(PrintSinkDescriptor::create()),
                                      Optimizer::ContainmentRelationship::NO_CONTAINMENT),
            QueryContainmentTestEntry("TestNoContainment",
                                      Query::from("windTurbines")
                                          .unionWith(Query::from("solarPanels"))
                                          .filter(Attribute("value") > 4)
                                          .joinWith(Query::from("households"))
                                          .where(Attribute("windTurbines$id1"))
                                          .equalsTo(Attribute("households$id"))
                                          .window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(100)))
                                          .sink(PrintSinkDescriptor::create()),
                                      Query::from("windTurbines")
                                          .unionWith(Query::from("solarPanels"))
                                          .joinWith(Query::from("households"))
                                          .where(Attribute("id1"))
                                          .equalsTo(Attribute("id"))
                                          .window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000)))
                                          .filter(Attribute("value") > 4)
                                          .sink(PrintSinkDescriptor::create()),
                                      Optimizer::ContainmentRelationship::NO_CONTAINMENT),
            //Limit of our algorithm, cannot detect equivalence among these windows
            QueryContainmentTestEntry(
                "TestNoContainment",
                Query::from("windTurbines")
                    .unionWith(Query::from("solarPanels"))
                    .filter(Attribute("value") > 4)
                    .joinWith(Query::from("households"))
                    .where(Attribute("windTurbines$id"))
                    .equalsTo(Attribute("households$id"))
                    .window(TumblingWindow::of(EventTime(Attribute("ts")), Hours(1)))
                    .window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000)))
                    .byKey(Attribute("id"))
                    .apply(Sum(Attribute("value")))
                    .sink(PrintSinkDescriptor::create()),
                Query::from("windTurbines")
                    .unionWith(Query::from("solarPanels"))
                    .window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000)))
                    .byKey(Attribute("id"))
                    .apply(Sum(Attribute("value")))
                    .joinWith(Query::from("households")
                                  .window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000)))
                                  .byKey(Attribute("id"))
                                  .apply(Sum(Attribute("value"))))
                    .where(Attribute("id"))
                    .equalsTo(Attribute("id"))
                    .window(TumblingWindow::of(EventTime(Attribute("start")), Hours(1)))
                    .filter(Attribute("windTurbines$value") > 4)
                    .sink(PrintSinkDescriptor::create()),
                Optimizer::ContainmentRelationship::NO_CONTAINMENT),
            QueryContainmentTestEntry("TestNoContainment",
                                      Query::from("windTurbines")
                                          .unionWith(Query::from("solarPanels"))
                                          .filter(Attribute("value") > 4)
                                          .joinWith(Query::from("households"))
                                          .where(Attribute("windTurbines$id1"))
                                          .equalsTo(Attribute("households$id"))
                                          .window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000)))
                                          .sink(PrintSinkDescriptor::create()),
                                      Query::from("windTurbines")
                                          .unionWith(Query::from("solarPanels"))
                                          .joinWith(Query::from("households"))
                                          .where(Attribute("id1"))
                                          .equalsTo(Attribute("id"))
                                          .window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000)))
                                          .project(Attribute("windTurbines$value"))
                                          .sink(PrintSinkDescriptor::create()),
                                      Optimizer::ContainmentRelationship::NO_CONTAINMENT),
            QueryContainmentTestEntry("TestNoContainment",
                                      Query::from("windTurbines")
                                          .unionWith(Query::from("solarPanels"))
                                          .joinWith(Query::from("households"))
                                          .where(Attribute("windTurbines$id1"))
                                          .equalsTo(Attribute("households$id"))
                                          .window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(10000)))
                                          .sink(PrintSinkDescriptor::create()),
                                      Query::from("windTurbines")
                                          .unionWith(Query::from("solarPanels"))
                                          .joinWith(Query::from("households"))
                                          .where(Attribute("id1"))
                                          .equalsTo(Attribute("id"))
                                          .window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000)))
                                          .project(Attribute("windTurbines$value"))
                                          .sink(PrintSinkDescriptor::create()),
                                      Optimizer::ContainmentRelationship::NO_CONTAINMENT),
            QueryContainmentTestEntry("TestNoContainment",
                                      Query::from("windTurbines")
                                          .joinWith(Query::from("solarPanels"))
                                          .where(Attribute("id1"))
                                          .equalsTo(Attribute("id"))
                                          .window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(10)))
                                          .sink(PrintSinkDescriptor::create()),
                                      Query::from("windTurbines")
                                          .joinWith(Query::from("solarPanels"))
                                          .where(Attribute("id1"))
                                          .equalsTo(Attribute("id"))
                                          .window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000)))
                                          .sink(PrintSinkDescriptor::create()),
                                      Optimizer::ContainmentRelationship::NO_CONTAINMENT),
            QueryContainmentTestEntry("TestNoContainment",
                                      Query::from("windTurbines")
                                          .joinWith(Query::from("solarPanels"))
                                          .where(Attribute("id1"))
                                          .equalsTo(Attribute("id"))
                                          .window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000)))
                                          .sink(PrintSinkDescriptor::create()),
                                      Query::from("windTurbines")
                                          .joinWith(Query::from("solarPanels"))
                                          .where(Attribute("id1"))
                                          .equalsTo(Attribute("id"))
                                          .window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(20)))
                                          .sink(PrintSinkDescriptor::create()),
                                      Optimizer::ContainmentRelationship::NO_CONTAINMENT),
            QueryContainmentTestEntry("TestNoContainment",
                                      Query::from("windTurbines")
                                          .unionWith(Query::from("solarPanels"))
                                          .filter(Attribute("value") > 4)
                                          .joinWith(Query::from("households"))
                                          .where(Attribute("windTurbines$id1"))
                                          .equalsTo(Attribute("households$id"))
                                          .window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(100)))
                                          .sink(PrintSinkDescriptor::create()),
                                      Query::from("windTurbines")
                                          .unionWith(Query::from("solarPanels"))
                                          .joinWith(Query::from("households"))
                                          .where(Attribute("id1"))
                                          .equalsTo(Attribute("id"))
                                          .window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000)))
                                          .filter(Attribute("windTurbines$value") > 4)
                                          .sink(PrintSinkDescriptor::create()),
                                      Optimizer::ContainmentRelationship::NO_CONTAINMENT),
            //projection conditions differ too much for containment to be picked up in the complete query
            //bottom up approach would detect it, though
            QueryContainmentTestEntry("TestNoContainment",
                                      Query::from("windTurbines")
                                          .unionWith(Query::from("solarPanels"))
                                          .joinWith(Query::from("households"))
                                          .where(Attribute("windTurbines$id1"))
                                          .equalsTo(Attribute("households$id"))
                                          .window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000)))
                                          .sink(PrintSinkDescriptor::create()),
                                      Query::from("windTurbines")
                                          .unionWith(Query::from("solarPanels"))
                                          .joinWith(Query::from("households"))
                                          .where(Attribute("id1"))
                                          .equalsTo(Attribute("id"))
                                          .window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000)))
                                          .window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000)))
                                          .byKey(Attribute("id"))
                                          .apply(Sum(Attribute("value")))
                                          .sink(PrintSinkDescriptor::create()),
                                      Optimizer::ContainmentRelationship::NO_CONTAINMENT),
            //another limit of our algorithm. If one query does not have a window, we cannot detect containment
            QueryContainmentTestEntry(
                "TestNoContainment",
                Query::from("windTurbines").unionWith(Query::from("solarPanels")).sink(PrintSinkDescriptor::create()),
                Query::from("windTurbines")
                    .unionWith(Query::from("solarPanels"))
                    .window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000)))
                    .byKey(Attribute("id"))
                    .apply(Sum(Attribute("value")))
                    .sink(PrintSinkDescriptor::create()),
                Optimizer::ContainmentRelationship::NO_CONTAINMENT),
            QueryContainmentTestEntry("TestNoContainment",
                                      Query::from("windTurbines")
                                          .unionWith(Query::from("solarPanels"))
                                          .window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(10)))
                                          .byKey(Attribute("id"))
                                          .apply(Sum(Attribute("value")))
                                          .sink(PrintSinkDescriptor::create()),
                                      Query::from("windTurbines")
                                          .unionWith(Query::from("solarPanels"))
                                          .window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(10)))
                                          .byKey(Attribute("id"))
                                          .apply(Sum(Attribute("value")))
                                          .window(TumblingWindow::of(EventTime(Attribute("start")), Milliseconds(1000)))
                                          .byKey(Attribute("id"))
                                          .apply(Min(Attribute("value")))
                                          .sink(PrintSinkDescriptor::create()),
                                      Optimizer::ContainmentRelationship::NO_CONTAINMENT)};
    }

    static auto createFilterContainmentCases() {
        return std::vector<QueryContainmentTestEntry>{
            QueryContainmentTestEntry("TestFilterContainment",
                                      Query::from("windTurbines")
                                          .map(Attribute("value") = 40)
                                          .filter(Attribute("id") < 45)
                                          .sink(PrintSinkDescriptor::create()),
                                      Query::from("windTurbines")
                                          .map(Attribute("value") = 40)
                                          .filter(Attribute("id") < 60)
                                          .sink(PrintSinkDescriptor::create()),
                                      Optimizer::ContainmentRelationship::LEFT_SIG_CONTAINED),
            QueryContainmentTestEntry("TestFilterContainment",
                                      Query::from("windTurbines")
                                          .map(Attribute("value") = 40)
                                          .filter(Attribute("id") < 60)
                                          .sink(PrintSinkDescriptor::create()),
                                      Query::from("windTurbines")
                                          .map(Attribute("value") = 40)
                                          .filter(Attribute("id") < 45)
                                          .sink(PrintSinkDescriptor::create()),
                                      Optimizer::ContainmentRelationship::RIGHT_SIG_CONTAINED),
            QueryContainmentTestEntry("TestFilterContainment",
                                      Query::from("windTurbines")
                                          .joinWith(Query::from("solarPanels"))
                                          .where(Attribute("id1"))
                                          .equalsTo(Attribute("id"))
                                          .window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000)))
                                          .sink(PrintSinkDescriptor::create()),
                                      Query::from("windTurbines")
                                          .joinWith(Query::from("solarPanels"))
                                          .where(Attribute("id1"))
                                          .equalsTo(Attribute("id"))
                                          .window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000)))
                                          .filter(Attribute("value") < 5)
                                          .sink(PrintSinkDescriptor::create()),
                                      Optimizer::ContainmentRelationship::RIGHT_SIG_CONTAINED),
            QueryContainmentTestEntry("TestFilterContainment",
                                      Query::from("windTurbines")
                                          .filter(Attribute("value") < 5)
                                          .joinWith(Query::from("solarPanels"))
                                          .where(Attribute("id1"))
                                          .equalsTo(Attribute("id"))
                                          .window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000)))
                                          .sink(PrintSinkDescriptor::create()),
                                      Query::from("windTurbines")
                                          .joinWith(Query::from("solarPanels"))
                                          .where(Attribute("id1"))
                                          .equalsTo(Attribute("id"))
                                          .window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000)))
                                          .sink(PrintSinkDescriptor::create()),
                                      Optimizer::ContainmentRelationship::LEFT_SIG_CONTAINED),
            QueryContainmentTestEntry("TestFilterContainment",
                                      Query::from("windTurbines")
                                          .filter(Attribute("value") == 5)
                                          .joinWith(Query::from("households"))
                                          .where(Attribute("id1"))
                                          .equalsTo(Attribute("id"))
                                          .window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000)))
                                          .sink(PrintSinkDescriptor::create()),
                                      Query::from("windTurbines")
                                          .joinWith(Query::from("households"))
                                          .where(Attribute("id1"))
                                          .equalsTo(Attribute("id"))
                                          .window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000)))
                                          .filter(Attribute("value") != 4)
                                          .sink(PrintSinkDescriptor::create()),
                                      Optimizer::ContainmentRelationship::LEFT_SIG_CONTAINED),
            QueryContainmentTestEntry("TestFilterContainment",
                                      Query::from("windTurbines")
                                          .filter(Attribute("value") == 5)
                                          .unionWith(Query::from("solarPanels"))
                                          .unionWith(Query::from("test"))
                                          .sink(PrintSinkDescriptor::create()),
                                      Query::from("windTurbines")
                                          .filter(Attribute("value") == 5)
                                          .unionWith(Query::from("solarPanels"))
                                          .unionWith(Query::from("test"))
                                          .filter(Attribute("value") >= 6)
                                          .sink(PrintSinkDescriptor::create()),
                                      Optimizer::ContainmentRelationship::RIGHT_SIG_CONTAINED),
            QueryContainmentTestEntry("TestFilterContainment",
                                      Query::from("windTurbines")
                                          .unionWith(Query::from("solarPanels"))
                                          .filter(Attribute("value") != 4)
                                          .joinWith(Query::from("households"))
                                          .where(Attribute("id1"))
                                          .equalsTo(Attribute("id"))
                                          .window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000)))
                                          .sink(PrintSinkDescriptor::create()),
                                      Query::from("windTurbines")
                                          .unionWith(Query::from("solarPanels"))
                                          .joinWith(Query::from("households"))
                                          .where(Attribute("id1"))
                                          .equalsTo(Attribute("id"))
                                          .window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000)))
                                          .filter(Attribute("value") > 4)
                                          .sink(PrintSinkDescriptor::create()),
                                      Optimizer::ContainmentRelationship::RIGHT_SIG_CONTAINED),
            QueryContainmentTestEntry("TestProjectionContainment",
                                      Query::from("windTurbines")
                                          .map(Attribute("value") = 5 * Attribute("value"))
                                          .map(Attribute("value1") = Attribute("value") + 10)
                                          .project(Attribute("value"), Attribute("id1"), Attribute("value1"), Attribute("ts"))
                                          .sink(PrintSinkDescriptor::create()),
                                      Query::from("windTurbines")
                                          .map(Attribute("id") = 5 * Attribute("value"))
                                          .map(Attribute("value1") = Attribute("value") + 10)
                                          .project(Attribute("id1"), Attribute("value1"), Attribute("ts"))
                                          .sink(PrintSinkDescriptor::create()),
                                      Optimizer::ContainmentRelationship::NO_CONTAINMENT),
            QueryContainmentTestEntry(
                "TestProjectionContainment",
                Query::from("windTurbines")
                    .unionWith(Query::from("solarPanels").map(Attribute("value") = 5 * Attribute("value")))
                    .filter(Attribute("value") > 4)
                    .project(Attribute("value"), Attribute("id1"), Attribute("value1"), Attribute("ts"))
                    .joinWith(Query::from("households")
                                  .project(Attribute("value"), Attribute("id"), Attribute("value1"), Attribute("ts")))
                    .where(Attribute("windTurbines$id1"))
                    .equalsTo(Attribute("households$id"))
                    .window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000)))
                    .sink(PrintSinkDescriptor::create()),
                Query::from("windTurbines")
                    .unionWith(Query::from("solarPanels"))
                    .joinWith(Query::from("households"))
                    .where(Attribute("id1"))
                    .equalsTo(Attribute("id"))
                    .window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000)))
                    .filter(Attribute("value") > 4)
                    .project(Attribute("windTurbines$value"),
                             Attribute("windTurbines$id1"),
                             Attribute("windTurbines$value1"),
                             Attribute("households$value"),
                             Attribute("households$id"),
                             Attribute("households$value1"),
                             Attribute("windTurbines$ts"),
                             Attribute("households$ts"))
                    .sink(PrintSinkDescriptor::create()),
                Optimizer::ContainmentRelationship::NO_CONTAINMENT),
            QueryContainmentTestEntry(
                "TestProjectionContainment",
                Query::from("windTurbines")
                    .project(Attribute("value").as("newValue"), Attribute("id1"), Attribute("ts"))
                    .sink(PrintSinkDescriptor::create()),
                Query::from("windTurbines")
                    .project(Attribute("value"), Attribute("id1"), Attribute("value1"), Attribute("ts"), Attribute("id"))
                    .sink(PrintSinkDescriptor::create()),
                Optimizer::ContainmentRelationship::NO_CONTAINMENT)};
    }

    static auto createProjectionContainmentCases() {
        return std::vector<QueryContainmentTestEntry>{
            QueryContainmentTestEntry("TestProjectionContainment",
                                      Query::from("windTurbines").project(Attribute("value")).sink(PrintSinkDescriptor::create()),
                                      Query::from("windTurbines").sink(PrintSinkDescriptor::create()),
                                      Optimizer::ContainmentRelationship::LEFT_SIG_CONTAINED),
            QueryContainmentTestEntry("TestProjectionContainment",
                                      Query::from("windTurbines")
                                          .map(Attribute("value") = 10)
                                          .map(Attribute("id") = 10)
                                          .map(Attribute("ts") = 10)
                                          .project(Attribute("value"), Attribute("id"))
                                          .sink(PrintSinkDescriptor::create()),
                                      Query::from("windTurbines")
                                          .map(Attribute("value") = 10)
                                          .map(Attribute("id") = 10)
                                          .map(Attribute("ts") = 10)
                                          .project(Attribute("value"), Attribute("id"), Attribute("ts"))
                                          .sink(PrintSinkDescriptor::create()),
                                      Optimizer::ContainmentRelationship::LEFT_SIG_CONTAINED),
            QueryContainmentTestEntry(
                "TestProjectionContainment",
                Query::from("windTurbines")
                    .map(Attribute("value") = 40)
                    .project(Attribute("value"))
                    .sink(PrintSinkDescriptor::create()),
                Query::from("windTurbines").map(Attribute("value") = 40).sink(PrintSinkDescriptor::create()),
                Optimizer::ContainmentRelationship::LEFT_SIG_CONTAINED),
            QueryContainmentTestEntry(
                "TestProjectionContainment",
                Query::from("windTurbines").map(Attribute("value") = 40).sink(PrintSinkDescriptor::create()),
                Query::from("windTurbines")
                    .map(Attribute("value") = 40)
                    .project(Attribute("value"))
                    .sink(PrintSinkDescriptor::create()),
                Optimizer::ContainmentRelationship::RIGHT_SIG_CONTAINED),
            QueryContainmentTestEntry(
                "TestProjectionContainment",
                Query::from("windTurbines")
                    .unionWith(Query::from("solarPanels"))
                    .filter(Attribute("value") > 4)
                    .project(Attribute("value"), Attribute("id1"), Attribute("value1"), Attribute("ts"))
                    .joinWith(Query::from("households")
                                  .project(Attribute("value"), Attribute("id"), Attribute("value1"), Attribute("ts")))
                    .where(Attribute("windTurbines$id1"))
                    .equalsTo(Attribute("households$id"))
                    .window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000)))
                    .sink(PrintSinkDescriptor::create()),
                Query::from("windTurbines")
                    .unionWith(Query::from("solarPanels"))
                    .joinWith(Query::from("households"))
                    .where(Attribute("id1"))
                    .equalsTo(Attribute("id"))
                    .window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000)))
                    .filter(Attribute("value") > 4)
                    .project(Attribute("windTurbines$value"),
                             Attribute("windTurbines$id1"),
                             Attribute("households$value"),
                             Attribute("households$id"),
                             Attribute("windTurbines$ts"),
                             Attribute("households$ts"))
                    .sink(PrintSinkDescriptor::create()),
                Optimizer::ContainmentRelationship::RIGHT_SIG_CONTAINED),
            QueryContainmentTestEntry("TestProjectionContainment",
                                      Query::from("windTurbines")
                                          .unionWith(Query::from("solarPanels"))
                                          .joinWith(Query::from("households"))
                                          .where(Attribute("windTurbines$id1"))
                                          .equalsTo(Attribute("households$id"))
                                          .window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000)))
                                          .sink(PrintSinkDescriptor::create()),
                                      Query::from("windTurbines")
                                          .unionWith(Query::from("solarPanels"))
                                          .joinWith(Query::from("households"))
                                          .where(Attribute("id1"))
                                          .equalsTo(Attribute("id"))
                                          .window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000)))
                                          .project(Attribute("windTurbines$value"))
                                          .sink(PrintSinkDescriptor::create()),
                                      Optimizer::ContainmentRelationship::RIGHT_SIG_CONTAINED),
            QueryContainmentTestEntry(
                "TestProjectionContainment",
                Query::from("windTurbines")
                    .unionWith(Query::from("solarPanels"))
                    .filter(Attribute("value") > 4)
                    .project(Attribute("value"), Attribute("id1"), Attribute("value1"), Attribute("ts"))
                    .joinWith(Query::from("households")
                                  .project(Attribute("value"), Attribute("id"), Attribute("value1"), Attribute("ts")))
                    .where(Attribute("windTurbines$id1"))
                    .equalsTo(Attribute("households$id"))
                    .window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000)))
                    .sink(PrintSinkDescriptor::create()),
                Query::from("windTurbines")
                    .unionWith(Query::from("solarPanels"))
                    .joinWith(Query::from("households"))
                    .where(Attribute("id1"))
                    .equalsTo(Attribute("id"))
                    .window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000)))
                    .filter(Attribute("value") > 4)
                    .project(Attribute("windTurbines$value"),
                             Attribute("windTurbines$id1"),
                             Attribute("windTurbines$value1"),
                             Attribute("households$value"),
                             Attribute("households$id"),
                             Attribute("households$value1"),
                             Attribute("windTurbines$ts"),
                             Attribute("households$ts"))
                    .sink(PrintSinkDescriptor::create()),
                Optimizer::ContainmentRelationship::RIGHT_SIG_CONTAINED)};
    }
    static auto createWindowContainmentCases() {
        return std::vector<QueryContainmentTestEntry>{
            QueryContainmentTestEntry("TestWindowContainment",
                                      Query::from("windTurbines")
                                          .window(TumblingWindow::of(EventTime(Attribute("ts")), Seconds(20)))
                                          .apply(Sum(Attribute("value1")))
                                          .sink(PrintSinkDescriptor::create()),
                                      Query::from("windTurbines")
                                          .window(TumblingWindow::of(EventTime(Attribute("ts")), Seconds(10)))
                                          .apply(Sum(Attribute("value1")))
                                          .sink(PrintSinkDescriptor::create()),
                                      Optimizer::ContainmentRelationship::LEFT_SIG_CONTAINED),
            QueryContainmentTestEntry("TestWindowContainment",
                                      Query::from("windTurbines")
                                          .window(TumblingWindow::of(EventTime(Attribute("ts")), Seconds(10)))
                                          .apply(Sum(Attribute("value1")))
                                          .sink(PrintSinkDescriptor::create()),
                                      Query::from("windTurbines")
                                          .window(TumblingWindow::of(EventTime(Attribute("ts")), Seconds(20)))
                                          .apply(Sum(Attribute("value1")))
                                          .sink(PrintSinkDescriptor::create()),
                                      Optimizer::ContainmentRelationship::RIGHT_SIG_CONTAINED),
            QueryContainmentTestEntry(
                "TestWindowContainment",
                Query::from("windTurbines")
                    .window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000)))
                    .apply(Sum(Attribute("value")), Min(Attribute("value"))->as(FieldAccessExpressionNode::create("newValue")))
                    .sink(PrintSinkDescriptor::create()),
                Query::from("windTurbines")
                    .window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000)))
                    .apply(Sum(Attribute("value")))
                    .sink(PrintSinkDescriptor::create()),
                Optimizer::ContainmentRelationship::RIGHT_SIG_CONTAINED),
            QueryContainmentTestEntry("TestWindowContainment",
                                      Query::from("windTurbines")
                                          .window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000)))
                                          .apply(Sum(Attribute("value")), Min(Attribute("value1")))
                                          .sink(PrintSinkDescriptor::create()),
                                      Query::from("windTurbines")
                                          .window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000)))
                                          .apply(Sum(Attribute("value")))
                                          .sink(PrintSinkDescriptor::create()),
                                      Optimizer::ContainmentRelationship::RIGHT_SIG_CONTAINED),
            QueryContainmentTestEntry("TestWindowContainment",
                                      Query::from("windTurbines")
                                          .window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000)))
                                          .apply(Sum(Attribute("value")))
                                          .sink(PrintSinkDescriptor::create()),
                                      Query::from("windTurbines")
                                          .window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000)))
                                          .apply(Sum(Attribute("value")), Min(Attribute("value1")))
                                          .sink(PrintSinkDescriptor::create()),
                                      Optimizer::ContainmentRelationship::LEFT_SIG_CONTAINED),
            QueryContainmentTestEntry("TestWindowContainment",
                                      Query::from("windTurbines")
                                          .window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000)))
                                          .apply(Sum(Attribute("value")))
                                          .window(TumblingWindow::of(EventTime(Attribute("start")), Milliseconds(10000)))
                                          .apply(Min(Attribute("value")))
                                          .sink(PrintSinkDescriptor::create()),
                                      Query::from("windTurbines")
                                          .window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000)))
                                          .apply(Sum(Attribute("value")))
                                          .window(TumblingWindow::of(EventTime(Attribute("start")), Milliseconds(100)))
                                          .apply(Min(Attribute("value")))
                                          .sink(PrintSinkDescriptor::create()),
                                      Optimizer::ContainmentRelationship::LEFT_SIG_CONTAINED),
            QueryContainmentTestEntry("TestWindowContainment",
                                      Query::from("windTurbines")
                                          .window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000)))
                                          .apply(Sum(Attribute("value")), Min(Attribute("value1")), Max(Attribute("value1")))
                                          .sink(PrintSinkDescriptor::create()),
                                      Query::from("windTurbines")
                                          .window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000)))
                                          .apply(Sum(Attribute("value")))
                                          .sink(PrintSinkDescriptor::create()),
                                      Optimizer::ContainmentRelationship::RIGHT_SIG_CONTAINED),
            QueryContainmentTestEntry("TestWindowContainment",
                                      Query::from("windTurbines")
                                          .window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000)))
                                          .apply(Sum(Attribute("value")))
                                          .sink(PrintSinkDescriptor::create()),
                                      Query::from("windTurbines")
                                          .window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000)))
                                          .apply(Sum(Attribute("value")), Min(Attribute("value1")), Max(Attribute("value1")))
                                          .sink(PrintSinkDescriptor::create()),
                                      Optimizer::ContainmentRelationship::LEFT_SIG_CONTAINED),
            QueryContainmentTestEntry("TestWindowContainment",
                                      Query::from("windTurbines")
                                          .unionWith(Query::from("solarPanels"))
                                          .window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000)))
                                          .byKey(Attribute("id"))
                                          .apply(Sum(Attribute("value")))
                                          .sink(PrintSinkDescriptor::create()),
                                      Query::from("windTurbines")
                                          .unionWith(Query::from("solarPanels"))
                                          .window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(100)))
                                          .byKey(Attribute("id"))
                                          .apply(Sum(Attribute("value")), Min(Attribute("value1")), Max(Attribute("value2")))
                                          .sink(PrintSinkDescriptor::create()),
                                      Optimizer::ContainmentRelationship::LEFT_SIG_CONTAINED),
            QueryContainmentTestEntry(
                "TestWindowContainment",
                Query::from("windTurbines")
                    .unionWith(Query::from("solarPanels"))
                    .window(SlidingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000), Milliseconds(10)))
                    .byKey(Attribute("id"))
                    .apply(Sum(Attribute("value")))
                    .sink(PrintSinkDescriptor::create()),
                Query::from("windTurbines")
                    .unionWith(Query::from("solarPanels"))
                    .window(SlidingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000), Milliseconds(20)))
                    .byKey(Attribute("id"))
                    .apply(Sum(Attribute("value")))
                    .sink(PrintSinkDescriptor::create()),
                Optimizer::ContainmentRelationship::NO_CONTAINMENT)};
    }
};

/**
 * @brief tests if the correct containment relationship is returned by the signature containment util
 */
TEST_P(QueryContainmentIdentificationTest, testContainmentIdentification) {

    auto containmentCases = GetParam();

    uint16_t counter = 0;
    for (auto containmentCase : containmentCases) {
        QueryPlanPtr queryPlanSQPQuery = containmentCase.leftQuery.getQueryPlan();
        QueryPlanPtr queryPlanNewQuery = containmentCase.rightQuery.getQueryPlan();
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
        auto signatureContainmentUtil = Optimizer::SignatureContainmentCheck::create(context, true);
        std::map<OperatorNodePtr, OperatorNodePtr> targetToHostSinkOperatorMap;
        auto sqpSink = queryPlanSQPQuery->getSinkOperators()[0];
        auto newSink = queryPlanNewQuery->getSinkOperators()[0];
        //Check if the host and target sink operator signatures have a containment relationship
        Optimizer::ContainmentRelationship containment =
            signatureContainmentUtil->checkContainmentForBottomUpMerging(sqpSink, newSink)->containmentRelationship;
        NES_TRACE("Z3SignatureBasedContainmentBasedCompleteQueryMergerRule: containment: {}", magic_enum::enum_name(containment));
        NES_TRACE("Query pairing number: {}", counter);
        ASSERT_EQ(containment, containmentCase.containmentRelationship);
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