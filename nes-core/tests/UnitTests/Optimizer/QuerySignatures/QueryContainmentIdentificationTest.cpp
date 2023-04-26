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
#include <Catalogs/Source/SourceCatalog.hpp>
#include <Catalogs/UDF/UdfCatalog.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Compiler/CPPCompiler/CPPCompiler.hpp>
#include <Compiler/JITCompilerBuilder.hpp>
#include <NesBaseTest.hpp>
#include <Operators/LogicalOperators/MapLogicalOperatorNode.hpp>
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

class QueryContainmentIdentificationTest
    : public Testing::TestWithErrorHandling<testing::Test>,
      public testing::WithParamInterface<
          std::tuple<std::string,
                     std::vector<std::tuple<std::tuple<std::string, std::string>, NES::Optimizer::ContainmentType>>>> {

  public:
    SchemaPtr schema;
    SchemaPtr schemaHouseholds;
    Catalogs::Source::SourceCatalogPtr sourceCatalog;
    std::shared_ptr<Catalogs::UDF::UdfCatalog> udfCatalog;
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
        sourceCatalog->addLogicalSource("households", schemaHouseholds);
        udfCatalog = Catalogs::UDF::UdfCatalog::create();
        auto cppCompiler = Compiler::CPPCompiler::create();
        auto jitCompiler = Compiler::JITCompilerBuilder().registerLanguageCompiler(cppCompiler).build();
        syntacticQueryValidation = Optimizer::SyntacticQueryValidation::create(QueryParsingService::create(jitCompiler));
    }

    /* Will be called before a test is executed. */
    void TearDown() override {
        NES_DEBUG("QueryContainmentIdentificationTest: Tear down QueryContainmentIdentificationTest test case.");
    }

    static auto createEqualityCases() {
        std::tuple<std::string, std::vector<std::tuple<std::tuple<std::string, std::string>, NES::Optimizer::ContainmentType>>>
            equality = {
                "equality",
                {//Equal
                 {std::tuple<std::string, std::string>(
                      R"(Query::from("windTurbines").map(Attribute("value") = 40).filter(Attribute("id") < 45).filter(Attribute("id") < 45))"
                      R"(.filter(Attribute("id") < 45).filter(Attribute("id") < 45).sink(PrintSinkDescriptor::create());)",
                      R"(Query::from("windTurbines").map(Attribute("value") = 40).filter(Attribute("id") < 45).filter(Attribute("id") < 45))"
                      R"(.filter(Attribute("id") < 45).filter(Attribute("id") < 45).sink(PrintSinkDescriptor::create());)"),
                  Optimizer::ContainmentType::EQUALITY},
                 //No containment due to different transformations
                 {std::tuple<std::string, std::string>(R"(Query::from("windTurbines"))"
                                                       R"(.window(TumblingWindow::of(EventTime(Attribute("ts")), Seconds(10))))"
                                                       R"(.apply(Sum(Attribute("value1"))))"
                                                       R"(.sink(PrintSinkDescriptor::create());)",
                                                       R"(Query::from("windTurbines"))"
                                                       R"(.window(TumblingWindow::of(EventTime(Attribute("ts")), Seconds(10))))"
                                                       R"(.apply(Sum(Attribute("value1"))))"
                                                       R"(.sink(PrintSinkDescriptor::create());)"),
                  Optimizer::ContainmentType::EQUALITY},
                 {std::tuple<std::string, std::string>(
                      R"(Query::from("windTurbines"))"
                      R"(.joinWith(Query::from("solarPanels")))"
                      R"(.where(Attribute("id1")))"
                      R"(.equalsTo(Attribute("id")))"
                      R"(.window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))))"
                      R"(.sink(PrintSinkDescriptor::create());)",
                      R"(Query::from("windTurbines"))"
                      R"(.joinWith(Query::from("solarPanels")))"
                      R"(.where(Attribute("id1")))"
                      R"(.equalsTo(Attribute("id")))"
                      R"(.window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))))"
                      R"(.sink(PrintSinkDescriptor::create());)"),
                  Optimizer::ContainmentType::EQUALITY},
                 {std::tuple<std::string, std::string>(
                      R"(Query::from("windTurbines"))"
                      R"(.filter(Attribute("value") < 5))"
                      R"(.joinWith(Query::from("solarPanels")))"
                      R"(.where(Attribute("id1")))"
                      R"(.equalsTo(Attribute("id")))"
                      R"(.window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))))"
                      R"(.sink(PrintSinkDescriptor::create());)",
                      R"(Query::from("windTurbines"))"
                      R"(.joinWith(Query::from("solarPanels")))"
                      R"(.where(Attribute("id1")))"
                      R"(.equalsTo(Attribute("id")))"
                      R"(.window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))))"
                      R"(.filter(Attribute("value") < 5))"
                      R"(.sink(PrintSinkDescriptor::create());)"),
                  Optimizer::ContainmentType::EQUALITY},
                 {std::tuple<std::string, std::string>(
                      R"(Query::from("windTurbines"))"
                      R"(.joinWith(Query::from("solarPanels")))"
                      R"(.where(Attribute("id1")))"
                      R"(.equalsTo(Attribute("id")))"
                      R"(.window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))))"
                      R"(.sink(PrintSinkDescriptor::create());)",
                      R"(Query::from("windTurbines"))"
                      R"(.joinWith(Query::from("solarPanels")))"
                      R"(.where(Attribute("id1")))"
                      R"(.equalsTo(Attribute("id")))"
                      R"(.window(SlidingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000), Milliseconds(1000))))"
                      R"(.sink(PrintSinkDescriptor::create());)"),
                  Optimizer::ContainmentType::EQUALITY},
                 {std::tuple<std::string, std::string>(
                      R"(Query::from("windTurbines"))"
                      R"(.window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))))"
                      R"(.apply(Sum(Attribute("value"))))"
                      R"(.sink(PrintSinkDescriptor::create());)",
                      R"(Query::from("windTurbines"))"
                      R"(.window(SlidingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000), Milliseconds(1000))))"
                      R"(.apply(Sum(Attribute("value"))))"
                      R"(.sink(PrintSinkDescriptor::create());)"),
                  Optimizer::ContainmentType::EQUALITY},
                 {std::tuple<std::string, std::string>(
                      R"(Query::from("windTurbines"))"
                      R"(.window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))))"
                      R"(.apply(Count()))"
                      R"(.sink(PrintSinkDescriptor::create());)",
                      R"(Query::from("windTurbines"))"
                      R"(.window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))))"
                      R"(.apply(Count()))"
                      R"(.sink(PrintSinkDescriptor::create());)"),
                  Optimizer::ContainmentType::EQUALITY},
                 {std::tuple<std::string, std::string>(R"(Query::from("windTurbines"))"
                                                       R"(.unionWith(Query::from("solarPanels")))"
                                                       R"(.sink(PrintSinkDescriptor::create());)",
                                                       R"(Query::from("windTurbines"))"
                                                       R"(.unionWith(Query::from("solarPanels")))"
                                                       R"(.sink(PrintSinkDescriptor::create());)"),
                  Optimizer::ContainmentType::EQUALITY},
                 {std::tuple<std::string, std::string>(
                      R"(Query::from("windTurbines"))"
                      R"(.unionWith(Query::from("solarPanels")))"
                      R"(.filter(Attribute("value") > 4))"
                      R"(.project(Attribute("value"), Attribute("id1"), Attribute("value1"), Attribute("ts")))"
                      R"(.joinWith(Query::from("households"))"
                      R"(.project(Attribute("value"), Attribute("id"), Attribute("value1"), Attribute("ts")))"
                      R"())"
                      R"(.where(Attribute("windTurbines$id1")))"
                      R"(.equalsTo(Attribute("households$id")))"
                      R"(.window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))))"
                      R"(.sink(PrintSinkDescriptor::create());)",
                      R"(Query::from("windTurbines"))"
                      R"(.unionWith(Query::from("solarPanels")))"
                      R"(.joinWith(Query::from("households")))"
                      R"(.where(Attribute("id1")))"
                      R"(.equalsTo(Attribute("id")))"
                      R"(.window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))))"
                      R"(.filter(Attribute("value") > 4))"
                      R"(.project(Attribute("windTurbines$value"), Attribute("windTurbines$id1"), Attribute("windTurbines$value1"), )"
                      R"(Attribute("windTurbines$ts"), Attribute("households$value"), Attribute("households$id"), )"
                      R"(Attribute("households$value1"), Attribute("households$ts")))"
                      R"(.sink(PrintSinkDescriptor::create());)"),
                  Optimizer::ContainmentType::EQUALITY},
                 {std::tuple<std::string, std::string>(
                      R"(Query::from("windTurbines"))"
                      R"(.unionWith(Query::from("solarPanels")))"
                      R"(.window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))))"
                      R"(.byKey(Attribute("id")))"
                      R"(.apply(Sum(Attribute("value"))))"
                      R"(.filter(Attribute("value") > 4))"
                      R"(.joinWith(Query::from("households"))"
                      R"(.window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))))"
                      R"(.byKey(Attribute("id")))"
                      R"(.apply(Sum(Attribute("value"))))"
                      R"())"
                      R"(.where(Attribute("windTurbines$id")))"
                      R"(.equalsTo(Attribute("households$id")))"
                      R"(.window(TumblingWindow::of(EventTime(Attribute("start")), Hours(1))))"
                      R"(.sink(PrintSinkDescriptor::create());)",
                      R"(Query::from("windTurbines"))"
                      R"(.unionWith(Query::from("solarPanels")))"
                      R"(.window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))))"
                      R"(.byKey(Attribute("id")))"
                      R"(.apply(Sum(Attribute("value"))))"
                      R"(.joinWith(Query::from("households"))"
                      R"(.window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))))"
                      R"(.byKey(Attribute("id")))"
                      R"(.apply(Sum(Attribute("value")))))"
                      R"(.where(Attribute("id")))"
                      R"(.equalsTo(Attribute("id")))"
                      R"(.window(TumblingWindow::of(EventTime(Attribute("start")), Hours(1))))"
                      R"(.filter(Attribute("windTurbines$value") > 4))"
                      R"(.sink(PrintSinkDescriptor::create());)"),
                  Optimizer::ContainmentType::EQUALITY}}};
        return equality;
    }

    static auto createNoContainmentCases() {
        std::tuple<std::string, std::vector<std::tuple<std::tuple<std::string, std::string>, NES::Optimizer::ContainmentType>>>
            noContainment = {
                "noContainment",
                {//Sig2 contains Sig1 but containment cannot be detected
                 {std::tuple<std::string, std::string>(
                      R"(Query::from("windTurbines"))"
                      R"(.map(Attribute("value") = 40))"
                      R"(.project(Attribute("value").as("newValue")))"
                      R"(.sink(PrintSinkDescriptor::create());)",
                      R"(Query::from("windTurbines").map(Attribute("value") = 40).sink(PrintSinkDescriptor::create());)"),
                  Optimizer::ContainmentType::NO_CONTAINMENT},//No containment due to different transformations
                 {std::tuple<std::string, std::string>(
                      R"(Query::from("windTurbines"))"
                      R"(.map(Attribute("value") = 40))"
                      R"(.map(Attribute("value") = Attribute("value") + 10))"
                      R"(.sink(PrintSinkDescriptor::create());)",
                      R"(Query::from("windTurbines").map(Attribute("value") = 40).sink(PrintSinkDescriptor::create());)"),
                  Optimizer::ContainmentType::NO_CONTAINMENT},
                 {std::tuple<std::string, std::string>(R"(Query::from("windTurbines"))"
                                                       R"(.filter(Attribute("value") < 40))"
                                                       R"(.window(TumblingWindow::of(EventTime(Attribute("ts")), Seconds(20))))"
                                                       R"(.apply(Sum(Attribute("value1"))))"
                                                       R"(.sink(PrintSinkDescriptor::create());)",
                                                       R"(Query::from("windTurbines"))"
                                                       R"(.window(TumblingWindow::of(EventTime(Attribute("ts")), Seconds(10))))"
                                                       R"(.apply(Sum(Attribute("value1"))))"
                                                       R"(.sink(PrintSinkDescriptor::create());)"),
                  Optimizer::ContainmentType::NO_CONTAINMENT},
                 //No containment because different join attributes
                 {std::tuple<std::string, std::string>(
                      R"(Query::from("windTurbines"))"
                      R"(.joinWith(Query::from("solarPanels")))"
                      R"(.where(Attribute("id1")))"
                      R"(.equalsTo(Attribute("id")))"
                      R"(.window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))))"
                      R"(.sink(PrintSinkDescriptor::create());)",
                      R"(Query::from("windTurbines"))"
                      R"(.joinWith(Query::from("solarPanels")))"
                      R"(.where(Attribute("id1")))"
                      R"(.equalsTo(Attribute("id")))"
                      R"(.window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(20))))"
                      R"(.filter(Attribute("value") < 10))"
                      R"(.sink(PrintSinkDescriptor::create());)"),
                  Optimizer::ContainmentType::NO_CONTAINMENT},
                 {std::tuple<std::string, std::string>(
                      R"(Query::from("windTurbines"))"
                      R"(.filter(Attribute("value") < 5))"
                      R"(.joinWith(Query::from("solarPanels")))"
                      R"(.where(Attribute("id1")))"
                      R"(.equalsTo(Attribute("id")))"
                      R"(.window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))))"
                      R"(.sink(PrintSinkDescriptor::create());)",
                      R"(Query::from("windTurbines"))"
                      R"(.joinWith(Query::from("solarPanels")))"
                      R"(.where(Attribute("id1")))"
                      R"(.equalsTo(Attribute("id")))"
                      R"(.window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(20))))"
                      R"(.sink(PrintSinkDescriptor::create());)"),
                  Optimizer::ContainmentType::NO_CONTAINMENT},
                 {std::tuple<std::string, std::string>(
                      R"(Query::from("windTurbines"))"
                      R"(.window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))))"
                      R"(.apply(Sum(Attribute("value"))))"
                      R"(.sink(PrintSinkDescriptor::create());)",
                      R"(Query::from("windTurbines"))"
                      R"(.window(SlidingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000), Milliseconds(10))))"
                      R"(.byKey(Attribute("id")))"
                      R"(.apply(Sum(Attribute("value"))))"
                      R"(.sink(PrintSinkDescriptor::create());)"),
                  Optimizer::ContainmentType::NO_CONTAINMENT},
                 {std::tuple<std::string, std::string>(
                      R"(Query::from("windTurbines"))"
                      R"(.window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))))"
                      R"(.apply(Sum(Attribute("value"))))"
                      R"(.window(TumblingWindow::of(EventTime(Attribute("start")), Milliseconds(10000))))"
                      R"(.apply(Min(Attribute("value"))))"
                      R"(.sink(PrintSinkDescriptor::create());)",
                      R"(Query::from("windTurbines"))"
                      R"(.window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))))"
                      R"(.apply(Sum(Attribute("value"))))"
                      R"(.sink(PrintSinkDescriptor::create());)"),
                  Optimizer::ContainmentType::NO_CONTAINMENT},
                 {std::tuple<std::string, std::string>(
                      R"(Query::from("windTurbines"))"
                      R"(.window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))))"
                      R"(.apply(Sum(Attribute("value"))))"
                      R"(.window(TumblingWindow::of(EventTime(Attribute("start")), Milliseconds(10000))))"
                      R"(.apply(Min(Attribute("value"))))"
                      R"(.sink(PrintSinkDescriptor::create());)",
                      R"(Query::from("windTurbines"))"
                      R"(.window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(100))))"
                      R"(.apply(Sum(Attribute("value"))))"
                      R"(.window(TumblingWindow::of(EventTime(Attribute("start")), Milliseconds(1000))))"
                      R"(.apply(Max(Attribute("value"))))"
                      R"(.sink(PrintSinkDescriptor::create());)"),
                  Optimizer::ContainmentType::NO_CONTAINMENT},
                 {std::tuple<std::string, std::string>(
                      R"(Query::from("windTurbines"))"
                      R"(.unionWith(Query::from("solarPanels")))"
                      R"(.window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(10000))))"
                      R"(.apply(Avg(Attribute("value"))))"
                      R"(.sink(PrintSinkDescriptor::create());)",
                      R"(Query::from("windTurbines"))"
                      R"(.unionWith(Query::from("solarPanels")))"
                      R"(.window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(10000))))"
                      R"(.apply(Sum(Attribute("value"))))"
                      R"(.sink(PrintSinkDescriptor::create());)"),
                  Optimizer::ContainmentType::NO_CONTAINMENT},
                 {std::tuple<std::string, std::string>(
                      R"(Query::from("windTurbines"))"
                      R"(.unionWith(Query::from("solarPanels")))"
                      R"(.window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(10000))))"
                      R"(.apply(Avg(Attribute("value"))))"
                      R"(.sink(PrintSinkDescriptor::create());)",
                      R"(Query::from("windTurbines"))"
                      R"(.unionWith(Query::from("solarPanels")))"
                      R"(.window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(100))))"
                      R"(.apply(Sum(Attribute("value"))))"
                      R"(.sink(PrintSinkDescriptor::create());)"),
                  Optimizer::ContainmentType::NO_CONTAINMENT},
                 {std::tuple<std::string, std::string>(
                      R"(Query::from("windTurbines"))"
                      R"(.unionWith(Query::from("solarPanels")))"
                      R"(.window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(10000))))"
                      R"(.byKey(Attribute("id")))"
                      R"(.apply(Sum(Attribute("value"))))"
                      R"(.sink(PrintSinkDescriptor::create());)",
                      R"(Query::from("windTurbines"))"
                      R"(.unionWith(Query::from("solarPanels")))"
                      R"(.window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(10000))))"
                      R"(.apply(Sum(Attribute("value"))))"
                      R"(.sink(PrintSinkDescriptor::create());)"),
                  Optimizer::ContainmentType::NO_CONTAINMENT},
                 {std::tuple<std::string, std::string>(
                      R"(Query::from("windTurbines"))"
                      R"(.unionWith(Query::from("solarPanels")))"
                      R"(.window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(10000))))"
                      R"(.apply(Sum(Attribute("value"))))"
                      R"(.filter(Attribute("value") == 1))"
                      R"(.sink(PrintSinkDescriptor::create());)",
                      R"(Query::from("windTurbines"))"
                      R"(.unionWith(Query::from("solarPanels")))"
                      R"(.window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(100))))"
                      R"(.apply(Sum(Attribute("value"))))"
                      R"(.sink(PrintSinkDescriptor::create());)"),
                  Optimizer::ContainmentType::NO_CONTAINMENT},
                 {std::tuple<std::string, std::string>(
                      R"(Query::from("windTurbines"))"
                      R"(.unionWith(Query::from("solarPanels")))"
                      R"(.window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(10000))))"
                      R"(.apply(Sum(Attribute("value"))))"
                      R"(.filter(Attribute("value") != 1))"
                      R"(.sink(PrintSinkDescriptor::create());)",
                      R"(Query::from("windTurbines").filter(Attribute("value") == 1))"
                      R"(.unionWith(Query::from("solarPanels")))"
                      R"(.window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(10000))))"
                      R"(.apply(Sum(Attribute("value"))))"
                      R"(.sink(PrintSinkDescriptor::create());)"),
                  Optimizer::ContainmentType::NO_CONTAINMENT},
                 {std::tuple<std::string, std::string>(
                      R"(Query::from("windTurbines"))"
                      R"(.filter(Attribute("value") == 5))"
                      R"(.unionWith(Query::from("solarPanels")))"
                      R"(.joinWith(Query::from("households")))"
                      R"(.where(Attribute("id1")))"
                      R"(.equalsTo(Attribute("id")))"
                      R"(.window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))))"
                      R"(.sink(PrintSinkDescriptor::create());)",
                      R"(Query::from("windTurbines"))"
                      R"(.joinWith(Query::from("households")))"
                      R"(.where(Attribute("id1")))"
                      R"(.equalsTo(Attribute("id")))"
                      R"(.window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))))"
                      R"(.filter(Attribute("value") != 4))"
                      R"(.sink(PrintSinkDescriptor::create());)"),
                  Optimizer::ContainmentType::NO_CONTAINMENT},
                 {std::tuple<std::string, std::string>(
                      R"(Query::from("windTurbines"))"
                      R"(.filter(Attribute("value") != 4))"
                      R"(.unionWith(Query::from("solarPanels")))"
                      R"(.joinWith(Query::from("households")))"
                      R"(.where(Attribute("id1")))"
                      R"(.equalsTo(Attribute("id")))"
                      R"(.window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))))"
                      R"(.sink(PrintSinkDescriptor::create());)",
                      R"(Query::from("windTurbines"))"
                      R"(.unionWith(Query::from("solarPanels")))"
                      R"(.joinWith(Query::from("households")))"
                      R"(.where(Attribute("id1")))"
                      R"(.equalsTo(Attribute("id")))"
                      R"(.window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))))"
                      R"(.filter(Attribute("value") > 4))"
                      R"(.sink(PrintSinkDescriptor::create());)"),
                  Optimizer::ContainmentType::NO_CONTAINMENT},
                 {std::tuple<std::string, std::string>(
                      R"(Query::from("windTurbines"))"
                      R"(.unionWith(Query::from("solarPanels")))"
                      R"(.filter(Attribute("value") > 4))"
                      R"(.project(Attribute("value"), Attribute("id1"), Attribute("value1"), Attribute("ts")))"
                      R"(.joinWith(Query::from("households"))"
                      R"(.project(Attribute("value"), Attribute("id"), Attribute("value1"), Attribute("ts")))"
                      R"())"
                      R"(.where(Attribute("windTurbines$id1")))"
                      R"(.equalsTo(Attribute("households$id")))"
                      R"(.window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))))"
                      R"(.sink(PrintSinkDescriptor::create());)",
                      R"(Query::from("windTurbines"))"
                      R"(.unionWith(Query::from("solarPanels")))"
                      R"(.joinWith(Query::from("households")))"
                      R"(.where(Attribute("id1")))"
                      R"(.equalsTo(Attribute("id")))"
                      R"(.window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))))"
                      R"(.filter(Attribute("value") > 4))"
                      R"(.project(Attribute("windTurbines$value"), Attribute("windTurbines$id1"), Attribute("windTurbines$value1"), )"
                      R"(Attribute("households$value"), Attribute("households$id"), Attribute("households$value1"), )"
                      R"(Attribute("windTurbines$ts"), Attribute("households$ts")))"
                      R"(.sink(PrintSinkDescriptor::create());)"),
                  Optimizer::ContainmentType::NO_CONTAINMENT},
                 {std::tuple<std::string, std::string>(
                      R"(Query::from("windTurbines"))"
                      R"(.unionWith(Query::from("solarPanels")))"
                      R"(.filter(Attribute("value") > 4))"
                      R"(.joinWith(Query::from("households"))"
                      R"())"
                      R"(.where(Attribute("windTurbines$id1")))"
                      R"(.equalsTo(Attribute("households$id")))"
                      R"(.window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(100))))"
                      R"(.sink(PrintSinkDescriptor::create());)",
                      R"(Query::from("windTurbines"))"
                      R"(.unionWith(Query::from("solarPanels")))"
                      R"(.joinWith(Query::from("households")))"
                      R"(.where(Attribute("id1")))"
                      R"(.equalsTo(Attribute("id")))"
                      R"(.window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))))"
                      R"(.filter(Attribute("value") > 4))"
                      R"(.sink(PrintSinkDescriptor::create());)"),
                  Optimizer::ContainmentType::NO_CONTAINMENT},
                 //Limit of our algorithm, cannot detect equivalence among these windows
                 {std::tuple<std::string, std::string>(
                      R"(Query::from("windTurbines"))"
                      R"(.unionWith(Query::from("solarPanels")))"
                      R"(.filter(Attribute("value") > 4))"
                      R"(.joinWith(Query::from("households"))"
                      R"())"
                      R"(.where(Attribute("windTurbines$id")))"
                      R"(.equalsTo(Attribute("households$id")))"
                      R"(.window(TumblingWindow::of(EventTime(Attribute("ts")), Hours(1))))"
                      R"(.window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))))"
                      R"(.byKey(Attribute("id")))"
                      R"(.apply(Sum(Attribute("value"))))"
                      R"(.sink(PrintSinkDescriptor::create());)",
                      R"(Query::from("windTurbines"))"
                      R"(.unionWith(Query::from("solarPanels")))"
                      R"(.window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))))"
                      R"(.byKey(Attribute("id")))"
                      R"(.apply(Sum(Attribute("value"))))"
                      R"(.joinWith(Query::from("households"))"
                      R"(.window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))))"
                      R"(.byKey(Attribute("id")))"
                      R"(.apply(Sum(Attribute("value")))))"
                      R"(.where(Attribute("id")))"
                      R"(.equalsTo(Attribute("id")))"
                      R"(.window(TumblingWindow::of(EventTime(Attribute("start")), Hours(1))))"
                      R"(.filter(Attribute("windTurbines$value") > 4))"
                      R"(.sink(PrintSinkDescriptor::create());)"),
                  Optimizer::ContainmentType::NO_CONTAINMENT},
                 {std::tuple<std::string, std::string>(
                      R"(Query::from("windTurbines"))"
                      R"(.unionWith(Query::from("solarPanels")))"
                      R"(.filter(Attribute("value") > 4))"
                      R"(.joinWith(Query::from("households"))"
                      R"())"
                      R"(.where(Attribute("windTurbines$id1")))"
                      R"(.equalsTo(Attribute("households$id")))"
                      R"(.window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))))"
                      R"(.sink(PrintSinkDescriptor::create());)",
                      R"(Query::from("windTurbines"))"
                      R"(.unionWith(Query::from("solarPanels")))"
                      R"(.joinWith(Query::from("households")))"
                      R"(.where(Attribute("id1")))"
                      R"(.equalsTo(Attribute("id")))"
                      R"(.window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))))"
                      R"(.project(Attribute("windTurbines$value")))"
                      R"(.sink(PrintSinkDescriptor::create());)"),
                  Optimizer::ContainmentType::NO_CONTAINMENT},
                 {std::tuple<std::string, std::string>(
                      R"(Query::from("windTurbines"))"
                      R"(.unionWith(Query::from("solarPanels")))"
                      R"(.joinWith(Query::from("households"))"
                      R"())"
                      R"(.where(Attribute("windTurbines$id1")))"
                      R"(.equalsTo(Attribute("households$id")))"
                      R"(.window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(10000))))"
                      R"(.sink(PrintSinkDescriptor::create());)",
                      R"(Query::from("windTurbines"))"
                      R"(.unionWith(Query::from("solarPanels")))"
                      R"(.joinWith(Query::from("households")))"
                      R"(.where(Attribute("id1")))"
                      R"(.equalsTo(Attribute("id")))"
                      R"(.window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))))"
                      R"(.project(Attribute("windTurbines$value")))"
                      R"(.sink(PrintSinkDescriptor::create());)"),
                  Optimizer::ContainmentType::NO_CONTAINMENT},
                 {std::tuple<std::string, std::string>(
                      R"(Query::from("windTurbines"))"
                      R"(.joinWith(Query::from("solarPanels")))"
                      R"(.where(Attribute("id1")))"
                      R"(.equalsTo(Attribute("id")))"
                      R"(.window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(10))))"
                      R"(.sink(PrintSinkDescriptor::create());)",
                      R"(Query::from("windTurbines"))"
                      R"(.joinWith(Query::from("solarPanels")))"
                      R"(.where(Attribute("id1")))"
                      R"(.equalsTo(Attribute("id")))"
                      R"(.window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))))"
                      R"(.sink(PrintSinkDescriptor::create());)"),
                  Optimizer::ContainmentType::NO_CONTAINMENT},
                 {std::tuple<std::string, std::string>(
                      R"(Query::from("windTurbines"))"
                      R"(.joinWith(Query::from("solarPanels")))"
                      R"(.where(Attribute("id1")))"
                      R"(.equalsTo(Attribute("id")))"
                      R"(.window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))))"
                      R"(.sink(PrintSinkDescriptor::create());)",
                      R"(Query::from("windTurbines"))"
                      R"(.joinWith(Query::from("solarPanels")))"
                      R"(.where(Attribute("id1")))"
                      R"(.equalsTo(Attribute("id")))"
                      R"(.window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(20))))"
                      R"(.sink(PrintSinkDescriptor::create());)"),
                  Optimizer::ContainmentType::NO_CONTAINMENT},
                 {std::tuple<std::string, std::string>(
                      R"(Query::from("windTurbines"))"
                      R"(.unionWith(Query::from("solarPanels")))"
                      R"(.filter(Attribute("value") > 4))"
                      R"(.joinWith(Query::from("households"))"
                      R"())"
                      R"(.where(Attribute("windTurbines$id1")))"
                      R"(.equalsTo(Attribute("households$id")))"
                      R"(.window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(100))))"
                      R"(.sink(PrintSinkDescriptor::create());)",
                      R"(Query::from("windTurbines"))"
                      R"(.unionWith(Query::from("solarPanels")))"
                      R"(.joinWith(Query::from("households")))"
                      R"(.where(Attribute("id1")))"
                      R"(.equalsTo(Attribute("id")))"
                      R"(.window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))))"
                      R"(.filter(Attribute("windTurbines$value") > 4))"
                      R"(.sink(PrintSinkDescriptor::create());)"),
                  Optimizer::ContainmentType::NO_CONTAINMENT},
                 //projection conditions differ too much for containment to be picked up in the complete query
                 //bottom up approach would detect it, though
                 {std::tuple<std::string, std::string>(
                      R"(Query::from("windTurbines"))"
                      R"(.unionWith(Query::from("solarPanels")))"
                      R"(.joinWith(Query::from("households"))"
                      R"())"
                      R"(.where(Attribute("windTurbines$id1")))"
                      R"(.equalsTo(Attribute("households$id")))"
                      R"(.window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))))"
                      R"(.sink(PrintSinkDescriptor::create());)",
                      R"(Query::from("windTurbines"))"
                      R"(.unionWith(Query::from("solarPanels")))"
                      R"(.joinWith(Query::from("households")))"
                      R"(.where(Attribute("id1")))"
                      R"(.equalsTo(Attribute("id")))"
                      R"(.window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))))"
                      R"(.window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))))"
                      R"(.byKey(Attribute("id")))"
                      R"(.apply(Sum(Attribute("value"))))"
                      R"(.sink(PrintSinkDescriptor::create());)"),
                  Optimizer::ContainmentType::NO_CONTAINMENT},
                 //another limit of our algorithm. If one query does not have a window, we cannot detect containment
                 {std::tuple<std::string, std::string>(
                      R"(Query::from("windTurbines"))"
                      R"(.unionWith(Query::from("solarPanels")))"
                      R"(.sink(PrintSinkDescriptor::create());)",
                      R"(Query::from("windTurbines"))"
                      R"(.unionWith(Query::from("solarPanels")))"
                      R"(.window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))))"
                      R"(.byKey(Attribute("id")))"
                      R"(.apply(Sum(Attribute("value"))))"
                      R"(.sink(PrintSinkDescriptor::create());)"),
                  Optimizer::ContainmentType::NO_CONTAINMENT},
                 {std::tuple<std::string, std::string>(
                      R"(Query::from("windTurbines"))"
                      R"(.unionWith(Query::from("solarPanels")))"
                      R"(.window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(10))))"
                      R"(.byKey(Attribute("id")))"
                      R"(.apply(Sum(Attribute("value"))))"
                      R"(.sink(PrintSinkDescriptor::create());)",
                      R"(Query::from("windTurbines"))"
                      R"(.unionWith(Query::from("solarPanels")))"
                      R"(.window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(10))))"
                      R"(.byKey(Attribute("id")))"
                      R"(.apply(Sum(Attribute("value"))))"
                      R"(.window(TumblingWindow::of(EventTime(Attribute("start")), Milliseconds(1000))))"
                      R"(.byKey(Attribute("id")))"
                      R"(.apply(Min(Attribute("value"))))"
                      R"(.sink(PrintSinkDescriptor::create());)"),
                  Optimizer::ContainmentType::NO_CONTAINMENT}}};
        return noContainment;
    }

    static auto createFilterContainmentCases() {
        std::tuple<std::string, std::vector<std::tuple<std::tuple<std::string, std::string>, NES::Optimizer::ContainmentType>>>
            filterContainment = {
                "filterContainment",
                {//Sig2 contains Sig1
                 {std::tuple<std::string, std::string>(
                      R"(Query::from("windTurbines").map(Attribute("value") = 40).filter(Attribute("id") < 45).sink(PrintSinkDescriptor::create());)",
                      R"(Query::from("windTurbines").map(Attribute("value") = 40).filter(Attribute("id") < 60).sink(PrintSinkDescriptor::create());)"),
                  Optimizer::ContainmentType::LEFT_SIG_CONTAINED},
                 //Sig1 contains Sig2
                 {std::tuple<std::string, std::string>(
                      R"(Query::from("windTurbines").map(Attribute("value") = 40).filter(Attribute("id") < 60).sink(PrintSinkDescriptor::create());)",
                      R"(Query::from("windTurbines").map(Attribute("value") = 40).filter(Attribute("id") < 45).sink(PrintSinkDescriptor::create());)"),
                  Optimizer::ContainmentType::RIGHT_SIG_CONTAINED},
                 {std::tuple<std::string, std::string>(
                      R"(Query::from("windTurbines"))"
                      R"(.joinWith(Query::from("solarPanels")))"
                      R"(.where(Attribute("id1")))"
                      R"(.equalsTo(Attribute("id")))"
                      R"(.window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))))"
                      R"(.sink(PrintSinkDescriptor::create());)",
                      R"(Query::from("windTurbines"))"
                      R"(.joinWith(Query::from("solarPanels")))"
                      R"(.where(Attribute("id1")))"
                      R"(.equalsTo(Attribute("id")))"
                      R"(.window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))))"
                      R"(.filter(Attribute("value") < 5))"
                      R"(.sink(PrintSinkDescriptor::create());)"),
                  Optimizer::ContainmentType::RIGHT_SIG_CONTAINED},
                 {std::tuple<std::string, std::string>(
                      R"(Query::from("windTurbines"))"
                      R"(.filter(Attribute("value") < 5))"
                      R"(.joinWith(Query::from("solarPanels")))"
                      R"(.where(Attribute("id1")))"
                      R"(.equalsTo(Attribute("id")))"
                      R"(.window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))))"
                      R"(.sink(PrintSinkDescriptor::create());)",
                      R"(Query::from("windTurbines"))"
                      R"(.joinWith(Query::from("solarPanels")))"
                      R"(.where(Attribute("id1")))"
                      R"(.equalsTo(Attribute("id")))"
                      R"(.window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))))"
                      R"(.sink(PrintSinkDescriptor::create());)"),
                  Optimizer::ContainmentType::LEFT_SIG_CONTAINED},
                 {std::tuple<std::string, std::string>(
                      R"(Query::from("windTurbines"))"
                      R"(.filter(Attribute("value") == 5))"
                      R"(.joinWith(Query::from("households")))"
                      R"(.where(Attribute("id1")))"
                      R"(.equalsTo(Attribute("id")))"
                      R"(.window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))))"
                      R"(.sink(PrintSinkDescriptor::create());)",
                      R"(Query::from("windTurbines"))"
                      R"(.joinWith(Query::from("households")))"
                      R"(.where(Attribute("id1")))"
                      R"(.equalsTo(Attribute("id")))"
                      R"(.window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))))"
                      R"(.filter(Attribute("value") != 4))"
                      R"(.sink(PrintSinkDescriptor::create());)"),
                  Optimizer::ContainmentType::LEFT_SIG_CONTAINED},
                 {std::tuple<std::string, std::string>(R"(Query::from("windTurbines"))"
                                                       R"(.filter(Attribute("value") == 5))"
                                                       R"(.unionWith(Query::from("solarPanels")))"
                                                       R"(.sink(PrintSinkDescriptor::create());)",
                                                       R"(Query::from("windTurbines"))"
                                                       R"(.unionWith(Query::from("solarPanels")))"
                                                       R"(.filter(Attribute("value") >= 4))"
                                                       R"(.sink(PrintSinkDescriptor::create());)"),
                  Optimizer::ContainmentType::LEFT_SIG_CONTAINED},
                 {std::tuple<std::string, std::string>(
                      R"(Query::from("windTurbines"))"
                      R"(.unionWith(Query::from("solarPanels")))"
                      R"(.filter(Attribute("value") != 4))"
                      R"(.joinWith(Query::from("households")))"
                      R"(.where(Attribute("id1")))"
                      R"(.equalsTo(Attribute("id")))"
                      R"(.window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))))"
                      R"(.sink(PrintSinkDescriptor::create());)",
                      R"(Query::from("windTurbines"))"
                      R"(.unionWith(Query::from("solarPanels")))"
                      R"(.joinWith(Query::from("households")))"
                      R"(.where(Attribute("id1")))"
                      R"(.equalsTo(Attribute("id")))"
                      R"(.window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))))"
                      R"(.filter(Attribute("value") > 4))"
                      R"(.sink(PrintSinkDescriptor::create());)"),
                  Optimizer::ContainmentType::RIGHT_SIG_CONTAINED}}};
        return filterContainment;
    }

    static auto createProjectionContainmentCases() {
        std::tuple<std::string, std::vector<std::tuple<std::tuple<std::string, std::string>, NES::Optimizer::ContainmentType>>>
            projectionContainment = {
                "projectionContainment",
                {//Sig2 contains Sig1
                 {std::tuple<std::string, std::string>(
                      R"(Query::from("windTurbines").map(Attribute("value") = 40).project(Attribute("value")).sink(PrintSinkDescriptor::create());)",
                      R"(Query::from("windTurbines").map(Attribute("value") = 40).sink(PrintSinkDescriptor::create());)"),
                  Optimizer::ContainmentType::LEFT_SIG_CONTAINED},
                 //Sig1 contains Sig2
                 {std::tuple<std::string, std::string>(
                      R"(Query::from("windTurbines").map(Attribute("value") = 40).sink(PrintSinkDescriptor::create());)",
                      R"(Query::from("windTurbines").map(Attribute("value") = 40).project(Attribute("value")).sink(PrintSinkDescriptor::create());)"),
                  Optimizer::ContainmentType::RIGHT_SIG_CONTAINED},
                 {std::tuple<std::string, std::string>(
                      R"(Query::from("windTurbines"))"
                      R"(.unionWith(Query::from("solarPanels")))"
                      R"(.filter(Attribute("value") > 4))"
                      R"(.project(Attribute("value"), Attribute("id1"), Attribute("value1"), Attribute("ts")))"
                      R"(.joinWith(Query::from("households"))"
                      R"(.project(Attribute("value"), Attribute("id"), Attribute("value1"), Attribute("ts")))"
                      R"())"
                      R"(.where(Attribute("windTurbines$id1")))"
                      R"(.equalsTo(Attribute("households$id")))"
                      R"(.window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))))"
                      R"(.sink(PrintSinkDescriptor::create());)",
                      R"(Query::from("windTurbines"))"
                      R"(.unionWith(Query::from("solarPanels")))"
                      R"(.joinWith(Query::from("households")))"
                      R"(.where(Attribute("id1")))"
                      R"(.equalsTo(Attribute("id")))"
                      R"(.window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))))"
                      R"(.filter(Attribute("value") > 4))"
                      R"(.project(Attribute("windTurbines$value"), Attribute("windTurbines$id1"), )"
                      R"(Attribute("households$value"), Attribute("households$id"), )"
                      R"(Attribute("windTurbines$ts"), Attribute("households$ts")))"
                      R"(.sink(PrintSinkDescriptor::create());)"),
                  Optimizer::ContainmentType::RIGHT_SIG_CONTAINED},
                 {std::tuple<std::string, std::string>(
                      R"(Query::from("windTurbines"))"
                      R"(.unionWith(Query::from("solarPanels")))"
                      R"(.joinWith(Query::from("households"))"
                      R"())"
                      R"(.where(Attribute("windTurbines$id1")))"
                      R"(.equalsTo(Attribute("households$id")))"
                      R"(.window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))))"
                      R"(.sink(PrintSinkDescriptor::create());)",
                      R"(Query::from("windTurbines"))"
                      R"(.unionWith(Query::from("solarPanels")))"
                      R"(.joinWith(Query::from("households")))"
                      R"(.where(Attribute("id1")))"
                      R"(.equalsTo(Attribute("id")))"
                      R"(.window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))))"
                      R"(.project(Attribute("windTurbines$value")))"
                      R"(.sink(PrintSinkDescriptor::create());)"),
                  Optimizer::ContainmentType::RIGHT_SIG_CONTAINED}}};
        return projectionContainment;
    }
    static auto createWindowContainmentCases() {
        std::tuple<std::string, std::vector<std::tuple<std::tuple<std::string, std::string>, NES::Optimizer::ContainmentType>>>
            windowContainment = {
                "windowContainment",
                {{std::tuple<std::string, std::string>(R"(Query::from("windTurbines"))"
                                                       R"(.window(TumblingWindow::of(EventTime(Attribute("ts")), Seconds(20))))"
                                                       R"(.apply(Sum(Attribute("value1"))))"
                                                       R"(.sink(PrintSinkDescriptor::create());)",
                                                       R"(Query::from("windTurbines"))"
                                                       R"(.window(TumblingWindow::of(EventTime(Attribute("ts")), Seconds(10))))"
                                                       R"(.apply(Sum(Attribute("value1"))))"
                                                       R"(.sink(PrintSinkDescriptor::create());)"),
                  Optimizer::ContainmentType::LEFT_SIG_CONTAINED},
                 {std::tuple<std::string, std::string>(R"(Query::from("windTurbines"))"
                                                       R"(.window(TumblingWindow::of(EventTime(Attribute("ts")), Seconds(10))))"
                                                       R"(.apply(Sum(Attribute("value1"))))"
                                                       R"(.sink(PrintSinkDescriptor::create());)",
                                                       R"(Query::from("windTurbines"))"
                                                       R"(.window(TumblingWindow::of(EventTime(Attribute("ts")), Seconds(20))))"
                                                       R"(.apply(Sum(Attribute("value1"))))"
                                                       R"(.sink(PrintSinkDescriptor::create());)"),
                  Optimizer::ContainmentType::RIGHT_SIG_CONTAINED},
                 {std::tuple<std::string, std::string>(
                      R"(Query::from("windTurbines"))"
                      R"(.window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))))"
                      R"(.apply(Sum(Attribute("value")), Min(Attribute("value"))->as(Attribute("newValue"))))"
                      R"(.sink(PrintSinkDescriptor::create());)",
                      R"(Query::from("windTurbines"))"
                      R"(.window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))))"
                      R"(.apply(Sum(Attribute("value"))))"
                      R"(.sink(PrintSinkDescriptor::create());)"),
                  Optimizer::ContainmentType::LEFT_SIG_CONTAINED},
                 {std::tuple<std::string, std::string>(
                      R"(Query::from("windTurbines"))"
                      R"(.window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))))"
                      R"(.apply(Sum(Attribute("value")), Min(Attribute("value1"))))"
                      R"(.sink(PrintSinkDescriptor::create());)",
                      R"(Query::from("windTurbines"))"
                      R"(.window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))))"
                      R"(.apply(Sum(Attribute("value"))))"
                      R"(.sink(PrintSinkDescriptor::create());)"),
                  Optimizer::ContainmentType::LEFT_SIG_CONTAINED},
                 {std::tuple<std::string, std::string>(
                      R"(Query::from("windTurbines"))"
                      R"(.window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))))"
                      R"(.apply(Sum(Attribute("value"))))"
                      R"(.sink(PrintSinkDescriptor::create());)",
                      R"(Query::from("windTurbines"))"
                      R"(.window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))))"
                      R"(.apply(Sum(Attribute("value")), Min(Attribute("value1"))))"
                      R"(.sink(PrintSinkDescriptor::create());)"),
                  Optimizer::ContainmentType::RIGHT_SIG_CONTAINED},
                 {std::tuple<std::string, std::string>(
                      R"(Query::from("windTurbines"))"
                      R"(.window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))))"
                      R"(.apply(Sum(Attribute("value"))))"
                      R"(.window(TumblingWindow::of(EventTime(Attribute("start")), Milliseconds(10000))))"
                      R"(.apply(Min(Attribute("value"))))"
                      R"(.sink(PrintSinkDescriptor::create());)",
                      R"(Query::from("windTurbines"))"
                      R"(.window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))))"
                      R"(.apply(Sum(Attribute("value"))))"
                      R"(.window(TumblingWindow::of(EventTime(Attribute("start")), Milliseconds(100))))"
                      R"(.apply(Min(Attribute("value"))))"
                      R"(.sink(PrintSinkDescriptor::create());)"),
                  Optimizer::ContainmentType::LEFT_SIG_CONTAINED},
                 {std::tuple<std::string, std::string>(
                      R"(Query::from("windTurbines"))"
                      R"(.window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))))"
                      R"(.apply(Sum(Attribute("value")), Min(Attribute("value1")), Max(Attribute("value1"))))"
                      R"(.sink(PrintSinkDescriptor::create());)",
                      R"(Query::from("windTurbines"))"
                      R"(.window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))))"
                      R"(.apply(Sum(Attribute("value"))))"
                      R"(.sink(PrintSinkDescriptor::create());)"),
                  Optimizer::ContainmentType::LEFT_SIG_CONTAINED},
                 {std::tuple<std::string, std::string>(
                      R"(Query::from("windTurbines"))"
                      R"(.window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))))"
                      R"(.apply(Sum(Attribute("value"))))"
                      R"(.sink(PrintSinkDescriptor::create());)",
                      R"(Query::from("windTurbines"))"
                      R"(.window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))))"
                      R"(.apply(Sum(Attribute("value")), Min(Attribute("value1")), Max(Attribute("value1"))))"
                      R"(.sink(PrintSinkDescriptor::create());)"),
                  Optimizer::ContainmentType::RIGHT_SIG_CONTAINED},
                 {std::tuple<std::string, std::string>(
                      R"(Query::from("windTurbines"))"
                      R"(.unionWith(Query::from("solarPanels")))"
                      R"(.window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))))"
                      R"(.byKey(Attribute("id")))"
                      R"(.apply(Sum(Attribute("value"))))"
                      R"(.sink(PrintSinkDescriptor::create());)",
                      R"(Query::from("windTurbines"))"
                      R"(.unionWith(Query::from("solarPanels")))"
                      R"(.window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(100))))"
                      R"(.byKey(Attribute("id")))"
                      R"(.apply(Sum(Attribute("value")), Min(Attribute("value1")), Max(Attribute("value2"))))"
                      R"(.sink(PrintSinkDescriptor::create());)"),
                  Optimizer::ContainmentType::RIGHT_SIG_CONTAINED}
                }};
        return windowContainment;
    }
};

/**
 * @brief tests if the correct containment relationship is returned by the signature containment util
 */
TEST_P(QueryContainmentIdentificationTest, testContainmentIdentification) {

    auto containmentCases = std::get<1>(GetParam());

    uint16_t counter = 0;
    for (auto entry : containmentCases) {
        auto queries = get<0>(entry);
        QueryPlanPtr queryPlanSQPQuery = syntacticQueryValidation->validate(get<0>(queries));
        QueryPlanPtr queryPlanNewQuery = syntacticQueryValidation->validate(get<1>(queries));
        //typ inference face
        auto typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);
        typeInferencePhase->execute(queryPlanSQPQuery);
        typeInferencePhase->execute(queryPlanNewQuery);
        //obtain context and create signatures
        z3::ContextPtr context = std::make_shared<z3::context>();
        auto signatureInferencePhase =
            Optimizer::SignatureInferencePhase::create(context, Optimizer::QueryMergerRule::Z3SignatureBasedQueryContainmentRule);
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
            signatureContainmentUtil->checkContainment(sqpSink->getZ3Signature(), newSink->getZ3Signature());
        NES_TRACE("Z3SignatureBasedContainmentBasedCompleteQueryMergerRule: containment: " << magic_enum::enum_name(containment));
        NES_TRACE("Query pairing number: " << counter);
        ASSERT_EQ(containment, get<1>(entry));
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
                            std::string name = std::get<0>(info.param);
                            return name;
                        });