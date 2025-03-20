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

#include <API/AttributeField.hpp>
#include <API/QueryAPI.hpp>
#include <BaseIntegrationTest.hpp>
#include <Catalogs/Source/LogicalSource.hpp>
#include <Catalogs/Source/PhysicalSource.hpp>
#include <Catalogs/Source/SourceCatalog.hpp>
#include <Catalogs/UDF/UDFCatalog.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Configurations/Coordinator/CoordinatorConfiguration.hpp>
#include <Expressions/FieldAssignmentExpressionNode.hpp>
#include <Expressions/Functions/LogicalFunctionRegistry.hpp>
#include <Operators/LogicalOperators/LogicalFilterOperator.hpp>
#include <Operators/LogicalOperators/LogicalOperatorFactory.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/LogicalOperators/Sources/LogicalSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperator.hpp>
#include <Optimizer/Phases/QueryRewritePhase.hpp>
#include <Optimizer/Phases/TypeInferencePhase.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Util/JavaUDFDescriptorBuilder.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>
#include <memory>
#include <string>

using namespace NES::API;
using namespace NES::Windowing;

namespace NES {

class QueryRewritePhaseTest : public Testing::BaseUnitTest {
  public:
    Catalogs::UDF::UDFCatalogPtr udfCatalog = Catalogs::UDF::UDFCatalog::create();
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("QueryRewritePhaseTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup QueryRewritePhaseTest test class.");
    }
};

/**
 * @brief In this test we try to apply rewrite phase to filter dominated query.
 */
TEST_F(QueryRewritePhaseTest, applyRewritePhaseToFilterDominatedQuery) {
    auto inputSchema = NES::Schema::create()
                           ->addField("m", BasicType::UINT64)
                           ->addField("n", BasicType::UINT64)
                           ->addField("o", BasicType::UINT64)
                           ->addField("p", BasicType::UINT64)
                           ->addField("q", BasicType::UINT64)
                           ->addField("r", BasicType::UINT64)
                           ->addField("time1", BasicType::UINT64)
                           ->addField("time2", BasicType::UINT64);

    Catalogs::Source::SourceCatalogPtr sourceCatalog = std::make_shared<Catalogs::Source::SourceCatalog>();
    auto sourceName = "example23";
    sourceCatalog->addLogicalSource(sourceName, inputSchema);

    auto query = Query::from(sourceName)
                     .map(Attribute("p") = 4)
                     .filter(Attribute("m") > Attribute("m") - 4)
                     .filter(Attribute("r") > Attribute("q"))
                     .map(Attribute("r") = Attribute("r") - 8)
                     .map(Attribute("o") = Attribute("o") - 9)
                     .filter(Attribute("m") <= 84)
                     .filter(Attribute("n") <= 57)
                     .map(Attribute("m") = 10 * Attribute("m"))
                     .filter(Attribute("m") < 33)
                     .sink(NullOutputSinkDescriptor::create());

    auto plan = query.getQueryPlan();
    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);
    auto resultPlan = typeInferencePhase->execute(plan);

    auto cordConfig = Configurations::CoordinatorConfiguration::createDefault();
    auto rewritePhase = Optimizer::QueryRewritePhase::create(cordConfig);
    resultPlan = rewritePhase->execute(resultPlan);

    auto filters = resultPlan->getOperatorByType<LogicalFilterOperator>();
    EXPECT_EQ(filters.size(), 1);

    auto actualFilter = filters[0];
    auto expectedPredicate = (10 * Attribute("m") < 33 && Attribute("n") <= 57 && Attribute("m") <= 84
                              && Attribute("r") > Attribute("q") && Attribute("m") > Attribute("m") - 4);
    inputSchema->updateSourceName(sourceName);
    expectedPredicate->inferStamp(inputSchema);
    EXPECT_EQ(actualFilter->getPredicate()->toString(), expectedPredicate->toString());
}

TEST_F(QueryRewritePhaseTest, applyRewritePhaseToMultiJoinQuery) {
    auto inputSchema = NES::Schema::create()
                           ->addField("m", BasicType::UINT64)
                           ->addField("n", BasicType::UINT64)
                           ->addField("o", BasicType::UINT64)
                           ->addField("p", BasicType::UINT64)
                           ->addField("q", BasicType::UINT64)
                           ->addField("r", BasicType::UINT64)
                           ->addField("time1", BasicType::UINT64)
                           ->addField("time2", BasicType::UINT64);

    Catalogs::Source::SourceCatalogPtr sourceCatalog = std::make_shared<Catalogs::Source::SourceCatalog>();
    auto sourceName1 = "example1";
    auto sourceName2 = "example2";
    auto sourceName3 = "example3";
    auto sourceName4 = "example4";
    auto sourceName5 = "example5";
    auto sourceName6 = "example6";
    sourceCatalog->addLogicalSource(sourceName1, inputSchema);
    sourceCatalog->addLogicalSource(sourceName2, inputSchema);
    sourceCatalog->addLogicalSource(sourceName3, inputSchema);
    sourceCatalog->addLogicalSource(sourceName4, inputSchema);
    sourceCatalog->addLogicalSource(sourceName5, inputSchema);
    sourceCatalog->addLogicalSource(sourceName6, inputSchema);

    auto query = Query::sink2(NullOutputSinkDescriptor::create(),
                              {Query::from(sourceName1).joinWith(Query::from(sourceName2))
                              .where(Attribute("m") == Attribute("m"))
                              .window(TumblingWindow::of(EventTime(Attribute("time1")), Milliseconds(500))),//join 1
                              Query::from(sourceName3).joinWith(Query::from(sourceName4))
                              .where(Attribute("m") == Attribute("m"))
                              .window(TumblingWindow::of(EventTime(Attribute("time1")), Milliseconds(500))),//join 2
                              Query::from(sourceName5).joinWith(Query::from(sourceName6))
                              .where(Attribute("m") == Attribute("m"))
                              .window(TumblingWindow::of(EventTime(Attribute("time1")), Milliseconds(500)))});//join 3

    auto plan = query.getQueryPlan();
    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);
    auto resultPlan = typeInferencePhase->execute(plan);

    auto cordConfig = Configurations::CoordinatorConfiguration::createDefault();
    auto rewritePhase = Optimizer::QueryRewritePhase::create(cordConfig);
    resultPlan = rewritePhase->execute(resultPlan);
    EXPECT_TRUE(resultPlan);
}


/**
 * @brief In this test we try to apply rewrite phase to same query multiple time.
 */
TEST_F(QueryRewritePhaseTest, applyRewritePhaseToSameQueryMultipleTime) {

    auto sourceCatalog = std::make_shared<Catalogs::Source::SourceCatalog>();
    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);
    auto cordConfig = Configurations::CoordinatorConfiguration::createDefault();
    auto rewritePhase = Optimizer::QueryRewritePhase::create(cordConfig);

    auto inputSchema = NES::Schema::create()
                           ->addField("m", BasicType::UINT64)
                           ->addField("n", BasicType::UINT64)
                           ->addField("o", BasicType::UINT64)
                           ->addField("p", BasicType::UINT64)
                           ->addField("q", BasicType::UINT64)
                           ->addField("r", BasicType::UINT64)
                           ->addField("time1", BasicType::UINT64)
                           ->addField("time2", BasicType::UINT64);
    auto sourceName = "example23";
    sourceCatalog->addLogicalSource(sourceName, inputSchema);

    auto query = Query::from(sourceName)
                     .map(Attribute("p") = 4)
                     .filter(Attribute("m") > Attribute("m") - 4)
                     .filter(Attribute("r") > Attribute("q"))
                     .map(Attribute("r") = Attribute("r") - 8)
                     .map(Attribute("o") = Attribute("o") - 9)
                     .filter(Attribute("m") <= 84)
                     .filter(Attribute("n") <= 57)
                     .map(Attribute("m") = 10 * Attribute("m"))
                     .filter(Attribute("m") < 33)
                     .sink(NullOutputSinkDescriptor::create());

    auto plan = query.getQueryPlan();

    // First time
    auto resultPlan = typeInferencePhase->execute(plan);
    resultPlan = rewritePhase->execute(resultPlan);

    // Assertions
    auto filters = resultPlan->getOperatorByType<LogicalFilterOperator>();
    EXPECT_EQ(filters.size(), 1);
    auto actualFilter = filters[0];
    auto expectedPredicate = (10 * Attribute("m") < 33 && Attribute("n") <= 57 && Attribute("m") <= 84
                              && Attribute("r") > Attribute("q") && Attribute("m") > Attribute("m") - 4);
    inputSchema->updateSourceName(sourceName);
    expectedPredicate->inferStamp(inputSchema);
    EXPECT_EQ(actualFilter->getPredicate()->toString(), expectedPredicate->toString());

    // Second time
    resultPlan = typeInferencePhase->execute(plan);
    resultPlan = rewritePhase->execute(resultPlan);
    filters = resultPlan->getOperatorByType<LogicalFilterOperator>();

    // Assertions
    EXPECT_EQ(filters.size(), 1);
    actualFilter = filters[0];
    EXPECT_EQ(actualFilter->getPredicate()->toString(), expectedPredicate->toString());
}

}// namespace NES
