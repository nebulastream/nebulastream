/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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
#include <Catalogs/StreamCatalog.hpp>
#include <Nodes/Expressions/ArithmeticalExpressions/AddExpressionNode.hpp>
#include <Nodes/Expressions/FieldAssignmentExpressionNode.hpp>
#include <Operators/LogicalOperators/FilterLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/LogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/MapLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/MergeLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/ProjectionLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/RenameStreamOperatorNode.hpp>
#include <Operators/LogicalOperators/Sinks/FileSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/LogicalStreamSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Phases/TypeInferencePhase.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Topology/TopologyNode.hpp>
#include <Util/Logger.hpp>
#include <Windowing/TimeCharacteristic.hpp>
#include <Windowing/WindowTypes/TumblingWindow.hpp>
#include <gtest/gtest.h>
#include <iostream>
#include <memory>

using namespace NES::API;
using namespace NES::Windowing;

namespace NES {

class TypeInferencePhaseTest : public testing::Test {
  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::setupLogging("TypeInferencePhaseTest.log", NES::LOG_DEBUG);
        NES_INFO("Setup TypeInferencePhaseTest test class.");
    }

    /* Will be called before a  test is executed. */
    void SetUp() { NES_INFO("Setup TypeInferencePhaseTest test case."); }

    /* Will be called before a test is executed. */
    void TearDown() { NES_INFO("Tear down TypeInferencePhaseTest test case."); }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_INFO("Tear down TypeInferencePhaseTest test class."); }
};

/**
 * @brief In this test we infer the output and input schemas of each operator in a query.
 */
TEST_F(TypeInferencePhaseTest, inferQueryPlan) {
    auto inputSchema = Schema::create();
    inputSchema->addField("f1", BasicType::INT32);
    inputSchema->addField("f2", BasicType::INT8);

    StreamCatalogPtr streamCatalog = std::make_shared<StreamCatalog>();
    streamCatalog->removeLogicalStream("default_logical");
    streamCatalog->addLogicalStream("default_logical", inputSchema);

    auto source = LogicalOperatorFactory::createSourceOperator(LogicalStreamSourceDescriptor::create("default_logical"));
    auto map = LogicalOperatorFactory::createMapOperator(Attribute("f3") = Attribute("f1") * 42);
    auto sink = LogicalOperatorFactory::createSinkOperator(FileSinkDescriptor::create(""));

    auto plan = QueryPlan::create(source);
    plan->appendOperatorAsNewRoot(map);
    plan->appendOperatorAsNewRoot(sink);

    auto phase = TypeInferencePhase::create(streamCatalog);
    auto resultPlan = phase->execute(plan);

    // we just access the old references
    auto expectedInputSchema = Schema::create();
    expectedInputSchema->addField("default_logical$f1", BasicType::INT32);
    expectedInputSchema->addField("default_logical$f2", BasicType::INT8);

    ASSERT_TRUE(source->getOutputSchema()->equals(expectedInputSchema));

    auto mappedSchema = Schema::create();
    mappedSchema->addField("default_logical$f1", BasicType::INT32);
    mappedSchema->addField("default_logical$f2", BasicType::INT8);
    mappedSchema->addField("_$f3", BasicType::INT8);

    ASSERT_TRUE(map->getOutputSchema()->equals(mappedSchema));
    ASSERT_TRUE(sink->getOutputSchema()->equals(mappedSchema));
}

/**
 * @brief In this test we infer the output and input schemas of each operator in a query.
 */
TEST_F(TypeInferencePhaseTest, inferWindowQuery) {

    auto query = Query::from("default_logical")
                     .windowByKey(Attribute("id"), TumblingWindow::of(TimeCharacteristic::createIngestionTime(), Seconds(10)),
                                  Sum(Attribute("value")))
                     .sink(FileSinkDescriptor::create(""));

    StreamCatalogPtr streamCatalog = std::make_shared<StreamCatalog>();
    auto phase = TypeInferencePhase::create(streamCatalog);
    auto resultPlan = phase->execute(query.getQueryPlan());

    std::cout << resultPlan->getSinkOperators()[0]->getOutputSchema()->toString() << std::endl;
    // we just access the old references
    ASSERT_EQ(resultPlan->getSinkOperators()[0]->getOutputSchema()->getSize(), 5);
}

/**
 * @brief In this test we try to infer the output and input scheas of an invalid query. This should fail.
 */
TEST_F(TypeInferencePhaseTest, inferQueryPlanError) {

    auto inputSchema = Schema::create();
    inputSchema->addField("f1", BasicType::INT32);
    inputSchema->addField("f2", BasicType::INT8);

    auto source = LogicalOperatorFactory::createSourceOperator(LogicalStreamSourceDescriptor::create("default_logical"));
    auto map = LogicalOperatorFactory::createMapOperator(Attribute("f3") = Attribute("f3") * 42);
    auto sink = LogicalOperatorFactory::createSinkOperator(FileSinkDescriptor::create(""));

    auto plan = QueryPlan::create(source);
    plan->appendOperatorAsNewRoot(map);
    plan->appendOperatorAsNewRoot(sink);

    StreamCatalogPtr streamCatalog = std::make_shared<StreamCatalog>();
    auto phase = TypeInferencePhase::create(streamCatalog);
    ASSERT_ANY_THROW(phase->execute(plan));
}

/**
 * @brief In this test we ensure that the source descriptor is correctly replaced, such that the schema can be propagated.
 */
TEST_F(TypeInferencePhaseTest, inferQuerySourceReplace) {

    auto query = Query::from("default_logical").map(Attribute("f3") = Attribute("id")++).sink(FileSinkDescriptor::create(""));
    auto plan = query.getQueryPlan();

    StreamCatalogPtr streamCatalog = std::make_shared<StreamCatalog>();
    auto phase = TypeInferencePhase::create(streamCatalog);
    plan = phase->execute(plan);
    auto sink = plan->getSinkOperators()[0];

    auto resultSchema = Schema::create()
                            ->addField("default_logical$id", BasicType::UINT32)
                            ->addField("default_logical$value", BasicType::UINT64)
                            ->addField("_$f3", BasicType::UINT32);

    NES_INFO(sink->getOutputSchema()->toString());

    ASSERT_TRUE(sink->getOutputSchema()->equals(resultSchema));
}

/**
 * @brief In this test we ensure that the schema can be propagated properly when merge operator is present.
 */
TEST_F(TypeInferencePhaseTest, inferQueryWithMergeOperator) {

    Query subQuery = Query::from("default_logical");
    auto query = Query::from("default_logical")
                     .merge(&subQuery)
                     .map(Attribute("f3") = Attribute("id")++)
                     .sink(FileSinkDescriptor::create(""));
    auto plan = query.getQueryPlan();

    StreamCatalogPtr streamCatalog = std::make_shared<StreamCatalog>();
    auto phase = TypeInferencePhase::create(streamCatalog);
    plan = phase->execute(plan);
    auto sink = plan->getSinkOperators()[0];

    auto resultSchema = Schema::create()
                            ->addField("default_logical$id", BasicType::UINT32)
                            ->addField("default_logical$value", BasicType::UINT64)
                            ->addField("_$f3", BasicType::UINT32);

    NES_INFO(sink->getOutputSchema()->toString());
    ASSERT_TRUE(sink->getOutputSchema()->equals(resultSchema));
}

/**
 * @brief In this test we test the rename operator
 */
TEST_F(TypeInferencePhaseTest, inferQueryRenameBothAttributes) {

    auto inputSchema = Schema::create();
    inputSchema->addField("f1", BasicType::INT32);
    inputSchema->addField("f2", BasicType::INT8);

    auto source = LogicalOperatorFactory::createSourceOperator(LogicalStreamSourceDescriptor::create("default_logical"));
    auto map = LogicalOperatorFactory::createMapOperator(Attribute("f3").rename("f4") = Attribute("f3").rename("f5") * 42);
    auto sink = LogicalOperatorFactory::createSinkOperator(FileSinkDescriptor::create(""));

    auto plan = QueryPlan::create(source);
    plan->appendOperatorAsNewRoot(map);
    plan->appendOperatorAsNewRoot(sink);

    StreamCatalogPtr streamCatalog = std::make_shared<StreamCatalog>();
    TopologyNodePtr physicalNode = TopologyNode::create(1, "localhost", 4000, 4002, 4);

    PhysicalStreamConfigPtr streamConf = PhysicalStreamConfig::create();

    StreamCatalogEntryPtr sce = std::make_shared<StreamCatalogEntry>(streamConf, physicalNode);
    streamCatalog->addPhysicalStream("default_logical", sce);
    auto phase = TypeInferencePhase::create(streamCatalog);
    ASSERT_ANY_THROW(phase->execute(plan));
}

/**
 * @brief In this test we test the rename operator
 */
TEST_F(TypeInferencePhaseTest, inferQueryRenameOneAttribute) {

    auto inputSchema = Schema::create();
    inputSchema->addField("f1", BasicType::INT32);
    inputSchema->addField("f2", BasicType::INT8);

    auto source = LogicalOperatorFactory::createSourceOperator(LogicalStreamSourceDescriptor::create("default_logical"));
    auto map = LogicalOperatorFactory::createMapOperator(Attribute("f3").rename("f4") = Attribute("f3") * 42);
    auto sink = LogicalOperatorFactory::createSinkOperator(FileSinkDescriptor::create(""));

    auto plan = QueryPlan::create(source);
    plan->appendOperatorAsNewRoot(map);
    plan->appendOperatorAsNewRoot(sink);

    StreamCatalogPtr streamCatalog = std::make_shared<StreamCatalog>();
    TopologyNodePtr physicalNode = TopologyNode::create(1, "localhost", 4000, 4002, 4);

    PhysicalStreamConfigPtr streamConf = PhysicalStreamConfig::create();

    StreamCatalogEntryPtr sce = std::make_shared<StreamCatalogEntry>(streamConf, physicalNode);
    streamCatalog->addPhysicalStream("default_logical", sce);
    auto phase = TypeInferencePhase::create(streamCatalog);
    ASSERT_ANY_THROW(phase->execute(plan));
}

/**
     * @brief In this test we test the rename operator
     */
TEST_F(TypeInferencePhaseTest, inferQueryRenameinAssignment) {

    auto inputSchema = Schema::create();
    inputSchema->addField("f1", BasicType::INT32);
    inputSchema->addField("f2", BasicType::INT8);

    auto source = LogicalOperatorFactory::createSourceOperator(LogicalStreamSourceDescriptor::create("default_logical"));
    auto map = LogicalOperatorFactory::createMapOperator(Attribute("f3").rename("f4") = 42);
    auto sink = LogicalOperatorFactory::createSinkOperator(FileSinkDescriptor::create(""));

    auto plan = QueryPlan::create(source);
    plan->appendOperatorAsNewRoot(map);
    plan->appendOperatorAsNewRoot(sink);

    StreamCatalogPtr streamCatalog = std::make_shared<StreamCatalog>();
    auto phase = TypeInferencePhase::create(streamCatalog);
    auto maps = plan->getOperatorByType<MapLogicalOperatorNode>();
    phase->execute(plan);
    NES_DEBUG("result schema is=" << maps[0]->getOutputSchema()->toString());
    //we have to forbit the renaming of the attribute in the assignment statement of the map
    ASSERT_NE(maps[0]->getOutputSchema()->getIndex("f4"), 2);
}

/**
 * @brief In this test we test the rename operator inside a project operator
 */
TEST_F(TypeInferencePhaseTest, inferTypeForSimpleQuery) {

    auto inputSchema = Schema::create();
    inputSchema->addField("f1", BasicType::INT32);
    inputSchema->addField("f2", BasicType::INT8);
    StreamCatalogPtr streamCatalog = std::make_shared<StreamCatalog>();
    streamCatalog->removeLogicalStream("default_logical");
    streamCatalog->addLogicalStream("default_logical", inputSchema);

    auto query = Query::from("default_logical")
                     .filter(Attribute("f2") < 42)
                     .map(Attribute("f1") = Attribute("f1") + 2)
                     .sink(FileSinkDescriptor::create(""));
    auto plan = query.getQueryPlan();

    auto phase = TypeInferencePhase::create(streamCatalog);
    plan = phase->execute(plan);
    auto sourceOperator = plan->getOperatorByType<SourceLogicalOperatorNode>();
    auto filterOperator = plan->getOperatorByType<FilterLogicalOperatorNode>();
    auto mapOperator = plan->getOperatorByType<MapLogicalOperatorNode>();
    auto sinkOperator = plan->getOperatorByType<SinkLogicalOperatorNode>();

    SchemaPtr filterOutputSchema = filterOperator[0]->getOutputSchema();
    ASSERT_TRUE(filterOutputSchema->fields.size() == 2);
    ASSERT_TRUE(filterOutputSchema->hasFieldName("default_logical$f2"));
    ASSERT_TRUE(filterOutputSchema->hasFieldName("default_logical$f1"));

    SchemaPtr sourceOutputSchema = sourceOperator[0]->getOutputSchema();
    ASSERT_TRUE(sourceOutputSchema->fields.size() == 2);
    ASSERT_TRUE(sourceOutputSchema->hasFieldName("default_logical$f2"));
    ASSERT_TRUE(sourceOutputSchema->hasFieldName("default_logical$f1"));

    SchemaPtr mapOutputSchema = mapOperator[0]->getOutputSchema();
    ASSERT_TRUE(mapOutputSchema->fields.size() == 2);
    ASSERT_TRUE(mapOutputSchema->hasFieldName("default_logical$f2"));
    ASSERT_TRUE(mapOutputSchema->hasFieldName("default_logical$f1"));

    SchemaPtr sinkOutputSchema = sinkOperator[0]->getOutputSchema();
    ASSERT_TRUE(sinkOutputSchema->fields.size() == 2);
    ASSERT_TRUE(sinkOutputSchema->hasFieldName("f2"));
    ASSERT_TRUE(sinkOutputSchema->hasFieldName("f1"));
}

/**
 * @brief In this test we test the type inference for query with Project operator
 */
TEST_F(TypeInferencePhaseTest, inferQueryWithProject) {

    auto inputSchema = Schema::create();
    inputSchema->addField("f1", BasicType::INT32);
    inputSchema->addField("f2", BasicType::INT8);
    StreamCatalogPtr streamCatalog = std::make_shared<StreamCatalog>();
    streamCatalog->removeLogicalStream("default_logical");
    streamCatalog->addLogicalStream("default_logical", inputSchema);

    auto query = Query::from("default_logical")
                     .filter(Attribute("f2") < 42)
                     .map(Attribute("f1") = Attribute("f1") + 2)
                     .project(Attribute("f1").rename("f3"), Attribute("f2").rename("f4"))
                     .sink(FileSinkDescriptor::create(""));
    auto plan = query.getQueryPlan();

    auto phase = TypeInferencePhase::create(streamCatalog);
    plan = phase->execute(plan);
    auto sourceOperator = plan->getOperatorByType<SourceLogicalOperatorNode>();
    auto filterOperator = plan->getOperatorByType<FilterLogicalOperatorNode>();
    auto mapOperator = plan->getOperatorByType<MapLogicalOperatorNode>();
    auto projectOperator = plan->getOperatorByType<ProjectionLogicalOperatorNode>();
    auto sinkOperator = plan->getOperatorByType<SinkLogicalOperatorNode>();

    SchemaPtr filterOutputSchema = filterOperator[0]->getOutputSchema();
    ASSERT_TRUE(filterOutputSchema->fields.size() == 2);
    ASSERT_TRUE(filterOutputSchema->hasFieldName("default_logical$f2"));
    ASSERT_TRUE(filterOutputSchema->hasFieldName("default_logical$f1"));

    SchemaPtr sourceOutputSchema = sourceOperator[0]->getOutputSchema();
    ASSERT_TRUE(sourceOutputSchema->fields.size() == 2);
    ASSERT_TRUE(sourceOutputSchema->hasFieldName("default_logical$f2"));
    ASSERT_TRUE(sourceOutputSchema->hasFieldName("default_logical$f1"));

    SchemaPtr mapOutputSchema = mapOperator[0]->getOutputSchema();
    ASSERT_TRUE(mapOutputSchema->fields.size() == 2);
    ASSERT_TRUE(mapOutputSchema->hasFieldName("default_logical$f2"));
    ASSERT_TRUE(mapOutputSchema->hasFieldName("default_logical$f1"));

    SchemaPtr projectOutputSchema = projectOperator[0]->getOutputSchema();
    ASSERT_TRUE(projectOutputSchema->fields.size() == 2);
    ASSERT_TRUE(projectOutputSchema->hasFieldName("default_logical$f3"));
    ASSERT_TRUE(projectOutputSchema->hasFieldName("default_logical$f4"));

    SchemaPtr sinkOutputSchema = sinkOperator[0]->getOutputSchema();
    ASSERT_TRUE(sinkOutputSchema->fields.size() == 2);
    ASSERT_TRUE(sinkOutputSchema->hasFieldName("default_logical$f3"));
    ASSERT_TRUE(sinkOutputSchema->hasFieldName("default_logical$f4"));
}

/**
 * @brief In this test we test the type inference for query with Stream Rename operator
 */
TEST_F(TypeInferencePhaseTest, inferQueryWithRenameStream) {

    auto inputSchema = Schema::create();
    inputSchema->addField("f1", BasicType::INT32);
    inputSchema->addField("f2", BasicType::INT8);
    StreamCatalogPtr streamCatalog = std::make_shared<StreamCatalog>();
    streamCatalog->removeLogicalStream("default_logical");
    streamCatalog->addLogicalStream("default_logical", inputSchema);

    auto query = Query::from("default_logical")
                     .filter(Attribute("f2") < 42)
                     .map(Attribute("f1") = Attribute("f1") + 2)
                     .as("x")
                     .sink(FileSinkDescriptor::create(""));
    auto plan = query.getQueryPlan();

    auto phase = TypeInferencePhase::create(streamCatalog);
    plan = phase->execute(plan);
    auto sourceOperator = plan->getOperatorByType<SourceLogicalOperatorNode>();
    auto filterOperator = plan->getOperatorByType<FilterLogicalOperatorNode>();
    auto mapOperator = plan->getOperatorByType<MapLogicalOperatorNode>();
    auto renameStreamOperator = plan->getOperatorByType<RenameStreamOperatorNode>();
    auto sinkOperator = plan->getOperatorByType<SinkLogicalOperatorNode>();

    SchemaPtr filterOutputSchema = filterOperator[0]->getOutputSchema();
    ASSERT_TRUE(filterOutputSchema->fields.size() == 2);
    ASSERT_TRUE(filterOutputSchema->hasFieldName("default_logical$f2"));
    ASSERT_TRUE(filterOutputSchema->hasFieldName("default_logical$f1"));

    SchemaPtr sourceOutputSchema = sourceOperator[0]->getOutputSchema();
    ASSERT_TRUE(sourceOutputSchema->fields.size() == 2);
    ASSERT_TRUE(sourceOutputSchema->hasFieldName("default_logical$f2"));
    ASSERT_TRUE(sourceOutputSchema->hasFieldName("default_logical$f1"));

    SchemaPtr mapOutputSchema = mapOperator[0]->getOutputSchema();
    ASSERT_TRUE(mapOutputSchema->fields.size() == 2);
    ASSERT_TRUE(mapOutputSchema->hasFieldName("default_logical$f2"));
    ASSERT_TRUE(mapOutputSchema->hasFieldName("default_logical$f1"));

    SchemaPtr renameStreamOutputSchema = renameStreamOperator[0]->getOutputSchema();
    ASSERT_TRUE(renameStreamOutputSchema->fields.size() == 2);
    ASSERT_TRUE(renameStreamOutputSchema->hasFieldName("x$f1"));
    ASSERT_TRUE(renameStreamOutputSchema->hasFieldName("x$f2"));

    SchemaPtr sinkOutputSchema = sinkOperator[0]->getOutputSchema();
    ASSERT_TRUE(sinkOutputSchema->fields.size() == 2);
    ASSERT_TRUE(sinkOutputSchema->hasFieldName("f1"));
    ASSERT_TRUE(sinkOutputSchema->hasFieldName("f2"));
}

/**
 * @brief In this test we test the type inference for query with Stream Rename and Project operators
 */
TEST_F(TypeInferencePhaseTest, inferQueryWithRenameStreamAndProject) {

    auto inputSchema = Schema::create();
    inputSchema->addField("f1", BasicType::INT32);
    inputSchema->addField("f2", BasicType::INT8);
    StreamCatalogPtr streamCatalog = std::make_shared<StreamCatalog>();
    streamCatalog->removeLogicalStream("default_logical");
    streamCatalog->addLogicalStream("default_logical", inputSchema);

    auto query = Query::from("default_logical")
                     .filter(Attribute("f2") < 42)
                     .project(Attribute("f1").rename("f3"), Attribute("f2").rename("f4"))
                     .map(Attribute("f3") = Attribute("f4") + 2)
                     .as("x")
                     .sink(FileSinkDescriptor::create(""));
    auto plan = query.getQueryPlan();

    auto phase = TypeInferencePhase::create(streamCatalog);
    plan = phase->execute(plan);
    auto sourceOperator = plan->getOperatorByType<SourceLogicalOperatorNode>();
    auto filterOperator = plan->getOperatorByType<FilterLogicalOperatorNode>();
    auto mapOperator = plan->getOperatorByType<MapLogicalOperatorNode>();
    auto projectOperator = plan->getOperatorByType<ProjectionLogicalOperatorNode>();
    auto renameStreamOperator = plan->getOperatorByType<RenameStreamOperatorNode>();
    auto sinkOperator = plan->getOperatorByType<SinkLogicalOperatorNode>();

    SchemaPtr sourceOutputSchema = sourceOperator[0]->getOutputSchema();
    ASSERT_TRUE(sourceOutputSchema->fields.size() == 2);
    ASSERT_TRUE(sourceOutputSchema->hasFieldName("default_logical$f2"));
    ASSERT_TRUE(sourceOutputSchema->hasFieldName("default_logical$f1"));

    SchemaPtr filterOutputSchema = filterOperator[0]->getOutputSchema();
    ASSERT_TRUE(filterOutputSchema->fields.size() == 2);
    ASSERT_TRUE(filterOutputSchema->hasFieldName("default_logical$f2"));
    ASSERT_TRUE(filterOutputSchema->hasFieldName("default_logical$f1"));

    SchemaPtr projectOutputSchema = projectOperator[0]->getOutputSchema();
    ASSERT_TRUE(projectOutputSchema->fields.size() == 2);
    ASSERT_TRUE(projectOutputSchema->hasFieldName("default_logical$f3"));
    ASSERT_TRUE(projectOutputSchema->hasFieldName("default_logical$f4"));

    SchemaPtr mapOutputSchema = mapOperator[0]->getOutputSchema();
    ASSERT_TRUE(mapOutputSchema->fields.size() == 2);
    ASSERT_TRUE(mapOutputSchema->hasFieldName("default_logical$f3"));
    ASSERT_TRUE(mapOutputSchema->hasFieldName("default_logical$f4"));

    SchemaPtr renameStreamOutputSchema = renameStreamOperator[0]->getOutputSchema();
    ASSERT_TRUE(renameStreamOutputSchema->fields.size() == 2);
    ASSERT_TRUE(renameStreamOutputSchema->hasFieldName("x$f3"));
    ASSERT_TRUE(renameStreamOutputSchema->hasFieldName("x$f4"));

    SchemaPtr sinkOutputSchema = sinkOperator[0]->getOutputSchema();
    ASSERT_TRUE(sinkOutputSchema->fields.size() == 2);
    ASSERT_TRUE(sinkOutputSchema->hasFieldName("f3"));
    ASSERT_TRUE(sinkOutputSchema->hasFieldName("f4"));
}

/**
 * @brief In this test we test the type inference for query with fully qualified attribute names
 */
TEST_F(TypeInferencePhaseTest, inferQueryWithPartlyOrFullyQualifiedAttributes) {

    auto inputSchema = Schema::create();
    inputSchema->addField("f1", BasicType::INT32);
    inputSchema->addField("f2", BasicType::INT8);
    StreamCatalogPtr streamCatalog = std::make_shared<StreamCatalog>();
    streamCatalog->removeLogicalStream("default_logical");
    streamCatalog->addLogicalStream("default_logical", inputSchema);

    auto query = Query::from("default_logical")
                     .filter(Attribute("default_logical$f2") < 42)
                     .map(Attribute("f1") = Attribute("f2") + 2)
                     .sink(FileSinkDescriptor::create(""));
    auto plan = query.getQueryPlan();

    auto phase = TypeInferencePhase::create(streamCatalog);
    plan = phase->execute(plan);
    auto sourceOperator = plan->getOperatorByType<SourceLogicalOperatorNode>();
    auto filterOperator = plan->getOperatorByType<FilterLogicalOperatorNode>();
    auto mapOperator = plan->getOperatorByType<MapLogicalOperatorNode>();
    auto sinkOperator = plan->getOperatorByType<SinkLogicalOperatorNode>();

    SchemaPtr sourceOutputSchema = sourceOperator[0]->getOutputSchema();
    ASSERT_TRUE(sourceOutputSchema->fields.size() == 2);
    ASSERT_TRUE(sourceOutputSchema->hasFieldName("default_logical$f1"));
    ASSERT_TRUE(sourceOutputSchema->hasFieldName("default_logical$f2"));

    SchemaPtr filterOutputSchema = filterOperator[0]->getOutputSchema();
    ASSERT_TRUE(filterOutputSchema->fields.size() == 2);
    ASSERT_TRUE(filterOutputSchema->hasFieldName("default_logical$f1"));
    ASSERT_TRUE(filterOutputSchema->hasFieldName("default_logical$f2"));

    SchemaPtr mapOutputSchema = mapOperator[0]->getOutputSchema();
    ASSERT_TRUE(mapOutputSchema->fields.size() == 2);
    ASSERT_TRUE(mapOutputSchema->hasFieldName("default_logical$f1"));
    ASSERT_TRUE(mapOutputSchema->hasFieldName("default_logical$f2"));

    SchemaPtr sinkOutputSchema = sinkOperator[0]->getOutputSchema();
    ASSERT_TRUE(sinkOutputSchema->fields.size() == 2);
    ASSERT_TRUE(sinkOutputSchema->hasFieldName("f1"));
    ASSERT_TRUE(sinkOutputSchema->hasFieldName("f2"));
}

/**
 * @brief In this test we test the type inference for query with Stream Rename and Project operators with fully qualified stream name
 */
TEST_F(TypeInferencePhaseTest, inferQueryWithRenameStreamAndProjectWithFullyQualifiedNames) {

    auto inputSchema = Schema::create();
    inputSchema->addField("f1", BasicType::INT32);
    inputSchema->addField("f2", BasicType::INT8);
    StreamCatalogPtr streamCatalog = std::make_shared<StreamCatalog>();
    streamCatalog->removeLogicalStream("default_logical");
    streamCatalog->addLogicalStream("default_logical", inputSchema);

    auto query = Query::from("default_logical")
                     .filter(Attribute("f2") < 42)
                     .project(Attribute("f1").rename("default_logical$f3"), Attribute("f2").rename("f4"))
                     .map(Attribute("default_logical$f3") = Attribute("f4") + 2)
                     .as("x")
                     .sink(FileSinkDescriptor::create(""));
    auto plan = query.getQueryPlan();

    auto phase = TypeInferencePhase::create(streamCatalog);
    plan = phase->execute(plan);
    auto sourceOperator = plan->getOperatorByType<SourceLogicalOperatorNode>();
    auto filterOperator = plan->getOperatorByType<FilterLogicalOperatorNode>();
    auto mapOperator = plan->getOperatorByType<MapLogicalOperatorNode>();
    auto projectOperator = plan->getOperatorByType<ProjectionLogicalOperatorNode>();
    auto renameStreamOperator = plan->getOperatorByType<RenameStreamOperatorNode>();
    auto sinkOperator = plan->getOperatorByType<SinkLogicalOperatorNode>();

    SchemaPtr sourceOutputSchema = sourceOperator[0]->getOutputSchema();
    ASSERT_TRUE(sourceOutputSchema->fields.size() == 2);
    ASSERT_TRUE(sourceOutputSchema->hasFieldName("default_logical$f2"));
    ASSERT_TRUE(sourceOutputSchema->hasFieldName("default_logical$f1"));

    SchemaPtr filterOutputSchema = filterOperator[0]->getOutputSchema();
    ASSERT_TRUE(filterOutputSchema->fields.size() == 2);
    ASSERT_TRUE(filterOutputSchema->hasFieldName("default_logical$f2"));
    ASSERT_TRUE(filterOutputSchema->hasFieldName("default_logical$f1"));

    SchemaPtr projectOutputSchema = projectOperator[0]->getOutputSchema();
    ASSERT_TRUE(projectOutputSchema->fields.size() == 2);
    ASSERT_TRUE(projectOutputSchema->hasFieldName("default_logical$f3"));
    ASSERT_TRUE(projectOutputSchema->hasFieldName("default_logical$f4"));

    SchemaPtr mapOutputSchema = mapOperator[0]->getOutputSchema();
    ASSERT_TRUE(mapOutputSchema->fields.size() == 2);
    ASSERT_TRUE(mapOutputSchema->hasFieldName("default_logical$f3"));
    ASSERT_TRUE(mapOutputSchema->hasFieldName("default_logical$f4"));

    SchemaPtr renameStreamOutputSchema = renameStreamOperator[0]->getOutputSchema();
    ASSERT_TRUE(renameStreamOutputSchema->fields.size() == 2);
    ASSERT_TRUE(renameStreamOutputSchema->hasFieldName("x$f3"));
    ASSERT_TRUE(renameStreamOutputSchema->hasFieldName("x$f4"));

    SchemaPtr sinkOutputSchema = sinkOperator[0]->getOutputSchema();
    ASSERT_TRUE(sinkOutputSchema->fields.size() == 2);
    ASSERT_TRUE(sinkOutputSchema->hasFieldName("f3"));
    ASSERT_TRUE(sinkOutputSchema->hasFieldName("f4"));
}

/**
 * @brief In this test we test the type inference for query with Merge, Stream Rename and Project operators with fully qualified stream name
 */
TEST_F(TypeInferencePhaseTest, inferQueryWithRenameStreamAndProjectWithFullyQualifiedNamesAndMergeOperator) {

    auto inputSchema = Schema::create();
    inputSchema->addField("f1", BasicType::INT32);
    inputSchema->addField("f2", BasicType::INT8);
    StreamCatalogPtr streamCatalog = std::make_shared<StreamCatalog>();
    streamCatalog->removeLogicalStream("default_logical");
    streamCatalog->addLogicalStream("default_logical", inputSchema);

    auto subQuery = Query::from("default_logical");

    auto query = Query::from("default_logical")
                     .merge(&subQuery)
                     .filter(Attribute("f2") < 42)
                     .project(Attribute("f1").rename("default_logical$f3"), Attribute("f2").rename("f4"))
                     .map(Attribute("default_logical$f3") = Attribute("f4") + 2)
                     .as("x")
                     .sink(FileSinkDescriptor::create(""));
    auto plan = query.getQueryPlan();

    auto phase = TypeInferencePhase::create(streamCatalog);
    plan = phase->execute(plan);
    auto sourceOperator = plan->getOperatorByType<SourceLogicalOperatorNode>();
    auto mergeOperator = plan->getOperatorByType<MergeLogicalOperatorNode>();
    auto filterOperator = plan->getOperatorByType<FilterLogicalOperatorNode>();
    auto mapOperator = plan->getOperatorByType<MapLogicalOperatorNode>();
    auto projectOperator = plan->getOperatorByType<ProjectionLogicalOperatorNode>();
    auto renameStreamOperator = plan->getOperatorByType<RenameStreamOperatorNode>();
    auto sinkOperator = plan->getOperatorByType<SinkLogicalOperatorNode>();

    SchemaPtr sourceOutputSchema = sourceOperator[0]->getOutputSchema();
    ASSERT_TRUE(sourceOutputSchema->fields.size() == 2);
    ASSERT_TRUE(sourceOutputSchema->hasFieldName("default_logical$f2"));
    ASSERT_TRUE(sourceOutputSchema->hasFieldName("default_logical$f1"));

    SchemaPtr mergeOutputSchema = mergeOperator[0]->getOutputSchema();
    ASSERT_TRUE(mergeOutputSchema->fields.size() == 2);
    ASSERT_TRUE(mergeOutputSchema->hasFieldName("default_logical$f2"));
    ASSERT_TRUE(mergeOutputSchema->hasFieldName("default_logical$f1"));

    SchemaPtr filterOutputSchema = filterOperator[0]->getOutputSchema();
    ASSERT_TRUE(filterOutputSchema->fields.size() == 2);
    ASSERT_TRUE(filterOutputSchema->hasFieldName("default_logical$f2"));
    ASSERT_TRUE(filterOutputSchema->hasFieldName("default_logical$f1"));

    SchemaPtr projectOutputSchema = projectOperator[0]->getOutputSchema();
    ASSERT_TRUE(projectOutputSchema->fields.size() == 2);
    ASSERT_TRUE(projectOutputSchema->hasFieldName("default_logical$f3"));
    ASSERT_TRUE(projectOutputSchema->hasFieldName("default_logical$f4"));

    SchemaPtr mapOutputSchema = mapOperator[0]->getOutputSchema();
    ASSERT_TRUE(mapOutputSchema->fields.size() == 2);
    ASSERT_TRUE(mapOutputSchema->hasFieldName("default_logical$f3"));
    ASSERT_TRUE(mapOutputSchema->hasFieldName("default_logical$f4"));

    SchemaPtr renameStreamOutputSchema = renameStreamOperator[0]->getOutputSchema();
    ASSERT_TRUE(renameStreamOutputSchema->fields.size() == 2);
    ASSERT_TRUE(renameStreamOutputSchema->hasFieldName("x$f3"));
    ASSERT_TRUE(renameStreamOutputSchema->hasFieldName("x$f4"));

    SchemaPtr sinkOutputSchema = sinkOperator[0]->getOutputSchema();
    ASSERT_TRUE(sinkOutputSchema->fields.size() == 2);
    ASSERT_TRUE(sinkOutputSchema->hasFieldName("x$f3"));
    ASSERT_TRUE(sinkOutputSchema->hasFieldName("x$f4"));
}

/**
 * @brief In this test we test the type inference for query with Join, Stream Rename and Project operators with fully qualified stream name
 */
TEST_F(TypeInferencePhaseTest, inferQueryWithRenameStreamAndProjectWithFullyQualifiedNamesAndJoinOperator) {

    auto inputSchema =
        Schema::create()->addField("f1", BasicType::INT32)->addField("f2", BasicType::INT8)->addField("ts", BasicType::INT64);
    StreamCatalogPtr streamCatalog = std::make_shared<StreamCatalog>();
    streamCatalog->removeLogicalStream("default_logical");
    streamCatalog->addLogicalStream("default_logical", inputSchema);

    auto windowType1 = TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(4));
    auto subQuery = Query::from("default_logical").as("x");
    auto query = Query::from("default_logical")
                     .as("y")
                     .join(&subQuery, Attribute("f1"), Attribute("f1"), windowType1)
                     .filter(Attribute("x$f2") < 42)
                     .project(Attribute("x$f1").rename("default_logical$f3"), Attribute("y$f2").rename("f4"))
                     .map(Attribute("default_logical$f3") = Attribute("f4") + 2)
                     .as("x")
                     .sink(FileSinkDescriptor::create(""));
    auto plan = query.getQueryPlan();

    auto phase = TypeInferencePhase::create(streamCatalog);
    plan = phase->execute(plan);
    auto sourceOperator = plan->getOperatorByType<SourceLogicalOperatorNode>();
    auto filterOperator = plan->getOperatorByType<FilterLogicalOperatorNode>();
    auto mapOperator = plan->getOperatorByType<MapLogicalOperatorNode>();
    auto projectOperator = plan->getOperatorByType<ProjectionLogicalOperatorNode>();
    auto renameStreamOperator = plan->getOperatorByType<RenameStreamOperatorNode>();
    auto sinkOperator = plan->getOperatorByType<SinkLogicalOperatorNode>();

    SchemaPtr sourceOutputSchema = sourceOperator[0]->getOutputSchema();
    ASSERT_TRUE(sourceOutputSchema->fields.size() == 3);
    ASSERT_TRUE(sourceOutputSchema->hasFieldName("default_logical$f2"));
    ASSERT_TRUE(sourceOutputSchema->hasFieldName("default_logical$f1"));
    ASSERT_TRUE(sourceOutputSchema->hasFieldName("default_logical$ts"));

    SchemaPtr filterOutputSchema = filterOperator[0]->getOutputSchema();
    ASSERT_TRUE(filterOutputSchema->fields.size() == 9);
    ASSERT_TRUE(filterOutputSchema->hasFieldName("x$f2"));
    ASSERT_TRUE(filterOutputSchema->hasFieldName("x$f1"));
    ASSERT_TRUE(filterOutputSchema->hasFieldName("x$ts"));
    ASSERT_TRUE(filterOutputSchema->hasFieldName("y$f2"));
    ASSERT_TRUE(filterOutputSchema->hasFieldName("y$f1"));
    ASSERT_TRUE(filterOutputSchema->hasFieldName("y$ts"));
    ASSERT_TRUE(filterOutputSchema->hasFieldName("_$start"));
    ASSERT_TRUE(filterOutputSchema->hasFieldName("_$end"));
    ASSERT_TRUE(filterOutputSchema->hasFieldName("_$key"));

    SchemaPtr projectOutputSchema = projectOperator[0]->getOutputSchema();
    ASSERT_TRUE(projectOutputSchema->fields.size() == 2);
    ASSERT_TRUE(projectOutputSchema->hasFieldName("default_logical$f3"));
    ASSERT_TRUE(projectOutputSchema->hasFieldName("y$f4"));

    SchemaPtr mapOutputSchema = mapOperator[0]->getOutputSchema();
    ASSERT_TRUE(mapOutputSchema->fields.size() == 2);
    ASSERT_TRUE(mapOutputSchema->hasFieldName("default_logical$f3"));
    ASSERT_TRUE(mapOutputSchema->hasFieldName("y$f4"));

    SchemaPtr renameStreamOutputSchema = renameStreamOperator[0]->getOutputSchema();
    ASSERT_TRUE(renameStreamOutputSchema->fields.size() == 2);
    ASSERT_TRUE(renameStreamOutputSchema->hasFieldName("x$f3"));
    ASSERT_TRUE(renameStreamOutputSchema->hasFieldName("x$f4"));

    SchemaPtr sinkOutputSchema = sinkOperator[0]->getOutputSchema();
    ASSERT_TRUE(sinkOutputSchema->fields.size() == 2);
    ASSERT_TRUE(sinkOutputSchema->hasFieldName("x$f3"));
    ASSERT_TRUE(sinkOutputSchema->hasFieldName("x$f4"));
}
}// namespace NES
