#include <gtest/gtest.h>//
#include <Nodes/Expressions/ConstantValueExpressionNode.hpp>
#include <Nodes/Operators/LogicalOperators/FilterLogicalOperatorNode.hpp>
#include <Nodes/Operators/LogicalOperators/LogicalOperatorNode.hpp>
#include <Nodes/Operators/LogicalOperators/Sinks/PrintSinkDescriptor.hpp>
#include <Nodes/Operators/LogicalOperators/Sources/DefaultSourceDescriptor.hpp>
#include <Nodes/Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Nodes/Operators/OperatorNode.hpp>
#include <Nodes/Util/ConsoleDumpHandler.hpp>
#include <Nodes/Util/DumpContext.hpp>
#include <Util/Logger.hpp>

#include <iostream>
#include <NodeEngine/NodeEngine.hpp>
#include <API/UserAPIExpression.hpp>
#include <Catalogs/StreamCatalog.hpp>
#include <Catalogs/LogicalStream.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Sources/DefaultSource.hpp>
#include <memory>

#include <API/Expressions/Expressions.hpp>
#include <API/Expressions/LogicalExpressions.hpp>
#include <Nodes/Expressions/FieldAccessExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/EqualsExpressionNode.hpp>
#include <Nodes/Util/Iterators/BreadthFirstNodeIterator.hpp>
#include <Nodes/Util/Iterators/DepthFirstNodeIterator.hpp>
#include <Phases/TranslateToLegacyPlanPhase.hpp>
#include <Phases/TypeInferencePhase.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <QueryCompiler/GeneratableOperators/GeneratableCompleteWindowOperator.hpp>
#include <QueryCompiler/GeneratableOperators/GeneratableFilterOperator.hpp>
#include <QueryCompiler/GeneratableOperators/GeneratableScanOperator.hpp>
#include <QueryCompiler/GeneratableOperators/GeneratableSinkOperator.hpp>
#include <QueryCompiler/GeneratableOperators/TranslateToGeneratableOperatorPhase.hpp>
#include <Util/UtilityFunctions.hpp>

using namespace std;
namespace NES {

class TranslateToGeneratableOperatorPhaseTest : public testing::Test {
  public:

    static void SetUpTestCase() {
        NES::setupLogging("QueryDeploymentTest.log", NES::LOG_DEBUG);
        NES_INFO("Setup QueryDeploymentTest test class.");
    }

    void SetUp() {
        dumpContext = DumpContext::create();
        dumpContext->registerDumpHandler(ConsoleDumpHandler::create());

        StreamCatalogPtr streamCatalog = std::make_shared<StreamCatalog>();
        sPtr = streamCatalog->getStreamForLogicalStreamOrThrowException("default_logical");
        SchemaPtr schema = sPtr->getSchema();
        auto sourceDescriptor = DefaultSourceDescriptor::create(schema, /*number of buffers*/ 0, /*frequency*/ 0u);

        pred1 = ConstantValueExpressionNode::create(DataTypeFactory::createBasicValue(DataTypeFactory::createInt8(), "1"));
        pred2 = ConstantValueExpressionNode::create(DataTypeFactory::createBasicValue(DataTypeFactory::createInt8(), "2"));
        pred3 = ConstantValueExpressionNode::create(DataTypeFactory::createBasicValue(DataTypeFactory::createInt8(), "3"));
        pred4 = ConstantValueExpressionNode::create(DataTypeFactory::createBasicValue(DataTypeFactory::createInt8(), "4"));
        pred5 = ConstantValueExpressionNode::create(DataTypeFactory::createBasicValue(DataTypeFactory::createInt8(), "5"));
        pred6 = ConstantValueExpressionNode::create(DataTypeFactory::createBasicValue(DataTypeFactory::createInt8(), "6"));
        pred7 = ConstantValueExpressionNode::create(DataTypeFactory::createBasicValue(DataTypeFactory::createInt8(), "7"));

        sourceOp = createSourceLogicalOperatorNode(sourceDescriptor);
        sourceOp->setId(0);
        filterOp1 = createFilterLogicalOperatorNode(pred1);
        filterOp1->setId(1);
        filterOp2 = createFilterLogicalOperatorNode(pred2);
        filterOp2->setId(2);
        filterOp3 = createFilterLogicalOperatorNode(pred3);
        filterOp3->setId(3);
        filterOp4 = createFilterLogicalOperatorNode(pred4);
        filterOp4->setId(4);
        filterOp5 = createFilterLogicalOperatorNode(pred5);
        filterOp5->setId(5);
        filterOp6 = createFilterLogicalOperatorNode(pred6);
        filterOp6->setId(6);
        filterOp7 = createFilterLogicalOperatorNode(pred7);
        filterOp7->setId(7);

        filterOp1Copy = createFilterLogicalOperatorNode(pred1);
        filterOp1Copy->setId(8);
        filterOp2Copy = createFilterLogicalOperatorNode(pred2);
        filterOp2Copy->setId(9);
        filterOp3Copy = createFilterLogicalOperatorNode(pred3);
        filterOp3Copy->setId(10);
        filterOp4Copy = createFilterLogicalOperatorNode(pred4);
        filterOp4Copy->setId(11);
        filterOp5Copy = createFilterLogicalOperatorNode(pred5);
        filterOp5Copy->setId(12);
        filterOp6Copy = createFilterLogicalOperatorNode(pred6);
        filterOp6Copy->setId(13);
        filterOp7Copy = createFilterLogicalOperatorNode(pred7);
        filterOp7Copy->setId(14);

        removed = false;
        replaced = false;
        children.clear();
        parents.clear();
    }

    void TearDown() {
        NES_DEBUG("Tear down LogicalOperatorNode Test.");
    }

  protected:
    bool removed;
    bool replaced;
    LogicalStreamPtr sPtr;
    DumpContextPtr dumpContext;

    ExpressionNodePtr pred1, pred2, pred3, pred4, pred5, pred6, pred7;
    LogicalOperatorNodePtr sourceOp;

    LogicalOperatorNodePtr filterOp1, filterOp2, filterOp3, filterOp4, filterOp5, filterOp6, filterOp7;
    LogicalOperatorNodePtr filterOp1Copy, filterOp2Copy, filterOp3Copy, filterOp4Copy, filterOp5Copy, filterOp6Copy, filterOp7Copy;

    std::vector<NodePtr> children{};
    std::vector<NodePtr> parents{};
};

TEST_F(TranslateToGeneratableOperatorPhaseTest, translateFilterQuery) {
    /**
     * Sink -> Filter -> Source
     */
    auto schema = Schema::create();
    auto printSinkDescriptorPtr = PrintSinkDescriptor::create();
    auto sinkOperator = createSinkLogicalOperatorNode(printSinkDescriptorPtr);
    sinkOperator->setId(UtilityFunctions::getNextOperatorId());
    auto constValue = ConstantValueExpressionNode::create(DataTypeFactory::createBasicValue(DataTypeFactory::createInt8(), "1"));
    auto fieldRead = FieldAccessExpressionNode::create(DataTypeFactory::createInt8(), "FieldName");
    auto andNode = EqualsExpressionNode::create(constValue, fieldRead);
    auto filter = createFilterLogicalOperatorNode(andNode);
    filter->setId(UtilityFunctions::getNextOperatorId());
    sinkOperator->addChild(filter);
    filter->addChild(sourceOp);

    auto engine = NodeEngine::create("127.0.0.1", 30000);
    ConsoleDumpHandler::create()->dump(sinkOperator, std::cout);
    // we pass null as the buffer manager as we just want to check if the topology is correct.
    auto translatePhase = TranslateToGeneratableOperatorPhase::create();
    auto generatableSinkOperator = translatePhase->transform(sinkOperator->as<OperatorNode>());

    ASSERT_TRUE(generatableSinkOperator->instanceOf<GeneratableSinkOperator>());

    auto generatableFilterOperator = generatableSinkOperator->getChildren()[0];
    ASSERT_TRUE(generatableFilterOperator->instanceOf<GeneratableFilterOperator>());

    auto generatableSourceOperator = generatableFilterOperator->getChildren()[0];
    ASSERT_TRUE(generatableSourceOperator->instanceOf<GeneratableScanOperator>());
}

TEST_F(TranslateToGeneratableOperatorPhaseTest, translateWindowQuery) {
    /**
     * Sink -> Window -> Source
     */
    auto schema = Schema::create();
    schema->addField(AttributeField::create("f1", DataTypeFactory::createInt64()));

    auto printSinkDescriptorPtr = PrintSinkDescriptor::create();
    auto sinkOperator = createSinkLogicalOperatorNode(printSinkDescriptorPtr);
    sinkOperator->setId(UtilityFunctions::getNextOperatorId());
    auto windowOperator = createWindowLogicalOperatorNode(createWindowDefinition(Sum::on(Attribute("f1")), TumblingWindow::of(TimeCharacteristic::createProcessingTime(), Seconds(10)), DistributionCharacteristic::createCompleteWindowType()));
    windowOperator->setId(UtilityFunctions::getNextOperatorId());
    sinkOperator->addChild(windowOperator);
    windowOperator->addChild(sourceOp);

    auto engine = NodeEngine::create("127.0.0.1", 30000);
    ConsoleDumpHandler::create()->dump(sinkOperator, std::cout);
    // we pass null as the buffer manager as we just want to check if the topology is correct.

    auto typeInferencePhase = TypeInferencePhase::create(std::make_shared<StreamCatalog>());
    auto queryPlan = typeInferencePhase->execute(QueryPlan::create(sinkOperator));
    auto translatePhase = TranslateToGeneratableOperatorPhase::create();
    auto generatableSinkOperator = translatePhase->transform(queryPlan->getRootOperators()[0]);
    ASSERT_TRUE(generatableSinkOperator->instanceOf<GeneratableSinkOperator>());

    auto generatableWindowScanOperator = generatableSinkOperator->getChildren()[0];
    ASSERT_TRUE(generatableWindowScanOperator->instanceOf<GeneratableScanOperator>());

    auto generatableWindowOperator = generatableWindowScanOperator->getChildren()[0];
    ASSERT_TRUE(generatableWindowOperator->instanceOf<GeneratableCompleteWindowOperator>());

    auto generatableSourceOperator = generatableWindowOperator->getChildren()[0];
    ASSERT_TRUE(generatableSourceOperator->instanceOf<GeneratableScanOperator>());
}

}// namespace NES
