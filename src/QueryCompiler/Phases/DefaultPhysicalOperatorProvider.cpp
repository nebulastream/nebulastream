#include <Operators/AbstractOperators/Arity/BinaryOperatorNode.hpp>
#include <Operators/AbstractOperators/Arity/UnaryOperatorNode.hpp>
#include <Operators/LogicalOperators/FilterLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/JoinLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/UnionLogicalOperatorNode.hpp>
#include <Operators/OperatorForwardDeclaration.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalDemultiplexOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalFilterOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalMultiplexOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalSinkOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalSourceOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalJ>
#include <QueryCompiler/Phases/DefaultPhysicalOperatorProvider.hpp>
namespace NES {
namespace QueryCompilation {

PhysicalOperatorProviderPtr DefaultPhysicalOperatorProvider::create() {
    return std::make_shared<DefaultPhysicalOperatorProvider>();
}

bool DefaultPhysicalOperatorProvider::isDemulticast(LogicalOperatorNodePtr operatorNode) {
    return operatorNode->getParents().size() > 1;
}

void DefaultPhysicalOperatorProvider::ingestDemulticastOperatorsBefore(LogicalOperatorNodePtr operatorNode) {
    auto demultiplexOperator = PhysicalOperators::PhysicalDemultiplexOperator::create();
    demultiplexOperator->setOutputSchema(operatorNode->getOutputSchema());
    operatorNode->insertBetweenThisAndParentNodes(demultiplexOperator);
}

void DefaultPhysicalOperatorProvider::ingestMulticastOperatorsAfter(LogicalOperatorNodePtr operatorNode) {
    auto multiplexOperator = PhysicalOperators::PhysicalMultiplexOperator::create();
    multiplexOperator->setOutputSchema(operatorNode->getOutputSchema());
    operatorNode->insertBetweenThisAndChildNodes(multiplexOperator);
}

void DefaultPhysicalOperatorProvider::lower(QueryPlanPtr queryPlan, LogicalOperatorNodePtr operatorNode) {
    if (isDemulticast(operatorNode)) {
        ingestDemulticastOperatorsBefore(operatorNode);
    }

    if (operatorNode->isUnaryOperator()) {
        lowerUnaryOperator(queryPlan, operatorNode);
    } else if (operatorNode->isBinaryOperator()) {
        lowerBinaryOperator(queryPlan, operatorNode);
    }
}

void DefaultPhysicalOperatorProvider::lowerUnaryOperator(QueryPlanPtr queryPlan, LogicalOperatorNodePtr operatorNode) {

    // If a unary operator has more then one parent, we introduce a implicit multiplex operator before.
    if (operatorNode->getChildren().size() > 1) {
        ingestMulticastOperatorsAfter(operatorNode);
    }

    NES_ASSERT(operatorNode->getParents().size() <= 1, "A unary operator should only have at most one parent.");

    if (operatorNode->instanceOf<SourceLogicalOperatorNode>()) {
        auto logicalSourceOperator = operatorNode->as<SourceLogicalOperatorNode>();
        auto physicalSourceOperator =
            PhysicalOperators::PhysicalSourceOperator::create(logicalSourceOperator->getSourceDescriptor());
        operatorNode->replace(physicalSourceOperator);
    } else if (operatorNode->instanceOf<SinkLogicalOperatorNode>()) {
        auto logicalSinkOperator = operatorNode->as<SinkLogicalOperatorNode>();
        auto physicalSinkOperator = PhysicalOperators::PhysicalSinkOperator::create(logicalSinkOperator->getSinkDescriptor());
        operatorNode->replace(physicalSinkOperator);
        queryPlan->replaceRootOperator(logicalSinkOperator, physicalSinkOperator);
    } else if (operatorNode->instanceOf<FilterLogicalOperatorNode>()) {
        auto filterOperator = operatorNode->as<FilterLogicalOperatorNode>();
        auto physicalFilterOperator = PhysicalOperators::PhysicalFilterOperator::create(filterOperator->getPredicate());
        operatorNode->replace(physicalFilterOperator);
    } else {
        NES_ERROR("No conversion for operator " + operatorNode->toString() + " was provided.");
    }
}

void DefaultPhysicalOperatorProvider::lowerBinaryOperator(QueryPlanPtr queryPlan, LogicalOperatorNodePtr operatorNode) {
    NES_ERROR("No conversion for operator " + operatorNode->toString() + " was provided.");

    if (operatorNode->instanceOf<UnionLogicalOperatorNode>()) {
        lowerUnionOperator(queryPlan, operatorNode);
    } else if(operatorNode->instanceOf<JoinLogicalOperatorNode>()){
        lowerJoinOperator(queryPlan, operatorNode);
    }
}

void DefaultPhysicalOperatorProvider::lowerUnionOperator(QueryPlanPtr, LogicalOperatorNodePtr operatorNode) {
    auto unaryOperator = operatorNode->as<UnionLogicalOperatorNode>();
    auto physicalMultiplexOperator = PhysicalOperators::PhysicalMultiplexOperator::create();
    operatorNode->replace(physicalMultiplexOperator);
}

void DefaultPhysicalOperatorProvider::lowerJoinOperator(QueryPlanPtr queryPlan, LogicalOperatorNodePtr operatorNode) {
    auto joinOperator = operatorNode->as<JoinLogicalOperatorNode>();
    auto physicalMultiplexOperator = PhysicalOperators::PhysicalMultiplexOperator::create();
    operatorNode->replace(physicalMultiplexOperator);
}

}// namespace QueryCompilation
}// namespace NES