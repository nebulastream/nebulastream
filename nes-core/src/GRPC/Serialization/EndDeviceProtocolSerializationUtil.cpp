//
// Created by kasper on 1/10/23.
//

#include <Sources/LoRaWANProxySource.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Common/DataTypes/Float.hpp>
#include <Common/DataTypes/Integer.hpp>
#include <Common/PhysicalTypes/BasicPhysicalType.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/SourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/LoRaWANProxySourceDescriptor.hpp>
#include <Common/ValueTypes/BasicValue.hpp>
#include <EndDeviceProtocol.pb.h>
#include <GRPC/Serialization/EndDeviceProtocolSerializationUtil.hpp>
#include <Nodes/Expressions/ArithmeticalExpressions/AbsExpressionNode.hpp>
#include <Nodes/Expressions/ArithmeticalExpressions/AddExpressionNode.hpp>
#include <Nodes/Expressions/ArithmeticalExpressions/ArithmeticalBinaryExpressionNode.hpp>
#include <Nodes/Expressions/ArithmeticalExpressions/ArithmeticalExpressionNode.hpp>
#include <Nodes/Expressions/ArithmeticalExpressions/CeilExpressionNode.hpp>
#include <Nodes/Expressions/ArithmeticalExpressions/DivExpressionNode.hpp>
#include <Nodes/Expressions/ArithmeticalExpressions/ExpExpressionNode.hpp>
#include <Nodes/Expressions/ArithmeticalExpressions/FloorExpressionNode.hpp>
#include <Nodes/Expressions/ArithmeticalExpressions/Log10ExpressionNode.hpp>
#include <Nodes/Expressions/ArithmeticalExpressions/LogExpressionNode.hpp>
#include <Nodes/Expressions/ArithmeticalExpressions/ModExpressionNode.hpp>
#include <Nodes/Expressions/ArithmeticalExpressions/MulExpressionNode.hpp>
#include <Nodes/Expressions/ArithmeticalExpressions/PowExpressionNode.hpp>
#include <Nodes/Expressions/ArithmeticalExpressions/RoundExpressionNode.hpp>
#include <Nodes/Expressions/ArithmeticalExpressions/SqrtExpressionNode.hpp>
#include <Nodes/Expressions/ArithmeticalExpressions/SubExpressionNode.hpp>
#include <Nodes/Expressions/BinaryExpressionNode.hpp>
#include <Nodes/Expressions/CaseExpressionNode.hpp>
#include <Nodes/Expressions/ConstantValueExpressionNode.hpp>
#include <Nodes/Expressions/ExpressionNode.hpp>
#include <Nodes/Expressions/FieldAccessExpressionNode.hpp>
#include <Nodes/Expressions/FieldAssignmentExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/AndExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/EqualsExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/GreaterEqualsExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/GreaterExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/LessEqualsExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/LessExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/NegateExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/OrExpressionNode.hpp>
#include <Nodes/Node.hpp>
#include <Operators/LogicalOperators/FilterLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/MapLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Windowing/WindowLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Windowing/WindowOperatorNode.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Plans/Utils/QueryPlanIterator.hpp>
#include <Sources/DataSource.hpp>
#include <Windowing/LogicalWindowDefinition.hpp>
#include <Windowing/WindowAggregations/WindowAggregationDescriptor.hpp>
#include <Windowing/WindowTypes/TimeBasedWindowType.hpp>
#include <Windowing/WindowTypes/TumblingWindow.hpp>
#include <Windowing/WindowTypes/WindowType.hpp>
#include <algorithm>
#include <stdexcept>
#include <utility>
namespace NES {
using namespace EndDeviceProtocol;
class UnsupportedEDSerialisationException: public std::runtime_error {
  public:
    UnsupportedEDSerialisationException(): std::runtime_error("Can't serialize this node, since the ED does not support it.") {};
};

DefaultPhysicalTypeFactory EndDeviceProtocolSerializationUtil::physicalFactory = DefaultPhysicalTypeFactory();

std::shared_ptr<EndDeviceProtocol::Query> EndDeviceProtocolSerializationUtil::serializeQueryPlanToEndDevice(NES::QueryPlanPtr QP) {
    //make sure we only have a single LoRaWANProxySource
    auto sourceOperators = QP->getSourceOperators();
    if (sourceOperators.size() != 1 && !sourceOperators.at(0)->instanceOf<LoRaWANProxySourceDescriptor>()){
        NES_THROW_RUNTIME_ERROR("Trying to serialize query with incompatible sources");
    }
    auto source = sourceOperators.at(0)->getSourceDescriptor()->as<LoRaWANProxySourceDescriptor>();
    auto sensorFields = source->getSourceConfig()->getSensorFields();

    auto qpiterator = QueryPlanIterator(std::move(QP));
    auto operatorNodeList = std::deque<NodePtr>();
    for (auto opNode :qpiterator) {
        operatorNodeList.push_back(opNode);
    }



    EndDeviceProtocol::Query_Operation operations;
    for (auto nodeIter = operatorNodeList.rbegin(); nodeIter < operatorNodeList.rend(); ++nodeIter) {
        auto node = nodeIter->get();
        node->remove()
        try {
            if (!node->instanceOf<ExpressionNode>()){
                //not an expression
                continue ;
            }
            auto exprNode = node->as<ExpressionNode>();
            serializeExpression(exprNode);
        } catch (UnsupportedEDSerialisationException& e){

        }
    }

}

std::string EndDeviceProtocolSerializationUtil::asString(ExpressionInstructions e) {
    auto res = std::string();
    res.push_back(e);
    return res;
}

std::string EndDeviceProtocolSerializationUtil::serializeConstantValue(ExpressionNodePtr node) {
    if (!node->instanceOf<ConstantValueExpressionNode>()) {
        throw UnsupportedEDSerialisationException();
    }
    auto cnode = node->as<ConstantValueExpressionNode>();
    auto basicValue = std::dynamic_pointer_cast<BasicValue>(cnode->getConstantValue());
    auto physType = physicalFactory.getPhysicalType(basicValue->dataType);
    auto basicPhysType = dynamic_pointer_cast<BasicPhysicalType>(physType);

    std::string res = asString(ExpressionInstructions::CONST);
    auto value = basicValue->value;

    switch (basicPhysType->nativeType) {
        case BasicPhysicalType::INT_8: res.push_back(DataTypes::INT8); break;
        case BasicPhysicalType::UINT_8: res.push_back(DataTypes::UINT8); break;
        case BasicPhysicalType::INT_16: res.push_back(DataTypes::INT16); break;
        case BasicPhysicalType::UINT_16: res.push_back(DataTypes::UINT16); break;
        case BasicPhysicalType::INT_32: res.push_back(DataTypes::INT32); break;
        case BasicPhysicalType::UINT_32: res.push_back(DataTypes::UINT32); break;
        case BasicPhysicalType::INT_64: res.push_back(DataTypes::INT64); break;
        case BasicPhysicalType::UINT_64: res.push_back(DataTypes::UINT64); break;
        case BasicPhysicalType::FLOAT: res.push_back(DataTypes::FLOAT); break;
        case BasicPhysicalType::DOUBLE: res.push_back(DataTypes::DOUBLE); break;
        default: throw UnsupportedEDSerialisationException();
    }

    res += value;
    return res;
}
std::string EndDeviceProtocolSerializationUtil::serializeArithmeticalExpression( ExpressionNodePtr anode, std::shared_ptr<std::vector<std::string>> registers) {
    auto bin = anode->as<ArithmeticalBinaryExpressionNode>();
    std::string res = serializeExpression(bin->getLeft(), registers) + serializeExpression(bin->getRight(), registers);

    if (bin->instanceOf<AddExpressionNode>()) {
        res.push_back(ExpressionInstructions::ADD);
    } else if (bin->instanceOf<SubExpressionNode>()) {
        res.push_back(ExpressionInstructions::SUB);
    } else if (bin->instanceOf<MulExpressionNode>()) {
        res.push_back(ExpressionInstructions::MUL);
    } else if (bin->instanceOf<DivExpressionNode>()) {
        res.push_back(ExpressionInstructions::DIV);
    } else if (bin->instanceOf<ModExpressionNode>()) {
        res.push_back(ExpressionInstructions::MOD);
    } else {
        throw UnsupportedEDSerialisationException();
    }

    return res;
}
std::string EndDeviceProtocolSerializationUtil::serializeLogicalExpression( ExpressionNodePtr lnode, std::shared_ptr<std::vector<std::string>> registers) {
    std::string res;
    if (lnode->instanceOf<LogicalBinaryExpressionNode>()) {
        auto bin = lnode->as<LogicalBinaryExpressionNode>();
        res += serializeExpression(bin->getLeft(),registers) + serializeExpression(bin->getRight(),registers);
        if (bin->instanceOf<AndExpressionNode>()) {
            res.push_back(ExpressionInstructions::AND);
        } else if (bin->instanceOf<OrExpressionNode>()) {
            res.push_back(ExpressionInstructions::OR);
        } else if (bin->instanceOf<EqualsExpressionNode>()) {
            res.push_back(ExpressionInstructions::EQ);
        } else if (bin->instanceOf<LessExpressionNode>()) {
            res.push_back(ExpressionInstructions::LT);
        } else if (bin->instanceOf<GreaterExpressionNode>()) {
            res.push_back(ExpressionInstructions::GT);
        } else {
            throw UnsupportedEDSerialisationException();
        }
    } else if (lnode->instanceOf<LogicalUnaryExpressionNode>()) {
        auto una = lnode->as<LogicalUnaryExpressionNode>();
        res += serializeExpression(una->child(), registers);
        if (una->instanceOf<NegateExpressionNode>()) {
            res.push_back(ExpressionInstructions::NOT);
        }
    }
    return res;
}
std::string EndDeviceProtocolSerializationUtil::serializeFieldAccessExpression(
    ExpressionNodePtr node,
    std::shared_ptr<std::vector<std::string>> registers) {

    auto fanode = node->as<FieldAccessExpressionNode>();
    auto name = fanode->getFieldName();
    if (std::find(registers->begin(), registers->end(), name) == registers->end()) {
        NES_WARNING("No register defined for field: " + name);
        throw UnsupportedEDSerialisationException();
    }
    //TODO: somehow fix this. This is real ugly.
    auto id = std::distance(registers->begin(),std::find(registers->begin(), registers->end(), name));
    std::string res;
    res += asString(ExpressionInstructions::VAR);
    res.push_back(id);
    return res;
}

std::string EndDeviceProtocolSerializationUtil::serializeExpression(ExpressionNodePtr node, std::shared_ptr<std::vector<std::string>> registers) {
    //TODO: implement
    if (node->instanceOf<ConstantValueExpressionNode>()) {
        return serializeConstantValue(node->as<ConstantValueExpressionNode>());
    } else if (node->instanceOf<FieldAccessExpressionNode>()){
      return serializeFieldAccessExpression(node, registers);
    } else if (node->instanceOf<ArithmeticalExpressionNode>()) {
        return serializeArithmeticalExpression(node, registers);
    } else if (node->instanceOf<LogicalExpressionNode>()) {
        return serializeLogicalExpression(node, registers);
    } else {
        throw UnsupportedEDSerialisationException();
    }
}
MapOperation
EndDeviceProtocolSerializationUtil::serializeMapOperator(NodePtr node,
                                                         std::shared_ptr<std::vector<std::string>> registers) {
    auto mapNode = node->as<MapLogicalOperatorNode>();
    auto expressionNode = mapNode->getMapExpression();
    auto fieldexprname = expressionNode->getField()->getFieldName();
    auto mapexpr = expressionNode->getAssignment();
    auto expression = serializeExpression(mapexpr, registers);
    int attribute;

    //if field doesnt already exists, then add it
    if (std::find(registers->begin(),registers->end(),fieldexprname) == registers->end()){
        attribute = registers->size();
        registers->push_back(fieldexprname);
    } else {
        attribute = std::distance(registers->begin(), std::find(registers->begin(), registers->end(), fieldexprname));

    }
    auto result = MapOperation();
    result.set_attribute(attribute);
    result.mutable_function()->set_instructions(expression);
    return result;
}

FilterOperation EndDeviceProtocolSerializationUtil::serializeFilterOperator(NodePtr node, std::shared_ptr<std::vector<std::string>> registers) {
    auto filterNode = node->as<FilterLogicalOperatorNode>();
    auto expressionNode = filterNode->getPredicate();
    auto ser_expr = serializeLogicalExpression(expressionNode, registers);

    auto result = FilterOperation();
    result.mutable_predicate()->set_instructions(ser_expr);
    return result;

}
EndDeviceProtocol::WindowOperation
EndDeviceProtocolSerializationUtil::serializeWindowOperator(NodePtr node, std::shared_ptr<std::vector<std::string>> __attribute__((unused))registers ) {
    //TODO: NES only supports time-based windows while ED atm only supports count-based. Need to add time-based support
    auto windowNode = node->as<WindowOperatorNode>();
    auto definition = windowNode->getWindowDefinition();

    auto windowType = definition->getWindowType();
    if (!windowType->isTumblingWindow()){
        NES_WARNING("Window type not supported. Only tumbling windows are supported currently");
        throw UnsupportedEDSerialisationException();
    }

    auto aggregations = definition->getWindowAggregation();
    if (aggregations.size() > 1){
        NES_WARNING("Multiple aggregations not supported here");
        throw UnsupportedEDSerialisationException();
    }
    auto aggDescriptor = aggregations.at(0);
    WindowAggregationType winAggType;
    switch (aggDescriptor->getType()) {
        case Windowing::WindowAggregationDescriptor::Avg: winAggType = WindowAggregationType::AVG; break;
        case Windowing::WindowAggregationDescriptor::Count: winAggType = WindowAggregationType::COUNT; break;
        case Windowing::WindowAggregationDescriptor::Max: winAggType = WindowAggregationType::MAX; break;
        case Windowing::WindowAggregationDescriptor::Min: winAggType = WindowAggregationType::MIN; break;
        case Windowing::WindowAggregationDescriptor::Sum: winAggType = WindowAggregationType::SUM; break;
        default: NES_WARNING("Window Aggregation type not supported"); throw UnsupportedEDSerialisationException(); break;
    }

    return EndDeviceProtocol::WindowOperation();
};
}// namespace NES
