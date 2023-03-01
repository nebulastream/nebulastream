//
// Created by kasper on 1/10/23.
//

#include <Common/DataTypes/DataType.hpp>
#include <Common/DataTypes/Float.hpp>
#include <Common/DataTypes/Integer.hpp>
#include <Common/PhysicalTypes/BasicPhysicalType.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
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
#include <Operators/LogicalOperators/Sources/LoRaWANProxySourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/SourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Windowing/WindowLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Windowing/WindowOperatorNode.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Plans/Utils/QueryPlanIterator.hpp>
#include <Sinks/Mediums/SinkMedium.hpp>
#include <Sources/DataSource.hpp>
#include <Sources/LoRaWANProxySource.hpp>
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

DefaultPhysicalTypeFactory EndDeviceProtocolSerializationUtil::physicalFactory = DefaultPhysicalTypeFactory();



std::string getFieldNameFromSchemaName(const std::string& s){
    auto pos = s.find('$');
    return s.substr(pos+1, s.length() - pos-1);
  };

void EndDeviceProtocolSerializationUtil::serializeConstantValue(ExpressionNodePtr node, EDDataVector* expression) {
    if (!node->instanceOf<ConstantValueExpressionNode>()) {
        throw UnsupportedEDSerialisationException();
    }
    auto cnode = node->as<ConstantValueExpressionNode>();
    auto basicValue = std::dynamic_pointer_cast<BasicValue>(cnode->getConstantValue());
    auto physType = physicalFactory.getPhysicalType(basicValue->dataType);
    auto basicPhysType = dynamic_pointer_cast<BasicPhysicalType>(physType);

    expression->Add()->set_instruction(ExpressionInstructions::CONST);

    EDData* data = expression->Add();
    switch (basicPhysType->nativeType) {
        case BasicPhysicalType::UINT_8: data->set__uint8(stoul(basicValue->value)); break;
        case BasicPhysicalType::UINT_16: data->set__uint16(stoul(basicValue->value)); break;
        case BasicPhysicalType::UINT_32: data->set__uint32(stoul(basicValue->value)); break;
        case BasicPhysicalType::UINT_64: data->set__uint64(stoul(basicValue->value)); break;
        case BasicPhysicalType::INT_8: data->set__int8(stoul(basicValue->value)); break;
        case BasicPhysicalType::INT_16: data->set__int16(stoul(basicValue->value)); break;
        case BasicPhysicalType::INT_32: data->set__int8(stoi(basicValue->value)); break;
        case BasicPhysicalType::INT_64: data->set__int64(stol(basicValue->value)); break;
        case BasicPhysicalType::FLOAT: data->set__float(std::stof(basicValue->value)); break;
        case BasicPhysicalType::DOUBLE: data->set__double(std::stod(basicValue->value)); break;
        default: throw UnsupportedEDSerialisationException();
    }

}
void EndDeviceProtocolSerializationUtil::serializeArithmeticalExpression(ExpressionNodePtr anode,
                                                                         EDRegistersRef registers, EDDataVector* expression) {
    auto bin = anode->as<ArithmeticalBinaryExpressionNode>();
    serializeExpression(bin->getLeft(), registers, expression);
    serializeExpression(bin->getRight(), registers,  expression);

    ExpressionInstructions instr;
    if (bin->instanceOf<AddExpressionNode>()) {
        instr = ExpressionInstructions::ADD;
    } else if (bin->instanceOf<SubExpressionNode>()) {
        instr = ExpressionInstructions::SUB;
    } else if (bin->instanceOf<MulExpressionNode>()) {
        instr = ExpressionInstructions::MUL;
    } else if (bin->instanceOf<DivExpressionNode>()) {
        instr = ExpressionInstructions::DIV;
    } else if (bin->instanceOf<ModExpressionNode>()) {
        instr = ExpressionInstructions::MOD;
    } else {
        throw UnsupportedEDSerialisationException();
    }
    expression->Add()->set_instruction(instr);
}

void EndDeviceProtocolSerializationUtil::serializeLogicalExpression(ExpressionNodePtr lnode, EDRegistersRef registers, EDDataVector* expression) {

    ExpressionInstructions instr;
    if (lnode->instanceOf<LogicalBinaryExpressionNode>()) {
        auto bin = lnode->as<LogicalBinaryExpressionNode>();
        serializeExpression(bin->getLeft(), registers, expression);
        serializeExpression(bin->getRight(), registers, expression);

        if (bin->instanceOf<AndExpressionNode>()) {
            instr = ExpressionInstructions::AND;
        } else if (bin->instanceOf<OrExpressionNode>()) {
            instr = ExpressionInstructions::OR;
        } else if (bin->instanceOf<EqualsExpressionNode>()) {
            instr = ExpressionInstructions::EQ;
        } else if (bin->instanceOf<LessExpressionNode>()) {
            instr = ExpressionInstructions::LT;
        } else if (bin->instanceOf<GreaterExpressionNode>()) {
            instr = ExpressionInstructions::GT;
        } else {
            throw UnsupportedEDSerialisationException();
        }
        expression->Add()->set_instruction(instr);
    } else if (lnode->instanceOf<LogicalUnaryExpressionNode>()) {
        auto una = lnode->as<LogicalUnaryExpressionNode>();
        serializeExpression(una->child(), registers, expression);
        if (una->instanceOf<NegateExpressionNode>()) {
            expression->Add()->set_instruction(ExpressionInstructions::NOT);
        } else {
            throw UnsupportedEDSerialisationException();
        }
    }

}
void EndDeviceProtocolSerializationUtil::serializeFieldAccessExpression(ExpressionNodePtr node, EDRegistersRef registers, EDDataVector* expression) {

    auto fanode = node->as<FieldAccessExpressionNode>();
    auto fullName = fanode->getFieldName();
    auto name = getFieldNameFromSchemaName(fullName);
    if (std::find(registers.begin(), registers.end(), name) == registers.end()) {
        NES_WARNING("No register defined for field: " + name);
        throw UnsupportedEDSerialisationException();
    }

    auto id = (int) std::distance(registers.begin(), std::find(registers.begin(), registers.end(), name));
    expression->Add()->set_instruction(ExpressionInstructions::VAR);

    expression->Add()->set__uint8(id);

}
void EndDeviceProtocolSerializationUtil::serializeExpression(ExpressionNodePtr node, EDRegistersRef registers, EDDataVector* expression) {
    if (node->instanceOf<ConstantValueExpressionNode>()) {
        serializeConstantValue(node->as<ConstantValueExpressionNode>(), expression);
    } else if (node->instanceOf<FieldAccessExpressionNode>()) {
        serializeFieldAccessExpression(node, registers, expression);
    } else if (node->instanceOf<ArithmeticalExpressionNode>()) {
        serializeArithmeticalExpression(node, registers, expression);
    } else if (node->instanceOf<LogicalExpressionNode>()) {
        serializeLogicalExpression(node, registers, expression);
    } else {
        throw UnsupportedEDSerialisationException();
    }
}
void EndDeviceProtocolSerializationUtil::serializeMapOperator(NodePtr node, EDRegistersRef registers, EDMapOperation* edMapOperation) {
    auto mapNode = node->as<MapLogicalOperatorNode>();
    auto expressionNode = mapNode->getMapExpression();
    auto schemaFieldName = expressionNode->getField()->getFieldName();
    auto mapexpr = expressionNode->getAssignment();

    serializeExpression(mapexpr, registers, edMapOperation->mutable_function());
    int attribute;

    //if field doesn't already exists, then add it
    if (std::find(registers.begin(), registers.end(), schemaFieldName) == registers.end()) {
        attribute = registers.size();
        registers.push_back(getFieldNameFromSchemaName(schemaFieldName));
    } else {
        attribute = std::distance(registers.begin(), std::find(registers.begin(), registers.end(), schemaFieldName));
    }
    edMapOperation->set_attribute(attribute);
}

void EndDeviceProtocolSerializationUtil::serializeFilterOperator(NodePtr node, EDRegistersRef registers, EDFilterOperation* filterOperation) {
    auto filterNode = node->as<FilterLogicalOperatorNode>();
    auto expressionNode = filterNode->getPredicate();
    serializeLogicalExpression(expressionNode, registers, filterOperation->mutable_predicate());

}
void EndDeviceProtocolSerializationUtil::serializeWindowOperator(NodePtr node, EDRegistersRef __attribute__((unused)) registers, EDWindowOperation*  __attribute__((unused)) windowOperation) {
    NES_NOT_IMPLEMENTED();
    //TODO: NES only supports time-based windows while ED atm only supports count-based. Need to add time-based support
    auto windowNode = node->as<WindowOperatorNode>();
    auto definition = windowNode->getWindowDefinition();

    auto windowType = definition->getWindowType();
    if (!windowType->isTumblingWindow()) {
        NES_WARNING("Window type not supported. Only tumbling windows are supported currently");
        throw UnsupportedEDSerialisationException();
    }

    auto aggregations = definition->getWindowAggregation();
    if (aggregations.size() > 1) {
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
        default:
            NES_WARNING("Window Aggregation type not supported");
            throw UnsupportedEDSerialisationException();
            break;
    }

};

void EndDeviceProtocolSerializationUtil::serializeOperator(NodePtr node, EDRegistersRef registers, EDOperation* serialized_Operation) {
    EDOperation operation;
    if (node->instanceOf<MapLogicalOperatorNode>()) {
        auto map = serialized_Operation->mutable_map();
        serializeMapOperator(node, registers, map);
    }
    if (node->instanceOf<FilterLogicalOperatorNode>()) {
        auto filter = serialized_Operation->mutable_filter();
        serializeFilterOperator(node, registers, filter);

    }
    if (node->instanceOf<WindowLogicalOperatorNode>()) {
        auto window = serialized_Operation->mutable_window();
        serializeWindowOperator(node, registers, window);
    } else {
        throw UnsupportedEDSerialisationException();
    }
}

EndDeviceProtocolSerializationUtil::EDQueryPtr
EndDeviceProtocolSerializationUtil::serializeQueryPlanToEndDevice(NES::QueryPlanPtr QP, LoRaWANProxySourceTypePtr st) {
    //make sure we only have a single LoRaWANProxySource
    auto sourceOperators = QP->getSourceOperators();
//    auto st = sourceOperators.at(0)->toString();
    if (sourceOperators.size() != 1) {
        NES_THROW_RUNTIME_ERROR("Trying to serialize query with incompatible sources");
    }
//    // fetch the sourceDescriptor and the sensor_fields
    auto sourceDescriptor = sourceOperators.at(0)->getSourceDescriptor();
//    auto sensorFields = std::make_shared<std::vector<std::string>>(sourceDescriptor->getSourceConfig()->getSensorFields()->getValue());
    auto sensorFields = std::vector<std::string>(st->getSensorFields()->getValue());

    // get operators as list so we can travel to it in reverse
    //i.e. from sourceDescriptor to sink
    auto qpiterator = QueryPlanIterator(std::move(QP));
    auto operatorNodeList = std::deque<NodePtr>();
    for (auto opNode : qpiterator) {
        operatorNodeList.push_back(opNode);
    }

    auto query = std::make_shared<EndDeviceProtocol::Query>();
    for (auto nodeIter = operatorNodeList.rbegin(); nodeIter < operatorNodeList.rend(); ++nodeIter) {
        auto node = nodeIter->get();
        //right now we only support the nodes below
        if (!(node->instanceOf<WindowOperatorNode>() || node->instanceOf<FilterLogicalOperatorNode>()
              || node->instanceOf<MapLogicalOperatorNode>())) {
            //not an operator we support
            continue;
        }
        auto opNode = node->as<LogicalUnaryOperatorNode>();
        try {
            auto op = query->mutable_operations()->Add();
             serializeOperator(opNode, sensorFields, op);


            sourceDescriptor->setSchema(opNode->getOutputSchema());
            opNode->removeAndJoinParentAndChildren();
        } catch (UnsupportedEDSerialisationException& e) {
            NES_DEBUG("unsupported Operation for serialization");
            break;
        }
    }
    return query;
}
}// namespace NES
