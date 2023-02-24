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



std::string getFieldNameFromSchemaName(std::string s){
    auto pos = s.find('$');
    return s.substr(pos+1, s.length() - pos-1);
  };

std::string EndDeviceProtocolSerializationUtil::serializeConstantValue(ExpressionNodePtr node) {
    if (!node->instanceOf<ConstantValueExpressionNode>()) {
        throw UnsupportedEDSerialisationException();
    }
    auto cnode = node->as<ConstantValueExpressionNode>();
    auto basicValue = std::dynamic_pointer_cast<BasicValue>(cnode->getConstantValue());
    auto physType = physicalFactory.getPhysicalType(basicValue->dataType);
    auto basicPhysType = dynamic_pointer_cast<BasicPhysicalType>(physType);

    std::string res = asString(ExpressionInstructions::CONST);

    EDData data;
    switch (basicPhysType->nativeType) {
        case BasicPhysicalType::UINT_8:
        case BasicPhysicalType::UINT_16:
        case BasicPhysicalType::UINT_32:
            data.set__uint8_32(stoul(basicValue->value));
             break;
        case BasicPhysicalType::UINT_64: data.set__uint64(stoul(basicValue->value)); break;
        case BasicPhysicalType::INT_8:
        case BasicPhysicalType::INT_16:
        case BasicPhysicalType::INT_32: data.set__int8_32(stoi(basicValue->value)); break;
        case BasicPhysicalType::INT_64: data.set__int64(stol(basicValue->value)); break;
        case BasicPhysicalType::FLOAT: data.set__float(std::stof(basicValue->value)); break;
        case BasicPhysicalType::DOUBLE: data.set__double(std::stod(basicValue->value)); break;
        default: throw UnsupportedEDSerialisationException();
    }

    res+=data.SerializeAsString();
    return res;
}
std::string EndDeviceProtocolSerializationUtil::serializeArithmeticalExpression(ExpressionNodePtr anode,
                                                                                EDRegistersPtr registers) {
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
std::string EndDeviceProtocolSerializationUtil::serializeLogicalExpression(ExpressionNodePtr lnode, EDRegistersPtr registers) {
    std::string res;
    if (lnode->instanceOf<LogicalBinaryExpressionNode>()) {
        auto bin = lnode->as<LogicalBinaryExpressionNode>();
        res += serializeExpression(bin->getLeft(), registers) + serializeExpression(bin->getRight(), registers);
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
EndDeviceProtocolSerializationUtil::EDDataVector EndDeviceProtocolSerializationUtil::serializeFieldAccessExpression(ExpressionNodePtr node, EDRegistersPtr registers) {

    auto fanode = node->as<FieldAccessExpressionNode>();
    auto fullName = fanode->getFieldName();
    auto name = getFieldNameFromSchemaName(fullName);
    if (std::find(registers->begin(), registers->end(), name) == registers->end()) {
        NES_WARNING("No register defined for field: " + name);
        throw UnsupportedEDSerialisationException();
    }

    auto id = std::distance(registers->begin(), std::find(registers->begin(), registers->end(), name));
    EDDataVector res;
    EDData instr;
    instr.set_instruction(ExpressionInstructions::VAR);
    res.push_back(instr);

    EDData number;
    number.set__int8_32(id);
    res.push_back(number);
    return res;
}
EndDeviceProtocolSerializationUtil::EDDataVector EndDeviceProtocolSerializationUtil::serializeExpression(ExpressionNodePtr node, EDRegistersPtr registers) {
    if (node->instanceOf<ConstantValueExpressionNode>()) {
        return serializeConstantValue(node->as<ConstantValueExpressionNode>());
    } else if (node->instanceOf<FieldAccessExpressionNode>()) {
        return serializeFieldAccessExpression(node, registers);
    } else if (node->instanceOf<ArithmeticalExpressionNode>()) {
        return serializeArithmeticalExpression(node, registers);
    } else if (node->instanceOf<LogicalExpressionNode>()) {
        return serializeLogicalExpression(node, registers);
    } else {
        throw UnsupportedEDSerialisationException();
    }
}
void EndDeviceProtocolSerializationUtil::serializeMapOperator(NodePtr node, EDRegistersPtr registers, EDMapOperation* edMapOperation) {
    auto mapNode = node->as<MapLogicalOperatorNode>();
    auto expressionNode = mapNode->getMapExpression();
    auto schemaFieldName = expressionNode->getField()->getFieldName();
    auto mapexpr = expressionNode->getAssignment();
    auto expression = serializeExpression(mapexpr, registers);
    int attribute;

    //if field doesn't already exists, then add it
    if (std::find(registers->begin(), registers->end(), schemaFieldName) == registers->end()) {
        attribute = registers->size();
        registers->push_back(getFieldNameFromSchemaName(schemaFieldName));
    } else {
        attribute = std::distance(registers->begin(), std::find(registers->begin(), registers->end(), schemaFieldName));
    }
    edMapOperation->set_attribute(attribute);
    edMapOperation->mutable_function()->Assign(expression.begin(), expression.end());
}

void EndDeviceProtocolSerializationUtil::serializeFilterOperator(NodePtr node, EDRegistersPtr registers, EDFilterOperation* filterOperation) {
    auto filterNode = node->as<FilterLogicalOperatorNode>();
    auto expressionNode = filterNode->getPredicate();
    auto data = serializeLogicalExpression(expressionNode, std::move(registers));

    filterOperation->mutable_predicate()->Assign(data.begin(), data.end());
}
void EndDeviceProtocolSerializationUtil::serializeWindowOperator(NodePtr node, EDRegistersPtr __attribute__((unused)) registers, EDWindowOperation* windowOperation) {
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

    return std::make_shared<EndDeviceProtocol::WindowOperation>();
};

void EndDeviceProtocolSerializationUtil::serializeOperator(NodePtr node, EDRegistersPtr registers, EDOperation* serialized_Operation) {
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
    auto sensorFields = std::make_shared<std::vector<std::string>>(st->getSensorFields()->getValue());

    // get operators as list so we can travel to it in reverse
    //i.e. from sourceDescriptor to sink
    auto qpiterator = QueryPlanIterator(std::move(QP));
    auto operatorNodeList = std::deque<NodePtr>();
    for (auto opNode : qpiterator) {
        operatorNodeList.push_back(opNode);
    }

    EDQuery query = EndDeviceProtocol::Query();
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
            auto op = *query.mutable_operations()->Add();
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
