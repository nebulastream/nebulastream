//
// Created by kasper on 1/10/23.
//

#include <Operators/LogicalOperators/MapLogicalOperatorNode.hpp>
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
#include <Nodes/Expressions/LogicalExpressions/AndExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/EqualsExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/GreaterEqualsExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/GreaterExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/LessEqualsExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/LessExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/NegateExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/OrExpressionNode.hpp>
#include <Nodes/Node.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Plans/Utils/QueryPlanIterator.hpp>
#include <utility>
namespace NES {
using namespace EndDeviceProtocol;
DefaultPhysicalTypeFactory EndDeviceProtocolSerializationUtil::physicalFactory = DefaultPhysicalTypeFactory();

std::shared_ptr<Message> EndDeviceProtocolSerializationUtil::serializeQueryPlanToEndDevice(NES::QueryPlanPtr QP) {
    auto qpiterator = QueryPlanIterator(std::move(QP));

    //    for (auto op :qpiterator) {
    //        op->
    //    }

    return {};
}

std::string EndDeviceProtocolSerializationUtil::asString(ExpressionInstructions e) {
    auto res = std::string();
    res.push_back(e);
    return res;
}

std::string EndDeviceProtocolSerializationUtil::serializeConstantValue(ExpressionNodePtr node) {
    if (!node->instanceOf<ConstantValueExpressionNode>()) {
        NES_THROW_RUNTIME_ERROR("Incompatible node type");
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
        default: NES_THROW_RUNTIME_ERROR("Incompatible type");
    }

    res += value;
    return res;
}
std::string EndDeviceProtocolSerializationUtil::serializeArithmeticalExpression( ExpressionNodePtr anode) {
    auto bin = anode->as<ArithmeticalBinaryExpressionNode>();
    std::string res = serializeExpression(bin->getLeft()) + serializeExpression(bin->getRight());

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
        NES_THROW_RUNTIME_ERROR("unsupported operator: " + anode->toString());
    }

    return res;
}
std::string EndDeviceProtocolSerializationUtil::serializeLogicalExpression( ExpressionNodePtr lnode) {
    std::string res;
    if (lnode->instanceOf<LogicalBinaryExpressionNode>()) {
        auto bin = lnode->as<LogicalBinaryExpressionNode>();
        res += serializeExpression(bin->getLeft()) + serializeExpression(bin->getRight());
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
            NES_THROW_RUNTIME_ERROR("unsupported operator: " + lnode->toString());
        }
    } else if (lnode->instanceOf<LogicalUnaryExpressionNode>()) {
        auto una = lnode->as<LogicalUnaryExpressionNode>();
        res += serializeExpression(una->child());
        if (una->instanceOf<NegateExpressionNode>()) {
            res.push_back(ExpressionInstructions::NOT);
        }
    }
    return res;
}
std::string EndDeviceProtocolSerializationUtil::serializeFieldAccessExpression(
    ExpressionNodePtr node,
    const std::shared_ptr<std::map<std::string, int8_t>>& registerMap) {

    auto fanode = node->as<FieldAccessExpressionNode>();
    auto name = fanode->getFieldName();
    if (!registerMap->contains(name)) {
        NES_THROW_RUNTIME_ERROR("No register defined for field: " + name);
    }
    auto id = registerMap->at(name);
    std::string res;
    res += asString(ExpressionInstructions::VAR);
    res.push_back(id);
    return res;
}

std::string EndDeviceProtocolSerializationUtil::serializeExpression(ExpressionNodePtr node, const std::shared_ptr<std::map<std::string, int8_t>>& registerMap) {
    //TODO: implement
    if (node->instanceOf<ConstantValueExpressionNode>()) {
        return serializeConstantValue(node->as<ConstantValueExpressionNode>());
    } else if (node->instanceOf<FieldAccessExpressionNode>()){
      return serializeFieldAccessExpression(node, registerMap);
    } else if (node->instanceOf<ArithmeticalExpressionNode>()) {
        return serializeArithmeticalExpression(node);
    } else if (node->instanceOf<LogicalExpressionNode>()) {
        return serializeLogicalExpression(node);
    } else {
        NES_THROW_RUNTIME_ERROR("Unsupported operator");
    }
}
std::string
EndDeviceProtocolSerializationUtil::serializeMapOperator(NodePtr node,
                                                         const std::shared_ptr<std::map<std::string, int8_t>>& registerMap) {
    auto mapnode = node->as<MapLogicalOperatorNode>();
    auto expre = mapnode->getMapExpression();
    auto expression = serializeExpression(expre->,registerMap);

}
}// namespace NES
