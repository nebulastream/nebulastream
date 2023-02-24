//
// Created by kasper on 1/10/23.
//

#ifndef NES_ENDDEVICEPROTOCOLSERIALIZATIONUTIL_HPP
#define NES_ENDDEVICEPROTOCOLSERIALIZATIONUTIL_HPP

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
#include <Catalogs/Source/PhysicalSourceTypes/LoRaWANProxySourceType.hpp>
#include <Nodes/Node.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Plans/Utils/QueryPlanIterator.hpp>
#include <utility>
namespace NES {
class EndDeviceProtocolSerializationUtil {
  public:
    class UnsupportedEDSerialisationException : public std::runtime_error {
      public:
        UnsupportedEDSerialisationException()
            : std::runtime_error("Can't serialize this node, since the ED does not support it."){};
    };
    using EDRegistersPtr = std::shared_ptr<std::vector<std::string>>;
    using EDMapOperation = EndDeviceProtocol::MapOperation;
    using EDFilterOperation = EndDeviceProtocol::FilterOperation;
    using EDWindowOperation = EndDeviceProtocol::WindowOperation;
    using EDOperation = EndDeviceProtocol::Operation;
    using EDQuery = EndDeviceProtocol::Query;
    using EDData = EndDeviceProtocol::Data;
    using EDDataVector = std::vector<EDData>;
    [[nodiscard]] static EDQuery serializeQueryPlanToEndDevice(QueryPlanPtr QP, LoRaWANProxySourceTypePtr st);

    [[nodiscard]] static EDDataVector serializeConstantValue(ExpressionNodePtr cnode);

    [[nodiscard]] static EDDataVector serializeArithmeticalExpression(ExpressionNodePtr, EDRegistersPtr);
    [[nodiscard]] static EDDataVector serializeLogicalExpression(ExpressionNodePtr, EDRegistersPtr);
    [[nodiscard]] static EDDataVector serializeFieldAccessExpression(ExpressionNodePtr, EDRegistersPtr);
    [[nodiscard]] static EDDataVector serializeExpression(ExpressionNodePtr, EDRegistersPtr);
    static void serializeMapOperator(NodePtr, EDRegistersPtr, EDMapOperation*);
    static void serializeFilterOperator(NodePtr, EDRegistersPtr, EDFilterOperation*);
    static void serializeWindowOperator(NodePtr, EDRegistersPtr, EDWindowOperation*);
    static void serializeOperator(NodePtr, EDRegistersPtr, EDOperation*);
    [[nodiscard]] static std::string asString(EndDeviceProtocol::ExpressionInstructions);

  private:
    static DefaultPhysicalTypeFactory physicalFactory;
};
}// namespace NES

#endif//NES_ENDDEVICEPROTOCOLSERIALIZATIONUTIL_HPP
