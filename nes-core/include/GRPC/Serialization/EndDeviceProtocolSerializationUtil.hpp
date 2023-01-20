//
// Created by kasper on 1/10/23.
//

#ifndef NES_ENDDEVICEPROTOCOLSERIALIZATIONUTIL_HPP
#define NES_ENDDEVICEPROTOCOLSERIALIZATIONUTIL_HPP

#include <EndDeviceProtocol.pb.h>
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
#include <Nodes/Node.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Plans/Utils/QueryPlanIterator.hpp>
#include <utility>
namespace NES {
class EndDeviceProtocolSerializationUtil {
  public:
    class UnsupportedEDSerialisationException : public std::runtime_error {
      public:
        UnsupportedEDSerialisationException() : std::runtime_error("Can't serialize this node, since the ED does not support it."){};
    };
    using EDRegistersPtr = std::shared_ptr<std::vector<std::string>>;
    using EDMapOperationPtr = std::shared_ptr<EndDeviceProtocol::MapOperation>;
    using EDFilterOperationPtr = std::shared_ptr<EndDeviceProtocol::FilterOperation>;
    using EDWindowOperationPtr = std::shared_ptr<EndDeviceProtocol::WindowOperation>;
    using EDQueryOperationPtr = std::shared_ptr<EndDeviceProtocol::Query_Operation>;
    using EDQueryPtr = std::shared_ptr<EndDeviceProtocol::Query>;

    [[nodiscard]] static EDQueryPtr serializeQueryPlanToEndDevice(QueryPlanPtr QP);

    [[nodiscard]] static std::string serializeConstantValue(ExpressionNodePtr cnode);

    [[nodiscard]] static std::string serializeArithmeticalExpression(ExpressionNodePtr, EDRegistersPtr);
    [[nodiscard]] static std::string serializeLogicalExpression(ExpressionNodePtr, EDRegistersPtr);
    [[nodiscard]] static std::string serializeFieldAccessExpression(ExpressionNodePtr, EDRegistersPtr);
    [[nodiscard]] static std::string serializeExpression(ExpressionNodePtr, EDRegistersPtr);
    [[nodiscard]] static EDMapOperationPtr serializeMapOperator( NodePtr, EDRegistersPtr);
    [[nodiscard]] static EDFilterOperationPtr serializeFilterOperator(NodePtr, EDRegistersPtr);
    [[nodiscard]] static EDWindowOperationPtr serializeWindowOperator(NodePtr, EDRegistersPtr);
    [[nodiscard]] static EDQueryOperationPtr serializeOperator(NodePtr, EDRegistersPtr);
    [[nodiscard]] static std::string asString(EndDeviceProtocol::ExpressionInstructions);
  private:
    static DefaultPhysicalTypeFactory physicalFactory;
};
}// namespace NES

#endif//NES_ENDDEVICEPROTOCOLSERIALIZATIONUTIL_HPP
