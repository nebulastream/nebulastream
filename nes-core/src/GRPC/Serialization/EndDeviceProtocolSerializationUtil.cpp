//
// Created by kasper on 1/10/23.
//

#include "Common/PhysicalTypes/BasicPhysicalType.hpp"
#include "Common/ValueTypes/BasicValue.hpp"
#include <Common/DataTypes/DataType.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <EndDeviceProtocol.pb.h>
#include <GRPC/Serialization/EndDeviceProtocolSerializationUtil.hpp>
#include <Nodes/Expressions/ArithmeticalExpressions/AbsExpressionNode.hpp>
#include <Nodes/Expressions/ArithmeticalExpressions/AddExpressionNode.hpp>
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
#include <Nodes/Expressions/CaseExpressionNode.hpp>
#include <Nodes/Expressions/ConstantValueExpressionNode.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Plans/Utils/QueryPlanIterator.hpp>
#include <utility>
namespace NES {
    DefaultPhysicalTypeFactory EndDeviceProtocolSerializationUtil::physicalFactory = DefaultPhysicalTypeFactory();

    std::shared_ptr<messages::Message> EndDeviceProtocolSerializationUtil::serializeQueryPlanToEndDevice(NES::QueryPlanPtr QP) {
        auto qpiterator = QueryPlanIterator(std::move(QP));

    //    for (auto op :qpiterator) {
    //        op->
    //    }

        return std::shared_ptr<messages::Message>();
        }

    std::tuple<int,int,int> EndDeviceProtocolSerializationUtil::serializeConstantValue(ConstantValueExpressionNode& cnode){
        auto val = cnode.getConstantValue();
        auto basicval = std::dynamic_pointer_cast<BasicValue>(val);
        auto physType = EndDeviceProtocolSerializationUtil::physicalFactory.getPhysicalType(basicval->dataType);
        auto basicPhystype = dynamic_pointer_cast<BasicPhysicalType>(physType);
        switch (basicPhystype->nativeType) {

        };

    }
}
