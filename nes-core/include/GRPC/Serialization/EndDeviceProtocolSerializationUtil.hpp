//
// Created by kasper on 1/10/23.
//

#ifndef NES_ENDDEVICEPROTOCOLSERIALIZATIONUTIL_HPP
#define NES_ENDDEVICEPROTOCOLSERIALIZATIONUTIL_HPP

#include <Nodes/Expressions/ConstantValueExpressionNode.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <EndDeviceProtocol.pb.h>
#include <Plans/Query/QueryPlan.hpp>
namespace NES {
    class EndDeviceProtocolSerializationUtil {
      public:
        static std::shared_ptr<messages::Message> serializeQueryPlanToEndDevice(QueryPlanPtr QP);

      private:
        static DefaultPhysicalTypeFactory physicalFactory;

        static std::tuple<int,int,int> serializeConstantValue(ConstantValueExpressionNode& cnode);
    };
}

#endif//NES_ENDDEVICEPROTOCOLSERIALIZATIONUTIL_HPP
