//
// Created by kasper on 1/10/23.
//

#ifndef NES_ENDDEVICEPROTOCOLSERIALIZATIONUTIL_HPP
#define NES_ENDDEVICEPROTOCOLSERIALIZATIONUTIL_HPP

#include <Plans/Query/QueryPlan.hpp>
#include <EndDeviceProtocol.pb.h>
namespace NES {
    class EndDeviceProtocolSerializationUtil {
      public:
        static std::shared_ptr<Message> serializeQueryPlanToEndDevice(QueryPlanPtr QP);
    };
}

#endif//NES_ENDDEVICEPROTOCOLSERIALIZATIONUTIL_HPP
