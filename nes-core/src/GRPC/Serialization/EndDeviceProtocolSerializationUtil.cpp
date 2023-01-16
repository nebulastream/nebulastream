//
// Created by kasper on 1/10/23.
//

#include <Plans/Utils/QueryPlanIterator.hpp>
#include <EndDeviceProtocol.pb.h>
#include <GRPC/Serialization/EndDeviceProtocolSerializationUtil.hpp>
#include <Plans/Query/QueryPlan.hpp>
namespace NES {
std::shared_ptr<Message> NES::EndDeviceProtocolSerializationUtil::serializeQueryPlanToEndDevice(NES::QueryPlanPtr QP) {
    auto qpiterator = QueryPlanIterator(QP);

    for (auto op :qpiterator) {
        op->
    }

    return std::shared_ptr<Message>();
    }
}
