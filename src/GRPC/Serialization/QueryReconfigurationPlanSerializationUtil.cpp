/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

#include <GRPC/Serialization/QueryReconfigurationPlanSerializationUtil.hpp>
#include <Plans/Query/QueryReconfigurationPlan.hpp>

#include <SerializableQueryReconfigurationPlan.pb.h>

namespace NES {
SerializableQueryReconfigurationPlan* QueryReconfigurationPlanSerializationUtil::serializeQueryReconfigurationPlan(
    QueryReconfigurationPlanPtr queryReconfigurationPlan) {
    auto serializableQueryReconfigurationPlan = new SerializableQueryReconfigurationPlan();
    serializableQueryReconfigurationPlan->set_id(queryReconfigurationPlan->getId());
    serializableQueryReconfigurationPlan->set_queryid(queryReconfigurationPlan->getQueryId());

    SerializableQueryReconfigurationPlan_QueryReconfigurationType reconfigurationType;

    if (queryReconfigurationPlan->getReconfigurationType() == QueryReconfigurationTypes::DATA_SINK) {
        reconfigurationType = SerializableQueryReconfigurationPlan_QueryReconfigurationType::
            SerializableQueryReconfigurationPlan_QueryReconfigurationType_DATA_SINK;
    } else {
        reconfigurationType = SerializableQueryReconfigurationPlan_QueryReconfigurationType::
            SerializableQueryReconfigurationPlan_QueryReconfigurationType_DATA_SOURCE;
    }

    serializableQueryReconfigurationPlan->set_queryreconfigurationtype(reconfigurationType);
    serializableQueryReconfigurationPlan->set_oldquerysubplanid(queryReconfigurationPlan->getOldQuerySubPlanId());
    serializableQueryReconfigurationPlan->set_newquerysubplanid(queryReconfigurationPlan->getNewQuerySubPlanId());
    return serializableQueryReconfigurationPlan;
}

QueryReconfigurationPlanPtr QueryReconfigurationPlanSerializationUtil::deserializeQueryReconfigurationPlan(
    SerializableQueryReconfigurationPlan* serializedQueryReconfigurationPlan) {

    QueryReconfigurationTypes reconfigurationType;

    if (serializedQueryReconfigurationPlan->queryreconfigurationtype()
        == SerializableQueryReconfigurationPlan_QueryReconfigurationType::
            SerializableQueryReconfigurationPlan_QueryReconfigurationType_DATA_SINK) {
        reconfigurationType = QueryReconfigurationTypes::DATA_SINK;
    } else {
        reconfigurationType = QueryReconfigurationTypes::DATA_SOURCE;
    }

    return std::make_shared<QueryReconfigurationPlan>(serializedQueryReconfigurationPlan->id(),
                                                      serializedQueryReconfigurationPlan->queryid(),
                                                      reconfigurationType,
                                                      serializedQueryReconfigurationPlan->oldquerysubplanid(),
                                                      serializedQueryReconfigurationPlan->newquerysubplanid());
}
}// namespace NES