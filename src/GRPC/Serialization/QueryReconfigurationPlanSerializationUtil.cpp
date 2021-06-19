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
    for (auto querySubPlanId : queryReconfigurationPlan->getQuerySubPlanIdsToStart()) {
        serializableQueryReconfigurationPlan->add_querysubplanidstostart(querySubPlanId);
    }
    for (auto querySubPlanId : queryReconfigurationPlan->getQuerySubPlanIdsToStop()) {
        serializableQueryReconfigurationPlan->add_querysubplanidstostop(querySubPlanId);
    }
    for (auto querySubPlanPair : queryReconfigurationPlan->getQuerySubPlanIdsToReplace()) {
        auto mapping = serializableQueryReconfigurationPlan->add_querysubplanidstoreplace();
        mapping->set_oldquerysubplanid(querySubPlanPair.first);
        mapping->set_newquerysubplanid(querySubPlanPair.second);
    }
    return serializableQueryReconfigurationPlan;
}

QueryReconfigurationPlanPtr QueryReconfigurationPlanSerializationUtil::deserializeQueryReconfigurationPlan(
    SerializableQueryReconfigurationPlan* serializedQueryReconfigurationPlan) {
    std::vector<QuerySubPlanId> subPlansToStart;
    std::vector<QuerySubPlanId> subPlansToStop;
    std::unordered_map<QuerySubPlanId, QuerySubPlanId> subPlansToReplace;
    for (auto subPlanId : serializedQueryReconfigurationPlan->querysubplanidstostart()) {
        subPlansToStart.emplace_back(subPlanId);
    }
    for (auto subPlanId : serializedQueryReconfigurationPlan->querysubplanidstostop()) {
        subPlansToStop.emplace_back(subPlanId);
    }
    for (auto mapping : serializedQueryReconfigurationPlan->querysubplanidstoreplace()) {
        subPlansToReplace.insert(
            std::pair<QuerySubPlanId, QuerySubPlanId>(mapping.oldquerysubplanid(), mapping.newquerysubplanid()));
    }
    auto queryReconfigurationPlan = QueryReconfigurationPlan::create(subPlansToStart, subPlansToStop, subPlansToReplace);
    queryReconfigurationPlan->setId(serializedQueryReconfigurationPlan->id());
    queryReconfigurationPlan->setQueryId(serializedQueryReconfigurationPlan->queryid());
    return queryReconfigurationPlan;
}
}// namespace NES