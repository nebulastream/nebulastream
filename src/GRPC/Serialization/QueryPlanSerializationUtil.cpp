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

#include <GRPC/Serialization/OperatorSerializationUtil.hpp>
#include <GRPC/Serialization/QueryPlanSerializationUtil.hpp>
#include <Operators/LogicalOperators/LogicalOperatorForwardRefs.hpp>
#include <Plans/Query/QueryPlan.hpp>

#include <SerializableOperator.pb.h>
#include <SerializableQueryPlan.pb.h>

namespace NES {

SerializableQueryPlan* QueryPlanSerializationUtil::serializeQueryPlan(const QueryPlanPtr& queryPlan) {
    NES_INFO("QueryPlanSerializationUtil: serializing query plan " << queryPlan->toString());
    auto* serializedQueryPlan = new SerializableQueryPlan();
    std::vector<OperatorNodePtr> rootOperators = queryPlan->getRootOperators();
    NES_TRACE("QueryPlanSerializationUtil: serializing the operator chain for each root operator independently");
    for (const auto& rootOperator : rootOperators) {
        auto& serializedOperatorMap = *serializedQueryPlan->mutable_operatormap();
        SerializableOperator* pOperator = OperatorSerializationUtil::serializeOperator(rootOperator);
        u_int64_t rootOperatorId = rootOperator->getId();
        serializedOperatorMap[rootOperatorId] = *pOperator;
        serializedQueryPlan->add_rootoperatorids(rootOperatorId);
    }
    NES_TRACE("QueryPlanSerializationUtil: serializing the Query sub plan id and query id");
    serializedQueryPlan->set_querysubplanid(queryPlan->getQuerySubPlanId());
    serializedQueryPlan->set_queryid(queryPlan->getQueryId());
    return serializedQueryPlan;
}

QueryPlanPtr QueryPlanSerializationUtil::deserializeQueryPlan(SerializableQueryPlan* serializedQueryPlan) {
    NES_TRACE("QueryPlanSerializationUtil: Deserializing query plan " << serializedQueryPlan->DebugString());
    std::vector<OperatorNodePtr> rootOperators;
    std::map<uint64_t, OperatorNodePtr> operatorIdToOperatorMap;

    //Deserialize all operators in the operator map
    for (const auto& operatorIdAndSerializedOperator : serializedQueryPlan->operatormap()) {
        auto serializedOperator = operatorIdAndSerializedOperator.second;
        operatorIdToOperatorMap[serializedOperator.operatorid()] =
            OperatorSerializationUtil::deserializeOperator(serializedOperator);
    }

    //Add deserialized children
    for (const auto& operatorIdAndSerializedOperator : serializedQueryPlan->operatormap()) {
        auto serializedOperator = operatorIdAndSerializedOperator.second;
        auto deserializedOperator = operatorIdToOperatorMap[serializedOperator.operatorid()];
        for (auto childId : serializedOperator.childrenids()) {
            deserializedOperator->addChild(operatorIdToOperatorMap[childId]);
        }
    }

    //add root operators
    for(auto rootOperatorId : serializedQueryPlan->rootoperatorids()){
        rootOperators.emplace_back(operatorIdToOperatorMap[rootOperatorId]);
    }

    //set properties of the query plan
    uint64_t queryId = serializedQueryPlan->queryid();
    uint64_t querySubPlanId = serializedQueryPlan->querysubplanid();
    return QueryPlan::create(queryId, querySubPlanId, rootOperators);
}

}// namespace NES