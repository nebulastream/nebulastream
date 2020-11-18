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

SerializableQueryPlan* QueryPlanSerializationUtil::serializeQueryPlan(QueryPlanPtr queryPlan) {
    NES_INFO("QueryPlanSerializationUtil: serializing query plan " << queryPlan->toString());
    auto serializedQueryPlan = new SerializableQueryPlan();
    std::vector<OperatorNodePtr> rootOperators = queryPlan->getRootOperators();
    NES_TRACE("QueryPlanSerializationUtil: serializing the operator chain for each root operator independently");
    for (auto rootOperator : rootOperators) {
        auto serializedRootOperator = serializedQueryPlan->add_rootoperators();
        OperatorSerializationUtil::serializeOperator(rootOperator, serializedRootOperator);
    }
    NES_TRACE("QueryPlanSerializationUtil: serializing the Query sub plan id and query id");
    serializedQueryPlan->set_querysubplanid(queryPlan->getQuerySubPlanId());
    serializedQueryPlan->set_queryid(queryPlan->getQueryId());
    return serializedQueryPlan;
}

QueryPlanPtr QueryPlanSerializationUtil::deserializeQueryPlan(SerializableQueryPlan* serializedQueryPlan) {
    NES_TRACE("QueryPlanSerializationUtil: Deserializing query plan " << serializedQueryPlan->DebugString());
    //We deserialize operator and its children together and prepare a map of operator id to operator.
    //We push the children operators to the queue of operators to be deserialized.
    //Every time we encounter an operator to be deserialized, we check in the map if the deserialized operator already exists.
    //We use the already deserialized operator whenever available other wise we deserialize the operator and add it to the map.
    std::vector<OperatorNodePtr> rootOperators;
    std::map<uint64_t, OperatorNodePtr> operatorIdToOperatorMap;
    for (auto serializedRootOperator : serializedQueryPlan->rootoperators()) {
        std::deque<SerializableOperator> operatorsToDeserialize;
        NES_TRACE("QueryPlanSerializationUtil: Add root operator to the Queue for deserialization");
        operatorsToDeserialize.push_back(serializedRootOperator);
        while (!operatorsToDeserialize.empty()) {
            auto operatorToDeserialize = operatorsToDeserialize.front();
            operatorsToDeserialize.pop_front();
            NES_TRACE("QueryPlanSerializationUtil: Deserialize operator " << operatorToDeserialize.DebugString());
            uint64_t operatorId = operatorToDeserialize.operatorid();
            OperatorNodePtr operatorNode;
            if (operatorIdToOperatorMap[operatorId]) {
                NES_TRACE("QueryPlanSerializationUtil: Operator was already deserialized previously");
                operatorNode = operatorIdToOperatorMap[operatorId];
            } else {
                NES_TRACE("QueryPlanSerializationUtil: Deserializing the operator and inserting into map");
                operatorNode = OperatorSerializationUtil::deserializeOperator(&operatorToDeserialize);
                operatorIdToOperatorMap[operatorId] = operatorNode;
            }
            NES_TRACE("QueryPlanSerializationUtil: Deserializing the children operators");
            for (auto childToDeserialize : operatorToDeserialize.children()) {
                uint64_t childOperatorId = childToDeserialize.operatorid();
                OperatorNodePtr childOperator;
                if (operatorIdToOperatorMap[childOperatorId]) {
                    childOperator = operatorIdToOperatorMap[childOperatorId];
                } else {
                    childOperator = OperatorSerializationUtil::deserializeOperator(&childToDeserialize);
                    operatorIdToOperatorMap[childOperatorId] = childOperator;
                }
                operatorNode->addChild(childOperator);
                NES_TRACE("QueryPlanSerializationUtil: Adding the children operators to the Queue in order to Deserializing its "
                          "children operators");
                operatorsToDeserialize.push_back(childToDeserialize);
            }
        }
        NES_TRACE("QueryPlanSerializationUtil: Adding the deserialized root operator to the vector of roots for the query plan");
        rootOperators.push_back(operatorIdToOperatorMap[serializedRootOperator.operatorid()]);
    }
    uint64_t queryId = serializedQueryPlan->queryid();
    uint64_t querySubPlanId = serializedQueryPlan->querysubplanid();
    return QueryPlan::create(queryId, querySubPlanId, rootOperators);
}

}// namespace NES