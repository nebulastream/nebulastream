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
#include <Plans/Utils/QueryPlanIterator.hpp>
#include <Plans/Query/QueryPlan.hpp>

#include <SerializableOperator.pb.h>
#include <SerializableQueryPlan.pb.h>

namespace NES {

void QueryPlanSerializationUtil::serializeQueryPlan(const QueryPlanPtr& queryPlan,
                                                                        SerializableQueryPlan* serializableQueryPlan) {
    NES_INFO("QueryPlanSerializationUtil: serializing query plan " << queryPlan->toString());
    std::vector<OperatorNodePtr> rootOperators = queryPlan->getRootOperators();
    NES_TRACE("QueryPlanSerializationUtil: serializing the operator chain for each root operator independently");

    //Serialize Query Plan operators
    auto& serializedOperatorMap = *serializableQueryPlan->mutable_operatormap();
    auto bfsIterator =  QueryPlanIterator(queryPlan);
    for (auto itr = bfsIterator.begin(); itr != QueryPlanIterator::end(); ++itr) {
        auto visitingOp = (*itr)->as<OperatorNode>();
        if (serializedOperatorMap.find(visitingOp->getId()) != serializedOperatorMap.end()) {
            // skip rest of the steps as the operator is already serialized
            continue;
        }
        NES_TRACE("QueryPlan: Inserting operator in collection of already visited node.");
        SerializableOperator serializeOperator = OperatorSerializationUtil::serializeOperator(visitingOp);
        serializedOperatorMap[visitingOp->getId()] = serializeOperator;
    }

    //Serialize the root operator ids
    for (const auto& rootOperator : rootOperators) {
        u_int64_t rootOperatorId = rootOperator->getId();
        serializableQueryPlan->add_rootoperatorids(rootOperatorId);
    }

    //Serialize the sub query plan and query plan id
    NES_TRACE("QueryPlanSerializationUtil: serializing the Query sub plan id and query id");
    serializableQueryPlan->set_querysubplanid(queryPlan->getQuerySubPlanId());
    serializableQueryPlan->set_queryid(queryPlan->getQueryId());
}

QueryPlanPtr QueryPlanSerializationUtil::deserializeQueryPlan(SerializableQueryPlan* serializedQueryPlan) {
    NES_DEBUG("QueryPlanSerializationUtil: Deserializing query plan " << serializedQueryPlan->DebugString());
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