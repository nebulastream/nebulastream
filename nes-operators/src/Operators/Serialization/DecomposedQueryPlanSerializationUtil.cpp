/*
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

#include <Operators/LogicalOperators/LogicalOperatorForwardRefs.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Operators/Serialization/DecomposedQueryPlanSerializationUtil.hpp>
#include <Operators/Serialization/OperatorSerializationUtil.hpp>
#include <Plans/DecomposedQueryPlan/DecomposedQueryPlan.hpp>
#include <Plans/Utils/PlanIdGenerator.hpp>
#include <Plans/Utils/PlanIterator.hpp>
#include <Util/Logger/Logger.hpp>
#include <SerializableDecomposedQueryPlan.pb.h>

namespace NES {

void DecomposedQueryPlanSerializationUtil::serializeDecomposedQueryPlan(
    const DecomposedQueryPlanPtr& decomposedQueryPlan,
    SerializableDecomposedQueryPlan* serializableDecomposedQueryPlan) {
    NES_DEBUG("QueryPlanSerializationUtil: serializing query plan {}", decomposedQueryPlan->toString());
    std::vector<OperatorNodePtr> rootOperators = decomposedQueryPlan->getRootOperators();
    NES_DEBUG("QueryPlanSerializationUtil: serializing the operator chain for each root operator independently");

    //Serialize Query Plan operators
    auto& serializedOperatorMap = *serializableDecomposedQueryPlan->mutable_operatormap();
    auto bfsIterator = PlanIterator(decomposedQueryPlan);
    for (auto itr = bfsIterator.begin(); itr != PlanIterator::end(); ++itr) {
        auto visitingOp = (*itr)->as<OperatorNode>();
        if (serializedOperatorMap.find(visitingOp->getId()) != serializedOperatorMap.end()) {
            // skip rest of the steps as the operator is already serialized
            continue;
        }
        NES_TRACE("QueryPlan: Inserting operator in collection of already visited node.");
        SerializableOperator serializeOperator = OperatorSerializationUtil::serializeOperator(visitingOp, false);
        serializedOperatorMap[visitingOp->getId()] = serializeOperator;
    }

    //Serialize the root operator ids
    for (const auto& rootOperator : rootOperators) {
        uint64_t rootOperatorId = rootOperator->getId();
        serializableDecomposedQueryPlan->add_rootoperatorids(rootOperatorId);
    }

    //Serialize the sub query plan and query plan id
    NES_TRACE("QueryPlanSerializationUtil: serializing the Query sub plan id and query id");
    serializableDecomposedQueryPlan->set_decomposedqueryplanid(decomposedQueryPlan->getDecomposedQueryPlanId());
    serializableDecomposedQueryPlan->set_sharedqueryplanid(decomposedQueryPlan->getSharedQueryId());
    serializableDecomposedQueryPlan->set_state(serializeQueryState(decomposedQueryPlan->getState()));
}

DecomposedQueryPlanPtr DecomposedQueryPlanSerializationUtil::deserializeDecomposedQueryPlan(
    NES::SerializableDecomposedQueryPlan* serializableDecomposedQueryPlan) {
    NES_TRACE("QueryPlanSerializationUtil: Deserializing query plan {}", serializableDecomposedQueryPlan->DebugString());
    std::vector<OperatorNodePtr> rootOperators;
    std::map<uint64_t, OperatorNodePtr> operatorIdToOperatorMap;

    //Deserialize all operators in the operator map
    for (const auto& operatorIdAndSerializedOperator : serializableDecomposedQueryPlan->operatormap()) {
        auto serializedOperator = operatorIdAndSerializedOperator.second;
        operatorIdToOperatorMap[serializedOperator.operatorid()] =
            OperatorSerializationUtil::deserializeOperator(serializedOperator);
    }

    //Add deserialized children
    for (const auto& operatorIdAndSerializedOperator : serializableDecomposedQueryPlan->operatormap()) {
        auto serializedOperator = operatorIdAndSerializedOperator.second;
        auto deserializedOperator = operatorIdToOperatorMap[serializedOperator.operatorid()];
        for (auto childId : serializedOperator.childrenids()) {
            deserializedOperator->addChild(operatorIdToOperatorMap[childId]);
        }
    }

    //add root operators
    for (auto rootOperatorId : serializableDecomposedQueryPlan->rootoperatorids()) {
        rootOperators.emplace_back(operatorIdToOperatorMap[rootOperatorId]);
    }

    //set properties of the query plan
    DecomposedQueryPlanId decomposableQueryPlanId = serializableDecomposedQueryPlan->decomposedqueryplanid();
    SharedQueryId sharedQueryId = serializableDecomposedQueryPlan->sharedqueryplanid();

    auto decomposedQueryPlan = DecomposedQueryPlan::create(decomposableQueryPlanId, sharedQueryId, rootOperators);
    if (serializableDecomposedQueryPlan->has_state()) {
        auto state = deserializeQueryState(serializableDecomposedQueryPlan->state());
        decomposedQueryPlan->setState(state);
    }
    return decomposedQueryPlan;
}

QueryState DecomposedQueryPlanSerializationUtil::deserializeQueryState(SerializableQueryState serializedQueryState) {
    switch (serializedQueryState) {
        case QUERY_STATE_REGISTERED: return QueryState::REGISTERED;
        case QUERY_STATE_OPTIMIZING: return QueryState::OPTIMIZING;
        case QUERY_STATE_DEPLOYED: return QueryState::DEPLOYED;
        case QUERY_STATE_RUNNING: return QueryState::RUNNING;
        case QUERY_STATE_MARKED_FOR_HARD_STOP: return QueryState::MARKED_FOR_HARD_STOP;
        case QUERY_STATE_MARKED_FOR_SOFT_STOP: return QueryState::MARKED_FOR_SOFT_STOP;
        case QUERY_STATE_SOFT_STOP_TRIGGERED: return QueryState::SOFT_STOP_TRIGGERED;
        case QUERY_STATE_SOFT_STOP_COMPLETED: return QueryState::SOFT_STOP_COMPLETED;
        case QUERY_STATE_STOPPED: return QueryState::STOPPED;
        case QUERY_STATE_MARKED_FOR_FAILURE: return QueryState::MARKED_FOR_FAILURE;
        case QUERY_STATE_FAILED: return QueryState::FAILED;
        case QUERY_STATE_RESTARTING: return QueryState::RESTARTING;
        case QUERY_STATE_MIGRATING: return QueryState::MIGRATING;
        case QUERY_STATE_MIGRATION_COMPLETED: return QueryState::MIGRATION_COMPLETED;
        case QUERY_STATE_EXPLAINED: return QueryState::EXPLAINED;
        case QUERY_STATE_REDEPLOYED: return QueryState::REDEPLOYED;
        case QUERY_STATE_MARKED_FOR_DEPLOYMENT: return QueryState::MARKED_FOR_DEPLOYMENT;
        case QUERY_STATE_MARKED_FOR_REDEPLOYMENT: return QueryState::MARKED_FOR_REDEPLOYMENT;
        case QUERY_STATE_MARKED_FOR_MIGRATION: return QueryState::MARKED_FOR_MIGRATION;
        case SerializableQueryState_INT_MIN_SENTINEL_DO_NOT_USE_: return QueryState::REGISTERED;
        case SerializableQueryState_INT_MAX_SENTINEL_DO_NOT_USE_: return QueryState::REGISTERED;
    }
}

SerializableQueryState DecomposedQueryPlanSerializationUtil::serializeQueryState(QueryState queryState) {
    switch (queryState) {
        case QueryState::REGISTERED: return QUERY_STATE_REGISTERED;
        case QueryState::OPTIMIZING: return QUERY_STATE_OPTIMIZING;
        case QueryState::DEPLOYED: return QUERY_STATE_DEPLOYED;
        case QueryState::RUNNING: return QUERY_STATE_RUNNING;
        case QueryState::MARKED_FOR_HARD_STOP: return QUERY_STATE_MARKED_FOR_HARD_STOP;
        case QueryState::MARKED_FOR_SOFT_STOP: return QUERY_STATE_MARKED_FOR_SOFT_STOP;
        case QueryState::SOFT_STOP_TRIGGERED: return QUERY_STATE_SOFT_STOP_TRIGGERED;
        case QueryState::SOFT_STOP_COMPLETED: return QUERY_STATE_SOFT_STOP_COMPLETED;
        case QueryState::STOPPED: return QUERY_STATE_STOPPED;
        case QueryState::MARKED_FOR_FAILURE: return QUERY_STATE_MARKED_FOR_FAILURE;
        case QueryState::FAILED: return QUERY_STATE_FAILED;
        case QueryState::RESTARTING: return QUERY_STATE_RESTARTING;
        case QueryState::MIGRATING: return QUERY_STATE_MIGRATING;
        case QueryState::MIGRATION_COMPLETED: return QUERY_STATE_MIGRATION_COMPLETED;
        case QueryState::EXPLAINED: return QUERY_STATE_EXPLAINED;
        case QueryState::REDEPLOYED: return QUERY_STATE_REDEPLOYED;
        case QueryState::MARKED_FOR_DEPLOYMENT: return QUERY_STATE_MARKED_FOR_DEPLOYMENT;
        case QueryState::MARKED_FOR_REDEPLOYMENT: return QUERY_STATE_MARKED_FOR_REDEPLOYMENT;
        case QueryState::MARKED_FOR_MIGRATION: return QUERY_STATE_MARKED_FOR_MIGRATION;
    }
}
}// namespace NES