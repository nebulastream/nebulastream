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
#include <SerializableDecomposedQueryPlan.pb.h>
#include <SerializableOperator.pb.h>
#include <Util/Logger/Logger.hpp>

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

    auto decomposedQueryPlan = DecomposedQueryPlan::create(decomposableQueryPlanId, sharedQueryId);
    for (const auto& rootOperator : rootOperators) {
        decomposedQueryPlan->addRootOperator(rootOperator);
    }
    return decomposedQueryPlan;
}

}// namespace NES