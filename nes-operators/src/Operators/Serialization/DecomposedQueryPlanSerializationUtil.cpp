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
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperator.hpp>
#include <Operators/Serialization/DecomposedQueryPlanSerializationUtil.hpp>
#include <Operators/Serialization/OperatorSerializationUtil.hpp>
#include <Plans/DecomposedQueryPlan/DecomposedQueryPlan.hpp>
#include <Plans/Utils/PlanIdGenerator.hpp>
#include <Plans/Utils/PlanIterator.hpp>
#include <SerializableDecomposedQueryPlan.pb.h>
#include <Util/CompilerConstants.hpp>
#include <Util/FaultToleranceType.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/QueryStateSerializationUtil.hpp>

namespace NES {

void DecomposedQueryPlanSerializationUtil::serializeDecomposedQueryPlan(
    const DecomposedQueryPlanPtr& decomposedQueryPlan,
    SerializableDecomposedQueryPlan* serializableDecomposedQueryPlan) {
    NES_DEBUG("QueryPlanSerializationUtil: serializing query plan {}", decomposedQueryPlan->toString());
    std::vector<OperatorPtr> rootOperators = decomposedQueryPlan->getRootOperators();
    NES_DEBUG("QueryPlanSerializationUtil: serializing the operator chain for each root operator independently");

    //Serialize Query Plan operators
    auto& serializedOperatorMap = *serializableDecomposedQueryPlan->mutable_operatormap();
    auto bfsIterator = PlanIterator(decomposedQueryPlan);
    for (auto itr = bfsIterator.begin(); itr != PlanIterator::end(); ++itr) {
        auto visitingOp = (*itr)->as<Operator>();
        if (serializedOperatorMap.find(visitingOp->getId().getRawValue()) != serializedOperatorMap.end()) {
            // skip rest of the steps as the operator is already serialized
            continue;
        }
        NES_TRACE("QueryPlan: Inserting operator in collection of already visited node.");
        SerializableOperator serializeOperator = OperatorSerializationUtil::serializeOperator(visitingOp, false);
        serializedOperatorMap[visitingOp->getId().getRawValue()] = serializeOperator;
    }

    //Serialize the root operator ids
    for (const auto& rootOperator : rootOperators) {
        auto rootOperatorId = rootOperator->getId();
        serializableDecomposedQueryPlan->add_rootoperatorids(rootOperatorId.getRawValue());
        if (rootOperator->instanceOf<SinkLogicalOperator>())
            serializableDecomposedQueryPlan->set_faulttolerance(
                static_cast<uint64_t>(rootOperator->as<SinkLogicalOperator>()->getSinkDescriptor()->getFaultToleranceType()));
    }

    //Serialize the sub query plan and query plan id
    NES_TRACE("QueryPlanSerializationUtil: serializing the Query sub plan id and query id");
    serializableDecomposedQueryPlan->set_decomposedqueryid(decomposedQueryPlan->getDecomposedQueryId().getRawValue());
    serializableDecomposedQueryPlan->set_decomposedqueryversion(decomposedQueryPlan->getVersion());
    serializableDecomposedQueryPlan->set_sharedqueryplanid(decomposedQueryPlan->getSharedQueryId().getRawValue());
    serializableDecomposedQueryPlan->set_state(QueryStateSerializationUtil::serializeQueryState(decomposedQueryPlan->getState()));
}

DecomposedQueryPlanPtr DecomposedQueryPlanSerializationUtil::deserializeDecomposedQueryPlan(
    const NES::SerializableDecomposedQueryPlan* serializableDecomposedQueryPlan) {
    NES_TRACE("QueryPlanSerializationUtil: Deserializing query plan {}", serializableDecomposedQueryPlan->DebugString());
    std::vector<OperatorPtr> rootOperators;
    std::map<uint64_t, OperatorPtr> operatorIdToOperatorMap;
    auto faultTolerance = serializableDecomposedQueryPlan->faulttolerance();
    //Deserialize all operators in the operator map
    for (const auto& operatorIdAndSerializedOperator : serializableDecomposedQueryPlan->operatormap()) {
        const auto& serializedOperator = operatorIdAndSerializedOperator.second;
        const OperatorPtr& deserializedOperator = OperatorSerializationUtil::deserializeOperator(serializedOperator);
        //Set the operator id sent by coordinator to the logical operator property
        const auto& operatorId = OperatorId(serializedOperator.operatorid());
        deserializedOperator->setId(operatorId);
        operatorIdToOperatorMap[serializedOperator.operatorid()] = deserializedOperator;
        if (deserializedOperator->instanceOf<SinkLogicalOperator>())
            deserializedOperator->as<SinkLogicalOperator>()->getSinkDescriptor()->setFaultToleranceType(
                static_cast<FaultToleranceType>(serializableDecomposedQueryPlan->faulttolerance()));
    }

    //Add deserialized children
    for (const auto& operatorIdAndSerializedOperator : serializableDecomposedQueryPlan->operatormap()) {
        const auto serializedOperator = operatorIdAndSerializedOperator.second;
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
    DecomposedQueryId decomposableQueryPlanId = DecomposedQueryId(serializableDecomposedQueryPlan->decomposedqueryid());
    DecomposedQueryPlanVersion decomposableQueryPlanVersion =
        DecomposedQueryPlanVersion(serializableDecomposedQueryPlan->decomposedqueryversion());
    SharedQueryId sharedQueryId = SharedQueryId(serializableDecomposedQueryPlan->sharedqueryplanid());

    auto decomposedQueryPlan =
        DecomposedQueryPlan::create(decomposableQueryPlanId, sharedQueryId, INVALID_WORKER_NODE_ID, rootOperators, FaultToleranceType(faultTolerance));
    decomposedQueryPlan->setVersion(decomposableQueryPlanVersion);
    if (serializableDecomposedQueryPlan->has_state()) {
        auto state = QueryStateSerializationUtil::deserializeQueryState(serializableDecomposedQueryPlan->state());
        decomposedQueryPlan->setState(state);
    }
    return decomposedQueryPlan;
}
}// namespace NES
