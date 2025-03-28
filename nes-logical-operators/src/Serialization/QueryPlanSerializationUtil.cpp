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

#include <Plans/QueryPlan.hpp>
#include <Serialization/OperatorSerializationUtil.hpp>
#include <Serialization/QueryPlanSerializationUtil.hpp>
#include <Util/Logger/Logger.hpp>
#include <SerializableOperator.pb.h>
#include <SerializableQueryPlan.pb.h>

namespace NES
{

void QueryPlanSerializationUtil::serializeQueryPlan(
    const QueryPlan& queryPlan, SerializableQueryPlan* serializableQueryPlan, bool isClientOriginated)
{
    NES_DEBUG("QueryPlanSerializationUtil: serializing query plan {}", queryPlan.toString());
    std::vector<std::shared_ptr<Operator>> rootOperators = queryPlan.getRootOperators();
    NES_DEBUG("QueryPlanSerializationUtil: serializing the operator chain for each root operator independently");

    ///Serialize Query Plan operators
    auto& serializedOperatorMap = *serializableQueryPlan->mutable_operatormap();
    auto bfsIterator = PlanIterator(queryPlan);
    for (auto itr = bfsIterator.begin(); itr != PlanIterator::end(); ++itr)
    {
        auto visitingOp = NES::Util::as<LogicalOperator>(*itr);
        if (serializedOperatorMap.find(visitingOp->getId().getRawValue()) != serializedOperatorMap.end())
        {
            /// skip rest of the steps as the operator is already serialized
            continue;
        }
        NES_TRACE("QueryPlan: Inserting operator in collection of already visited node.");
        SerializableOperator serializeOperator = OperatorSerializationUtil::serializeOperator(visitingOp);
        serializedOperatorMap[visitingOp->getId().getRawValue()] = serializeOperator;
    }

    ///Serialize the root operator ids
    for (const auto& rootOperator : rootOperators)
    {
        auto rootOperatorId = rootOperator->getId();
        serializableQueryPlan->add_rootoperatorids(rootOperatorId.getRawValue());
    }

    if (!isClientOriginated)
    {
        ///Serialize the sub query plan and query plan id
        NES_TRACE("QueryPlanSerializationUtil: serializing the Query sub plan id and query id");
        serializableQueryPlan->set_queryid(queryPlan.getQueryId().getRawValue());
    }
}

std::shared_ptr<QueryPlan> QueryPlanSerializationUtil::deserializeQueryPlan(const SerializableQueryPlan* serializedQueryPlan)
{
    NES_TRACE("QueryPlanSerializationUtil: Deserializing query plan {}", serializedQueryPlan->DebugString());
    std::vector<std::shared_ptr<Operator>> rootOperators;
    std::map<uint64_t, std::shared_ptr<Operator>> operatorIdToOperatorMap;

    ///Deserialize all operators in the operator map
    for (const auto& operatorIdAndSerializedOperator : serializedQueryPlan->operatormap())
    {
        const auto& serializedOperator = operatorIdAndSerializedOperator.second;
        operatorIdToOperatorMap[serializedOperator.operatorid()] = OperatorSerializationUtil::deserializeOperator(serializedOperator);
    }

    ///Add deserialized children
    for (const auto& operatorIdAndSerializedOperator : serializedQueryPlan->operatormap())
    {
        const auto& serializedOperator = operatorIdAndSerializedOperator.second;
        auto deserializedOperator = operatorIdToOperatorMap[serializedOperator.operatorid()];
        for (auto childId : serializedOperator.childrenids())
        {
            deserializedOperator->addChild(operatorIdToOperatorMap[childId]);
        }
    }

    ///add root operators
    for (auto rootOperatorId : serializedQueryPlan->rootoperatorids())
    {
        rootOperators.emplace_back(operatorIdToOperatorMap[rootOperatorId]);
    }

    ///set properties of the query plan
    QueryId queryId = INVALID_QUERY_ID;

    if (serializedQueryPlan->has_queryid())
    {
        queryId = QueryId(serializedQueryPlan->queryid());
    }

    return QueryPlan::create(queryId, rootOperators);
}
}
