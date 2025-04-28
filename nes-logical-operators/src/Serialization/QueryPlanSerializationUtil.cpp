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

#include <Plans/LogicalPlan.hpp>
#include <Serialization/OperatorSerializationUtil.hpp>
#include <Serialization/QueryPlanSerializationUtil.hpp>
#include <Util/Logger/Logger.hpp>
#include <SerializableOperator.pb.h>
#include <SerializableQueryPlan.pb.h>

namespace NES
{

SerializableQueryPlan QueryPlanSerializationUtil::serializeQueryPlan(const LogicalPlan& queryPlan)
{
    INVARIANT(queryPlan.rootOperators.size() == 1, "Query plan should currently have only one root operator");
    auto rootOperator = queryPlan.rootOperators[0];

    SerializableQueryPlan serializableQueryPlan;
    ///Serialize Query Plan operators
    auto& serializedOperatorMap = *serializableQueryPlan.mutable_operatormap();
    for (auto itr : BFSRange(rootOperator))
    {
        if (serializedOperatorMap.find(itr.getId().getRawValue()) != serializedOperatorMap.end())
        {
            /// skip rest of the steps as the operator is already serialized
            continue;
        }
        NES_TRACE("QueryPlan: Inserting operator in collection of already visited node.");
        SerializableOperator serializeOperator = itr.serialize();
        serializedOperatorMap[itr.getId().getRawValue()] = serializeOperator;
    }

    ///Serialize the root operator ids
    auto rootOperatorId = rootOperator.getId();
    serializableQueryPlan.add_rootoperatorids(rootOperatorId.getRawValue());
    return serializableQueryPlan;
}

LogicalPlan QueryPlanSerializationUtil::deserializeQueryPlan(const SerializableQueryPlan& serializedQueryPlan)
{
    std::vector<LogicalOperator> rootOperators;
    std::map<uint64_t, LogicalOperator> operatorIdToOperatorMap;

    ///Deserialize all operators in the operator map
    for (const auto& operatorIdAndSerializedOperator : serializedQueryPlan.operatormap())
    {
        const auto& serializedOperator = operatorIdAndSerializedOperator.second;
        operatorIdToOperatorMap.emplace(
            serializedOperator.operator_id(), OperatorSerializationUtil::deserializeOperator(serializedOperator));
    }

    ///Add deserialized children
    for (const auto& operatorIdAndSerializedOperator : serializedQueryPlan.operatormap())
    {
        const auto& serializedOperator = operatorIdAndSerializedOperator.second;
        for (auto childId : serializedOperator.children_ids())
        {
            auto child = operatorIdToOperatorMap.find(childId);
            if (child != operatorIdToOperatorMap.end())
            {
                auto it = operatorIdToOperatorMap.find(serializedOperator.operator_id());
                if (it != operatorIdToOperatorMap.end())
                {
                    it->second = it->second.withChildren({child->second});
                }
            }
        }
    }

    ///add root operators
    for (auto rootOperatorId : serializedQueryPlan.rootoperatorids())
    {
        auto root = operatorIdToOperatorMap.find(rootOperatorId);
        if (root != operatorIdToOperatorMap.end())
        {
            rootOperators.emplace_back(root->second);
        }
    }

    ///set properties of the query plan
    QueryId queryId = INVALID_QUERY_ID;
    if (serializedQueryPlan.has_queryid())
    {
        queryId = QueryId(serializedQueryPlan.queryid());
    }

    return LogicalPlan(queryId, std::move(rootOperators));
}
}
