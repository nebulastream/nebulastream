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

#include <functional>
#include <unordered_map>
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
    /// Serialize Query Plan operators
    auto& serializedOperatorMap = *serializableQueryPlan.mutable_operatormap();
    for (auto itr : BFSRange(rootOperator))
    {
        if (serializedOperatorMap.find(itr.getId().getRawValue()) != serializedOperatorMap.end())
        {
            /// Skip rest of the steps as the operator is already serialized
            continue;
        }
        NES_TRACE("QueryPlan: Inserting operator in collection of already visited node.");
        SerializableOperator serializeOperator = itr.serialize();
        serializedOperatorMap[itr.getId().getRawValue()] = serializeOperator;
    }

    /// Serialize the root operator ids
    auto rootOperatorId = rootOperator.getId();
    serializableQueryPlan.add_rootoperatorids(rootOperatorId.getRawValue());
    return serializableQueryPlan;
}

LogicalPlan QueryPlanSerializationUtil::deserializeQueryPlan(const SerializableQueryPlan& serializedQueryPlan)
{
    /// 1) Deserialize all operators into a map
    std::unordered_map<uint64_t, LogicalOperator> baseOps;
    for (const auto& kv : serializedQueryPlan.operatormap())
    {
        const auto& serializedOp = kv.second;
        baseOps.emplace(
            serializedOp.operator_id(),
            OperatorSerializationUtil::deserializeOperator(serializedOp)
        );
    }

    /// 2) Recursive builder to attach all children
    std::unordered_map<uint64_t, LogicalOperator> builtOps;
    std::function<LogicalOperator(uint64_t)> build = [&](uint64_t id) -> LogicalOperator
    {
        auto memoIt = builtOps.find(id);
        if (memoIt != builtOps.end())
        {
            return memoIt->second;
        }

        auto baseIt = baseOps.find(id);
        INVARIANT(baseIt != baseOps.end(), "Unknown operator id: {}", id);
        LogicalOperator op = baseIt->second;

        const auto& serializedOp = serializedQueryPlan.operatormap().at(id);
        std::vector<LogicalOperator> children;
        for (auto childId : serializedOp.children_ids())
        {
            children.push_back(build(childId));
        }

        LogicalOperator withKids = op.withChildren(std::move(children));
        builtOps.emplace(id, withKids);
        return withKids;
    };

    /// 3) Build root-operators
    std::vector<LogicalOperator> rootOperators;
    for (auto rootId : serializedQueryPlan.rootoperatorids())
    {
        rootOperators.push_back(build(rootId));
    }

    /// 4) Finalize plan
    auto queryId = INVALID_QUERY_ID;
    if (serializedQueryPlan.has_queryid())
    {
        queryId = QueryId(serializedQueryPlan.queryid());
    }
    return LogicalPlan(queryId, std::move(rootOperators));
}
}
