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

#include <iostream>
#include <optional>
#include <vector>
#include <SystestState.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Serialization/QueryPlanSerializationUtil.hpp>
#include <fmt/format.h>

namespace NES::Systest
{

std::vector<RunningQuery> testSerialization(const std::vector<Query>& queries)
{
    std::vector<RunningQuery> failedQueries;
    for (const auto& query : queries)
    {
        SerializableQueryPlan serialized = QueryPlanSerializationUtil::serializeQueryPlan(query.queryPlan);
        auto result = QueryPlanSerializationUtil::deserializeQueryPlan(serialized);

        if (query.queryPlan != result)
        {
            std::cerr << fmt::format("Failed serialization! Before serialization:\n{}After serialization:\n{}\n",
                query.queryPlan.toString(), result.toString());
            RunningQuery failedQuery{query, INVALID_QUERY_ID, std::nullopt };
            failedQueries.push_back(failedQuery);
        }
    }
    return failedQueries;
}

}
