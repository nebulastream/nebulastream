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

#include <QueryId.hpp>

#include <utility>

namespace NES
{

QueryId::QueryId(LocalQueryId localQueryId, DistributedQueryId distributedQueryId)
    : localQueryId(std::move(localQueryId)), distributedQueryId(std::move(distributedQueryId))
{
}

QueryId QueryId::createLocal(LocalQueryId localQueryId)
{
    return QueryId(std::move(localQueryId), DistributedQueryId(DistributedQueryId::INVALID));
}

QueryId QueryId::createDistributed(DistributedQueryId distributedQueryId)
{
    return QueryId(INVALID_LOCAL_QUERY_ID, std::move(distributedQueryId));
}

QueryId QueryId::create(LocalQueryId localQueryId, DistributedQueryId distributedQueryId)
{
    return QueryId(std::move(localQueryId), std::move(distributedQueryId));
}

bool QueryId::isDistributed() const
{
    return distributedQueryId != DistributedQueryId(DistributedQueryId::INVALID);
}

bool QueryId::operator==(const QueryId& other) const
{
    return localQueryId == other.localQueryId && distributedQueryId == other.distributedQueryId;
}

std::ostream& operator<<(std::ostream& os, const QueryId& queryId)
{
    if (queryId.isValid() && queryId.isDistributed())
    {
        os << "QueryId(local=" << queryId.localQueryId << ", distributed=" << queryId.distributedQueryId << ")";
    }
    else if (queryId.isValid())
    {
        os << "QueryId(local=" << queryId.localQueryId << ")";
    }
    else if (queryId.isDistributed())
    {
        os << "QueryId(distributed=" << queryId.distributedQueryId << ")";
    }
    else
    {
        os << "QueryId(INVALID)";
    }
    return os;
}

}
