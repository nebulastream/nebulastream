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

#include <Util/UUID.hpp>

namespace NES
{

QueryId::QueryId() : localQueryId(INVALID_LOCAL_QUERY_ID), globalQueryId(DistributedQueryId(DistributedQueryId::INVALID))
{
}

QueryId::QueryId(LocalQueryId localQueryId) : localQueryId(localQueryId), globalQueryId(DistributedQueryId(DistributedQueryId::INVALID))
{
}

QueryId::QueryId(LocalQueryId localQueryId, DistributedQueryId globalQueryId)
    : localQueryId(localQueryId), globalQueryId(std::move(globalQueryId))
{
}

QueryId QueryId::createLocal()
{
    auto localQueryId = LocalQueryId(UUIDToString(generateUUID()));
    return QueryId(localQueryId, DistributedQueryId(DistributedQueryId::INVALID));
}

QueryId QueryId::createDistributed(DistributedQueryId globalQueryId)
{
    auto localQueryId = LocalQueryId(UUIDToString(generateUUID()));
    return QueryId(localQueryId, std::move(globalQueryId));
}

bool QueryId::isValid() const
{
    return localQueryId != INVALID_LOCAL_QUERY_ID;
}

bool QueryId::isDistributed() const
{
    return globalQueryId != DistributedQueryId(DistributedQueryId::INVALID);
}

bool QueryId::operator==(const QueryId& other) const
{
    return localQueryId == other.localQueryId && globalQueryId == other.globalQueryId;
}

bool QueryId::operator!=(const QueryId& other) const
{
    return !(*this == other);
}

bool QueryId::operator==(const LocalQueryId& other) const
{
    return localQueryId == other;
}

bool QueryId::operator!=(const LocalQueryId& other) const
{
    return localQueryId != other;
}

std::ostream& operator<<(std::ostream& os, const QueryId& queryId)
{
    if (queryId.isDistributed())
    {
        os << "QueryId(local=" << queryId.localQueryId << ", global=" << queryId.globalQueryId << ")";
    }
    else
    {
        os << "QueryId(local=" << queryId.localQueryId << ")";
    }
    return os;
}

}
