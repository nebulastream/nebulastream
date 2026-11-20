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

#include <QueryIdSerialization.hpp>

#include <Identifiers/Identifiers.hpp>
#include <QueryId.hpp>
#include <SerializableQueryId.pb.h>

namespace NES
{

SerializableQueryId serializeQueryId(const QueryId& queryId)
{
    SerializableQueryId proto;
    proto.set_local_query_id(queryId.getLocalQueryId().getRawValue());
    proto.set_distributed_query_id(queryId.getDistributedQueryId().getRawValue());
    return proto;
}

QueryId deserializeQueryId(const SerializableQueryId& proto)
{
    const auto localId = LocalQueryId(proto.local_query_id());
    const auto distributedId = DistributedQueryId(proto.distributed_query_id());
    const bool hasLocal = localId != INVALID_LOCAL_QUERY_ID;
    const bool hasDistributed = distributedId != DistributedQueryId(DistributedQueryId::INVALID);
    if (hasLocal && hasDistributed)
    {
        return QueryId::create(localId, distributedId);
    }
    if (hasLocal)
    {
        return QueryId::createLocal(localId);
    }
    if (hasDistributed)
    {
        return QueryId::createDistributed(distributedId);
    }
    return QueryId::invalid();
}

}
