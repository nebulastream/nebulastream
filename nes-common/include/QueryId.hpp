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

#pragma once

#include <cstddef>
#include <functional>
#include <ostream>
#include <string>
#include <Identifiers/Identifiers.hpp>
#include <Identifiers/NESStrongType.hpp>
#include <Util/Logger/Formatter.hpp>
#include <nlohmann/json.hpp>

namespace NES
{

using DistributedQueryId = NESStrongStringType<struct DistributedQueryId_, "invalid">;

/**
 * @brief Unified query identifier combining local and distributed query IDs.
 *
 * This class encapsulates both the LocalQueryId (UUID) used for worker-local tracking
 * and the DistributedQueryId (human-readable name) used for distributed coordination.
 *
 * Four states:
 * - invalid():                     both INVALID — sentinel value
 * - createLocal(localId):          LocalQueryId set, DistributedQueryId INVALID — single-node / embedded
 * - createDistributed(globalId):   DistributedQueryId set, LocalQueryId INVALID — coordinator before dispatch
 * - create(localId, globalId):     both set — worker query belonging to a distributed query
 */
class QueryId
{
public:
    QueryId() = delete;

    static QueryId invalid() { return {INVALID_LOCAL_QUERY_ID, DistributedQueryId(DistributedQueryId::INVALID)}; }

    static QueryId createLocal(LocalQueryId localQueryId);
    static QueryId createDistributed(DistributedQueryId distributedQueryId);
    static QueryId create(LocalQueryId localQueryId, DistributedQueryId distributedQueryId);

    [[nodiscard]] bool isValid() const
    {
        return localQueryId != INVALID_LOCAL_QUERY_ID || distributedQueryId != DistributedQueryId(DistributedQueryId::INVALID);
    }

    [[nodiscard]] bool isDistributed() const;

    [[nodiscard]] const LocalQueryId& getLocalQueryId() const { return localQueryId; }

    [[nodiscard]] const DistributedQueryId& getDistributedQueryId() const { return distributedQueryId; }

    bool operator==(const QueryId& other) const;

    bool operator!=(const QueryId& other) const { return !(*this == other); }

    friend std::ostream& operator<<(std::ostream& os, const QueryId& queryId);

private:
    QueryId(LocalQueryId localQueryId, DistributedQueryId distributedQueryId);
    LocalQueryId localQueryId;
    DistributedQueryId distributedQueryId;
};

inline const QueryId INVALID_QUERY_ID = QueryId::invalid(); /// NOLINT(cert-err58-cpp)

}

namespace std
{
template <>
struct hash<NES::QueryId>
{
    std::size_t operator()(const NES::QueryId& queryId) const noexcept
    {
        const std::size_t localHash = std::hash<NES::LocalQueryId>{}(queryId.getLocalQueryId());
        const std::size_t distributedHash = std::hash<NES::DistributedQueryId>{}(queryId.getDistributedQueryId());
        return localHash ^ (static_cast<std::size_t>(distributedHash) << 1U);
    }
};
}

FMT_OSTREAM(NES::QueryId);

namespace nlohmann
{
template <>
struct adl_serializer<NES::QueryId>
{
    ///NOLINTNEXTLINE(readability-identifier-naming)
    static NES::QueryId from_json(const json& jsonObject)
    {
        if (jsonObject.is_object())
        {
            auto localId = NES::LocalQueryId(jsonObject.at("local_query_id").get<std::string>());
            auto distributedId = NES::DistributedQueryId(jsonObject.at("distributed_query_id").get<std::string>());
            return NES::QueryId::create(localId, distributedId);
        }
        return NES::QueryId::createLocal(NES::LocalQueryId(jsonObject.get<std::string>()));
    }

    ///NOLINTNEXTLINE(readability-identifier-naming)
    static void to_json(json& jsonObject, const NES::QueryId& queryId)
    {
        jsonObject["local_query_id"] = queryId.getLocalQueryId().getRawValue();
        jsonObject["distributed_query_id"] = queryId.getDistributedQueryId().getRawValue();
    }
};
}
