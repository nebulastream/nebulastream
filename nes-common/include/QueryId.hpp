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

#include <ostream>
#include <string>
#include <Identifiers/Identifiers.hpp>
#include <Util/Logger/Formatter.hpp>
#include <nlohmann/json.hpp>

namespace NES
{

/**
 * @brief Unified query identifier that encapsulates query identification.
 *
 * This class serves as a unified container for query IDs, currently wrapping
 * the LocalQueryId. This abstraction allows for future extension to include
 * distributed query identifiers without changing the API.
 *
 * The QueryId can be in an invalid state if constructed with an invalid LocalQueryId.
 */
class QueryId
{
public:
    QueryId() = delete;

    static QueryId invalid() { return QueryId(INVALID_LOCAL_QUERY_ID); }

    static QueryId createLocal(LocalQueryId localQueryId) { return QueryId(localQueryId); }

    [[nodiscard]] bool isValid() const { return localQueryId != INVALID_LOCAL_QUERY_ID; }

    [[nodiscard]] LocalQueryId getLocalQueryId() const { return localQueryId; }

    bool operator==(const QueryId& other) const { return localQueryId == other.localQueryId; }

    bool operator!=(const QueryId& other) const { return !(*this == other); }

    friend std::ostream& operator<<(std::ostream& os, const QueryId& queryId);

private:
    explicit QueryId(LocalQueryId localQueryId) : localQueryId(localQueryId) { }

    LocalQueryId localQueryId;
};

/// Convenience constant
inline const QueryId INVALID_QUERY_ID = QueryId::invalid();

}

/// Hash support for unordered containers
namespace std
{
template <>
struct hash<NES::QueryId>
{
    size_t operator()(const NES::QueryId& queryId) const noexcept { return std::hash<NES::LocalQueryId>{}(queryId.getLocalQueryId()); }
};
}

FMT_OSTREAM(NES::QueryId);

/// JSON serialization support
namespace nlohmann
{
template <>
struct adl_serializer<NES::QueryId>
{
    ///NOLINTNEXTLINE(readability-identifier-naming)
    static NES::QueryId from_json(const json& jsonObject)
    {
        return NES::QueryId::createLocal(NES::LocalQueryId(jsonObject.get<std::string>()));
    }

    ///NOLINTNEXTLINE(readability-identifier-naming)
    static void to_json(json& jsonObject, const NES::QueryId& queryId) { jsonObject = queryId.getLocalQueryId().getRawValue(); }
};
}
