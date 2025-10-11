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
#include <Identifiers/Identifiers.hpp>
#include <fmt/format.h>

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
    /**
     * @brief Default constructor - creates an invalid QueryId
     */
    QueryId() : localQueryId(INVALID_LOCAL_QUERY_ID) { }

    /**
     * @brief Construct from a LocalQueryId
     * @param localQueryId The local query identifier
     */
    QueryId(LocalQueryId localQueryId) : localQueryId(localQueryId) { }

    /**
     * @brief Create a new QueryId with a freshly generated UUID
     * @return A valid QueryId with a new local ID
     */
    static QueryId createLocal();

    /**
     * @brief Check if this QueryId is valid
     * @return true if the local query ID is valid
     */
    [[nodiscard]] bool isValid() const { return localQueryId != INVALID_LOCAL_QUERY_ID; }

    /**
     * @brief Get the local query ID
     * @return The LocalQueryId
     */
    [[nodiscard]] LocalQueryId getLocalQueryId() const { return localQueryId; }

    /**
     * @brief Equality operator
     */
    bool operator==(const QueryId& other) const { return localQueryId == other.localQueryId; }

    /**
     * @brief Inequality operator
     */
    bool operator!=(const QueryId& other) const { return !(*this == other); }

    /**
     * @brief Equality operator with LocalQueryId
     */
    bool operator==(const LocalQueryId& other) const { return localQueryId == other; }

    /**
     * @brief Inequality operator with LocalQueryId
     */
    bool operator!=(const LocalQueryId& other) const { return !(*this == other); }

    /**
     * @brief Output stream operator for logging
     */
    friend std::ostream& operator<<(std::ostream& os, const QueryId& queryId);

private:
    LocalQueryId localQueryId;
};

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

/// fmt formatter support
template <>
struct fmt::formatter<NES::QueryId>
{
    constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }

    template <typename FormatContext>
    auto format(const NES::QueryId& queryId, FormatContext& ctx) const
    {
        return fmt::format_to(ctx.out(), "{}", queryId.getLocalQueryId());
    }
};
