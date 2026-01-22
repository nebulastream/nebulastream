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

/// Forward declare DistributedQueryId (defined in nes-nebuli)
using DistributedQueryId = NESStrongStringType<struct DistributedQueryId_, "invalid">;

/**
 * @brief Unified query identifier combining local and distributed query IDs.
 *
 * This class encapsulates both the LocalQueryId (UUID) used for worker-local tracking
 * and the DistributedQueryId (human-readable name) used for distributed coordination.
 *
 * Usage patterns:
 * - createLocal(): For local-only queries (no distributed ID)
 * - createDistributed(globalId): For new distributed queries
 * - QueryId(localId, globalId): When both IDs are already known
 * - QueryId(localId): For local-only or when global ID is not yet known
 */
class QueryId
{
public:
    /**
     * @brief Creates a new QueryId with a freshly generated local UUID and no distributed ID.
     * Use this for local-only queries that are not part of a distributed query plan.
     */
    static QueryId createLocal();

    /**
     * @brief Creates a new QueryId with a freshly generated local UUID and the provided distributed ID.
     * Use this on the coordinator when submitting a new distributed query.
     */
    static QueryId createDistributed(DistributedQueryId globalQueryId);

    /**
     * @brief Creates a QueryId from existing IDs.
     * Use this when both IDs are already known (e.g., when deserializing from RPC).
     */
    QueryId(LocalQueryId localQueryId, DistributedQueryId globalQueryId);

    /**
     * @brief Creates a QueryId with only a local ID (distributed ID will be INVALID).
     * Use this for local-only queries or when the distributed ID is not yet known.
     */
    explicit QueryId(LocalQueryId localQueryId);

    /**
     * @brief Default constructor creates an INVALID QueryId.
     * Should only be used in contexts where the QueryId will be immediately assigned a valid value.
     */
    QueryId();

    /**
     * @brief Returns true if this QueryId represents a distributed query (has a valid global ID)
     */
    [[nodiscard]] bool isDistributed() const;

    /**
     * @brief Returns true if this QueryId is valid (has a valid local ID)
     */
    [[nodiscard]] bool isValid() const;

    /**
     * @brief Returns the local query ID component
     */
    [[nodiscard]] const LocalQueryId& getLocalQueryId() const { return localQueryId; }

    /**
     * @brief Returns the distributed query ID component (may be INVALID)
     */
    [[nodiscard]] const DistributedQueryId& getGlobalQueryId() const { return globalQueryId; }

    /**
     * @brief Equality based on local query ID only (since local ID is the primary identifier)
     */
    bool operator==(const QueryId& other) const;

    /**
     * @brief Inequality operator
     */
    bool operator!=(const QueryId& other) const;

    /**
     * @brief Equality operator with LocalQueryId
     */
    bool operator==(const LocalQueryId& other) const;

    /**
     * @brief Inequality operator with LocalQueryId
     */
    bool operator!=(const LocalQueryId& other) const;

    /**
     * @brief Output stream operator for logging
     */
    friend std::ostream& operator<<(std::ostream& os, const QueryId& queryId);

    /// Public members for direct access (maintaining backward compatibility)
    LocalQueryId localQueryId;
    DistributedQueryId globalQueryId;
};

}

/// Hash support for unordered containers (combines both local and global IDs)
namespace std
{
template <>
struct hash<NES::QueryId>
{
    size_t operator()(const NES::QueryId& queryId) const noexcept
    {
        size_t h1 = std::hash<NES::LocalQueryId>{}(queryId.localQueryId);
        size_t h2 = std::hash<NES::DistributedQueryId>{}(queryId.globalQueryId);
        return h1 ^ (h2 << 1);
    }
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
        if (queryId.isDistributed())
        {
            return fmt::format_to(ctx.out(), "QueryId(local={}, global={})", queryId.localQueryId, queryId.globalQueryId);
        }
        return fmt::format_to(ctx.out(), "QueryId(local={})", queryId.localQueryId);
    }
};
