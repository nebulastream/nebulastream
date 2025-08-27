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

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <ostream>
#include <string>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>
#include <ErrorHandling.hpp>

#include <nlohmann/json.hpp>
#include <yaml-cpp/node/convert.h>

#include <Identifiers/Identifiers.hpp>
#include <Listeners/QueryLog.hpp>
#include <Util/Logger/Formatter.hpp>
#include <WorkerConfig.hpp>

namespace NES
{
inline DistributedQueryId getNextDistributedQueryId()
{
    static std::atomic_uint64_t id = INITIAL_DISTRIBUTED_QUERY_ID.getRawValue();
    return DistributedQueryId(id++);
}

struct LocalQuery
{
    LocalQueryId id;
    GrpcAddr grpcAddr;

    /// Default constructor for YAML deserialization
    LocalQuery() : id{0} { }

    LocalQuery(const size_t id, const GrpcAddr& addr) : id{id}, grpcAddr{addr} { }

    bool operator==(const LocalQuery& other) const = default;
};

class Query
{
    /// id that we expose to the outside at coordinator scope
    DistributedQueryId id;
    struct Backend
    {
        using Embedded = LocalQueryId;
        using Cluster = std::vector<LocalQuery>;
        std::variant<Embedded, Cluster> embeddedOrCluster;
    } backend;

public:
    /// Friend declarations for YAML serialization
    template <typename T>
    friend struct YAML::convert;

    /// Default constructor for YAML deserialization
    Query() : id{getNextDistributedQueryId()}, backend{std::vector<LocalQuery>{}} { }

    /// Embedded worker constructor
    Query(const LocalQueryId queryId) : id{getNextDistributedQueryId()}, backend{queryId} { }

    /// Cluster constructor
    explicit Query(std::vector<LocalQuery> queries) : id{getNextDistributedQueryId()}, backend{std::move(queries)}
    {
        PRECONDITION(
            not std::get<std::vector<LocalQuery>>(backend.embeddedOrCluster).empty(), "Cannot construct a distributed query without any local queries");
    }

    ~Query() = default;
    /// A query should only have one owner - move-only semantics
    Query(const Query& other) = delete;
    Query& operator=(const Query& other) = delete;
    Query(Query&& other) = default;
    Query& operator=(Query&& other) = default;

    [[nodiscard]] bool backendIsEmbedded() const { return std::holds_alternative<Backend::Embedded>(backend.embeddedOrCluster); }

    [[nodiscard]] bool backendIsCluster() const { return std::holds_alternative<Backend::Cluster>(backend.embeddedOrCluster); }

    [[nodiscard]] LocalQueryId getEmbeddedId() const
    {
        PRECONDITION(backendIsEmbedded(), "Query is not embedded");
        return std::get<LocalQueryId>(backend.embeddedOrCluster);
    }

    [[nodiscard]] const std::vector<LocalQuery>& getLocalQueries() const
    {
        PRECONDITION(backendIsCluster(), "Query is not deployed on a cluster");
        return std::get<std::vector<LocalQuery>>(backend.embeddedOrCluster);
    }

    [[nodiscard]] DistributedQueryId getId() const { return id; }

    /// Internal dispatch based on deployment
    template <typename EmbeddedFunc, typename ClusterFunc>
    auto dispatch(EmbeddedFunc&& onEmbedded, ClusterFunc&& onCluster) const
    {
        return std::visit(
            [&](const auto& value)
            {
                using T = std::decay_t<decltype(value)>;
                if constexpr (std::is_same_v<T, LocalQueryId>)
                {
                    return onEmbedded(value);
                }
                else
                {
                    return onCluster(value);
                }
            },
            backend.embeddedOrCluster);
    }

    /// Iteration support - only works in cluster mode
    [[nodiscard]] auto begin() const {
        PRECONDITION(backendIsCluster(), "Iteration only supported for cluster queries");
        return getLocalQueries().cbegin();
    }

    [[nodiscard]] auto end() const {
        PRECONDITION(backendIsCluster(), "Iteration only supported for cluster queries");
        return getLocalQueries().cend();
    }

    bool operator==(const Query& other) const
    {
        return backend.embeddedOrCluster == other.backend.embeddedOrCluster;
    }

    /// Serialization
    static Query load(const std::string& identifier);
    static std::string save(const Query& query);

    friend std::ostream& operator<<(std::ostream& os, const Query& query);
};

inline std::ostream& operator<<(std::ostream& os, const Query& query)
{
    os << "Query" << query.id.getRawValue() << "[";

    query.dispatch(
        [&os](const LocalQueryId& embeddedId) { os << embeddedId.getRawValue() << "@embedded"; },
        [&os](const std::vector<LocalQuery>& queries)
        {
            for (size_t i = 0; i < queries.size(); ++i)
            {
                if (i > 0)
                    os << ", ";
                const auto& localQuery = queries[i];
                os << localQuery.id.getRawValue();
                if (!localQuery.grpcAddr.empty())
                {
                    os << "@" << localQuery.grpcAddr;
                }
            }
        });

    os << "]";
    return os;
}

}

FMT_OSTREAM(NES::Query);

template <>
struct std::hash<NES::Query>
{
    size_t operator()(const NES::Query& query) const noexcept { return std::hash<uint64_t>{}(query.getId().getRawValue()); }
};
