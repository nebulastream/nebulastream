/// WASM stub: QueryStatus from nes-single-node-worker/interface.
/// Duplicated here to avoid adding nes-single-node-worker/interface to include paths
/// (which would conflict with our SingleNodeWorkerConfiguration stub).
#pragma once

#include <chrono>
#include <cstdint>
#include <optional>
#include <ostream>
#include <Identifiers/Identifiers.hpp>
#include <Util/Logger/Formatter.hpp>
#include <magic_enum/magic_enum.hpp>
#include <ErrorHandling.hpp>
#include <QueryId.hpp>

namespace NES
{

enum class QueryState : uint8_t
{
    Registered,
    Started,
    Running,
    Stopped,
    Failed,
};

inline std::ostream& operator<<(std::ostream& ostream, const QueryState& status)
{
    return ostream << magic_enum::enum_name(status);
}

struct QueryMetrics
{
    std::optional<std::chrono::system_clock::time_point> start;
    std::optional<std::chrono::system_clock::time_point> running;
    std::optional<std::chrono::system_clock::time_point> stop;
    std::optional<Exception> error;
};

struct LocalQueryStatus
{
    QueryId queryId = INVALID_QUERY_ID;
    QueryState state = QueryState::Registered;
    QueryMetrics metrics{};
};

}

FMT_OSTREAM(NES::QueryState);
