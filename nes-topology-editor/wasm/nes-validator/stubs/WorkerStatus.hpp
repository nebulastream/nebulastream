/// WASM stub: WorkerStatus without protobuf serialize/deserialize.
/// The real header includes SingleNodeWorkerRPCService.pb.h which we don't have in WASM.
#pragma once

#include <chrono>
#include <optional>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <ErrorHandling.hpp>
#include <QueryId.hpp>

namespace NES
{

struct WorkerStatus
{
    struct ActiveQuery
    {
        QueryId queryId = INVALID_QUERY_ID;
        std::optional<std::chrono::system_clock::time_point> started;
    };

    struct TerminatedQuery
    {
        QueryId queryId = INVALID_QUERY_ID;
        std::optional<std::chrono::system_clock::time_point> started;
        std::chrono::system_clock::time_point terminated;
        std::optional<Exception> error;
    };

    std::chrono::system_clock::time_point after;
    std::chrono::system_clock::time_point until;
    std::vector<ActiveQuery> activeQueries;
    std::vector<TerminatedQuery> terminatedQueries;
};

/// Omitted: serializeWorkerStatus / deserializeWorkerStatus (require protobuf)

}
