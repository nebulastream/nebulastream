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

#include <QueryStateBackend.hpp>

#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <string>
#include <system_error>
#include <unordered_map>
#include <utility>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Serialization/QueryPlanSerializationUtil.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Strings.hpp>
#include <fmt/format.h>
#include <google/protobuf/util/json_util.h>
#include <rfl/Rename.hpp>
#include <rfl/json/read.hpp>
#include <rfl/json/write.hpp>
#include <DistributedQuery.hpp>
#include <ErrorHandling.hpp>

namespace NES::CLI
{
namespace
{

struct PersistedLocalQueryState
{
    rfl::Rename<"host", std::string> host;
    rfl::Rename<"local_query_id", std::string> localQueryId;
    rfl::Rename<"logical_plan_protobuf", std::string> logicalPlanProtobuf;
    rfl::Rename<"epoch", int> epoch;
};

struct PersistedDistributedQueryState
{
    rfl::Rename<"query_id", std::string> queryId;
    rfl::Rename<"local_queries", std::unordered_map<std::string, std::vector<PersistedLocalQueryState>>> localQueries;
    rfl::Rename<"created_at", std::string> createdAt;
};
}

std::string PersistedQueryId::toString() const
{
    return queryId.getRawValue();
}

PersistedQueryId PersistedQueryId::fromString(std::string_view input)
{
    return PersistedQueryId{DistributedQueryId(input)};
}

QueryStateBackend::QueryStateBackend() : stateDirectory(getStateDirectory())
{
    NES_DEBUG("QueryStateBackend initialized with state directory: {}", stateDirectory.string());
}

std::filesystem::path QueryStateBackend::getStateDirectory()
{
    std::filesystem::path stateDir;
    ///NOLINTNEXTLINE(concurrency-mt-unsafe) This is only used at startup on a single thread
    if (const char* xdgState = std::getenv("XDG_STATE_HOME"))
    {
        if (*xdgState != '\0')
        {
            stateDir = std::filesystem::path(xdgState) / "nebucli";
            NES_DEBUG("Using XDG_STATE_HOME: {}", stateDir.string());
        }
    }
    if (stateDir.empty())
    {
        ///NOLINTNEXTLINE(concurrency-mt-unsafe) This is only used at startup on a single thread
        const char* home = std::getenv("HOME");
        if (home == nullptr || *home == '\0')
        {
            throw InvalidConfigParameter("Cannot determine state directory: Neither XDG_STATE_HOME nor HOME environment variables are set");
        }
        stateDir = std::filesystem::path(home) / ".local" / "state" / "nebucli";
        NES_DEBUG("Using fallback state directory: {}", stateDir.string());
    }
    std::error_code errorCode;
    std::filesystem::create_directories(stateDir, errorCode);
    if (errorCode)
    {
        throw InvalidConfigParameter("Failed to create state directory {}: {}", stateDir.string(), errorCode.message());
    }
    return stateDir;
}

std::filesystem::path QueryStateBackend::getQueryFilePath(const DistributedQueryId& distributedQueryId)
{
    return stateDirectory / fmt::format("{}.json", distributedQueryId.getRawValue());
}

PersistedQueryId QueryStateBackend::store(const DistributedQueryId& distributedQueryId, const DistributedQuery& distributedQuery)
{
    auto filePath = getQueryFilePath(distributedQueryId);
    PersistedDistributedQueryState state;
    state.queryId = distributedQueryId.getRawValue();
    for (const auto& [grpcAddr, localQuery] : distributedQuery.iterate())
    {
        std::string serializedLocalPlan;
        if (!google::protobuf::util::MessageToJsonString(
                 QueryPlanSerializationUtil::serializeQueryPlan(localQuery.localPlan), &serializedLocalPlan)
                 .ok())
        {
            INVARIANT(false, "Couldn't serialize local plan, find out why");
        }

        state.localQueries()[grpcAddr.getRawValue()].push_back(
            PersistedLocalQueryState{
                grpcAddr.getRawValue(), localQuery.queryId.getLocalQueryId().getRawValue(), serializedLocalPlan, localQuery.epoch});
    }
    state.createdAt = fmt::format("{:%Y-%m-%dT%H:%M:%S%z}", std::chrono::system_clock::now());

    std::ofstream file(filePath);
    if (!file)
    {
        throw InvalidConfigParameter("Failed to open state file for writing: {}", filePath.string());
    }
    file << rfl::json::write(state, rfl::json::pretty);
    file.close();
    if (!file)
    {
        throw InvalidConfigParameter("Failed to write state file: {}", filePath.string());
    }
    NES_DEBUG("Stored query state: {}", filePath.string());
    return PersistedQueryId{distributedQueryId};
}

DistributedQuery QueryStateBackend::load(PersistedQueryId persistedId)
{
    auto filePath = getQueryFilePath(persistedId.queryId);
    if (!std::filesystem::exists(filePath))
    {
        throw InvalidConfigParameter("Could not find query with id {}. Expected file: {}", persistedId.queryId, filePath.string());
    }
    std::ifstream file(filePath);
    if (!file)
    {
        throw InvalidConfigParameter("Could not open state file: {}", filePath.string());
    }
    std::stringstream contents;
    contents << file.rdbuf();
    auto parsed = rfl::json::read<PersistedDistributedQueryState>(contents.str());
    if (!parsed)
    {
        throw InvalidConfigParameter("Failed to parse state file {}: {}", filePath.string(), parsed.error().what());
    }
    const auto state = std::move(parsed.value());
    auto distributedId = DistributedQueryId(state.queryId());
    std::unordered_map<Host, std::vector<LocalQuery>> queries;
    for (const auto& [workerKey, persistedLocalQueries] : state.localQueries())
    {
        const Host worker(workerKey);
        std::vector<LocalQuery> localQueries;
        localQueries.reserve(persistedLocalQueries.size());
        for (const auto& localQuery : persistedLocalQueries)
        {
            SerializableQueryPlan serializableQueryPlan;
            auto status = google::protobuf::util::JsonStringToMessage(localQuery.logicalPlanProtobuf.get(), &serializableQueryPlan);

            if (!status.ok())
            {
                INVARIANT(false, "Couldn't parse local plan, find out why");
            }
            localQueries.emplace_back(
                LocalQuery{
                    worker,
                    QueryId::create(LocalQueryId(localQuery.localQueryId.get()), distributedId),
                    QueryPlanSerializationUtil::deserializeQueryPlan(serializableQueryPlan),
                    localQuery.epoch.get()});
        }
        queries.emplace(worker, std::move(localQueries));
    }
    NES_DEBUG("Loaded query state from: {}", filePath.string());
    return DistributedQuery(queries);
}

void QueryStateBackend::remove(PersistedQueryId persistedId)
{
    auto filePath = getQueryFilePath(persistedId.queryId);
    std::error_code errorCode;
    std::filesystem::remove(filePath, errorCode);
    if (errorCode)
    {
        NES_WARNING("Failed to remove state file {}: {}", filePath.string(), errorCode.message());
    }
    else
    {
        NES_DEBUG("Removed query state file: {}", filePath.string());
    }
}
}
