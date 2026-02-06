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
#include <fstream>
#include <Util/Logger/Logger.hpp>
#include <Util/Strings.hpp>
#include <fmt/format.h>
#include <nlohmann/json.hpp>
#include <ErrorHandling.hpp>

namespace NES::CLI
{

std::string PersistedQueryId::toString() const
{
    return queryId.toString();
}

PersistedQueryId PersistedQueryId::fromString(std::string_view input)
{
    auto maybeValue = from_chars<uint64_t>(input);
    if (!maybeValue)
    {
        throw InvalidConfigParameter("Invalid query ID format: {}", input);
    }
    return PersistedQueryId{QueryId(*maybeValue)};
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
        if (!home || *home == '\0')
        {
            throw InvalidConfigParameter("Cannot determine state directory: Neither XDG_STATE_HOME nor HOME environment variables are set");
        }
        stateDir = std::filesystem::path(home) / ".local" / "state" / "nebucli";
        NES_DEBUG("Using fallback state directory: {}", stateDir.string());
    }

    std::error_code ec;
    std::filesystem::create_directories(stateDir, ec);

    if (ec)
    {
        throw InvalidConfigParameter("Failed to create state directory {}: {}", stateDir.string(), ec.message());
    }

    return stateDir;
}

std::filesystem::path QueryStateBackend::getQueryFilePath(QueryId queryId)
{
    return stateDirectory / fmt::format("{}.json", queryId.getRawValue());
}

PersistedQueryId QueryStateBackend::store(QueryId queryId)
{
    auto filePath = getQueryFilePath(queryId);

    nlohmann::json jsonObject;
    jsonObject["query_id"] = queryId.getRawValue();

    auto now = std::chrono::system_clock::now();
    jsonObject["created_at"] = fmt::format("{:%Y-%m-%dT%H:%M:%S%z}", now);

    std::ofstream file(filePath);
    if (!file)
    {
        throw InvalidConfigParameter("Failed to open state file for writing: {}", filePath.string());
    }

    file << jsonObject.dump(4);
    file.close();

    if (!file)
    {
        throw InvalidConfigParameter("Failed to write state file: {}", filePath.string());
    }

    NES_DEBUG("Stored query state: {}", filePath.string());
    return PersistedQueryId{queryId};
}

QueryId QueryStateBackend::load(PersistedQueryId persistedId)
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

    nlohmann::json jsonObject;
    try
    {
        jsonObject = nlohmann::json::parse(file);
    }
    catch (const nlohmann::json::parse_error& e)
    {
        throw InvalidConfigParameter("Failed to parse state file {}: {}", filePath.string(), e.what());
    }

    if (!jsonObject.contains("query_id"))
    {
        throw InvalidConfigParameter("State file {} is missing query_id field", filePath.string());
    }

    auto queryId = QueryId(jsonObject["query_id"].get<uint64_t>());
    NES_DEBUG("Loaded query state from: {}", filePath.string());
    return queryId;
}

void QueryStateBackend::remove(PersistedQueryId persistedId)
{
    auto filePath = getQueryFilePath(persistedId.queryId);

    std::error_code ec;
    std::filesystem::remove(filePath, ec);

    if (ec)
    {
        NES_WARNING("Failed to remove state file {}: {}", filePath.string(), ec.message());
    }
    else
    {
        NES_DEBUG("Removed query state file: {}", filePath.string());
    }
}

}
