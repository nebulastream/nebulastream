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

#include <filesystem>
#include <string>
#include <string_view>
#include <Identifiers/Identifiers.hpp>
#include <DistributedQuery.hpp>
#include <QueryId.hpp>

namespace NES::CLI
{

/// Type-safe wrapper for persisted query IDs
/// Contains the distributed query ID and can be serialized for CLI output
struct PersistedQueryId
{
    DistributedQueryId queryId;

    /// Convert to string for CLI output
    [[nodiscard]] std::string toString() const;

    /// Parse from CLI input
    static PersistedQueryId fromString(std::string_view input);
};

/// Manages persistent storage of query state in XDG-compliant directories
/// Stores mapping from DistributedQueryId to DistributedQuery (containing local query IDs)
class QueryStateBackend
{
public:
    QueryStateBackend();

    /// Store distributed query state, returns persisted query ID for user
    PersistedQueryId store(const DistributedQueryId& distributedQueryId, const DistributedQuery& distributedQuery);

    /// Load distributed query from persistent state
    DistributedQuery load(PersistedQueryId persistedId);

    /// Remove query state file
    void remove(PersistedQueryId persistedId);

private:
    /// Resolve XDG state directory ($XDG_STATE_HOME/nebucli/ or $HOME/.local/state/nebucli/)
    /// Creates directory if it doesn't exist
    [[nodiscard]] std::filesystem::path getStateDirectory();

    /// Get the full path to a query state file
    [[nodiscard]] std::filesystem::path getQueryFilePath(const DistributedQueryId& distributedQueryId);

    /// Cached state directory path
    std::filesystem::path stateDirectory;
};

}
