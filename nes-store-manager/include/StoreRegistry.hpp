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

#include <optional>
#include <shared_mutex>
#include <string>
#include <unordered_map>

namespace NES::StoreManager
{

/// Manages multiple replay store files, allowing concurrent TIME_TRAVEL queries
/// to each write to their own file without overwriting each other.
class StoreRegistry
{
public:
    static StoreRegistry&  instance();

    /// Register a new store, generating a unique file path under the given base directory.
    /// Returns the generated file path.
    std::string registerStore(const std::string& storeName);

    /// Look up the file path for a given store name.
    [[nodiscard]] std::optional<std::string> getFilePath(const std::string& storeName) const;

    /// Get the file path of the most recently registered store.
    [[nodiscard]] std::optional<std::string> getLatestStorePath() const;

    /// Remove a store registration.
    void unregisterStore(const std::string& storeName);

    /// Clear all registrations.
    void clear();

    /// Delete all registered store files from disk and clear the registry.
    void clearAndDeleteFiles();

private:
    StoreRegistry() = default;

    mutable std::shared_mutex mutex;
    std::unordered_map<std::string, std::string> stores;
    std::string latestStoreId;
};

} // namespace NES::Replay
