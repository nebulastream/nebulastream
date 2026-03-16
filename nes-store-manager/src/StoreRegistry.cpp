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

#include <StoreRegistry.hpp>

#include <chrono>
#include <filesystem>
#include <iomanip>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <sstream>
#include <string>

namespace NES::StoreManager
{

StoreRegistry& StoreRegistry::instance()
{
    static StoreRegistry registry;
    return registry;
}

std::string StoreRegistry::registerStore(const std::string& storeName)
{
    const std::unique_lock lock(mutex);

    std::filesystem::create_directories(STORE_MANAGER_WORKING_DIR);

    const auto now = std::chrono::system_clock::now();
    const auto timeT = std::chrono::system_clock::to_time_t(now);
    const auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()) % 1000;
    std::ostringstream ts;
    ts << std::put_time(std::localtime(&timeT), "%Y%m%d_%H%M%S") << '_' << std::setfill('0') << std::setw(3) << ms.count();

    std::string filePath = std::string(STORE_MANAGER_WORKING_DIR) + "/replay_" + storeName + "_" + ts.str() + ".bin";

    stores[storeName] = filePath;
    latestStoreId = storeName;
    return filePath;
}

std::optional<std::string> StoreRegistry::getFilePath(const std::string& storeName) const
{
    const std::shared_lock lock(mutex);
    auto it = stores.find(storeName);
    if (it != stores.end())
    {
        return it->second;
    }
    return std::nullopt;
}

std::optional<std::string> StoreRegistry::getLatestStorePath() const
{
    const std::shared_lock lock(mutex);
    if (latestStoreId.empty())
    {
        return std::nullopt;
    }
    auto it = stores.find(latestStoreId);
    if (it != stores.end())
    {
        return it->second;
    }
    return std::nullopt;
}

void StoreRegistry::unregisterStore(const std::string& storeId)
{
    const std::unique_lock lock(mutex);
    stores.erase(storeId);
    if (latestStoreId == storeId)
    {
        latestStoreId.clear();
    }
}

void StoreRegistry::clear()
{
    const std::unique_lock lock(mutex);
    stores.clear();
    latestStoreId.clear();
}

void StoreRegistry::clearAndDeleteFiles()
{
    const std::unique_lock lock(mutex);
    for (const auto& [name, filePath] : stores)
    {
        std::filesystem::remove(filePath);
    }
    stores.clear();
    latestStoreId.clear();
}

} // namespace NES::Replay
