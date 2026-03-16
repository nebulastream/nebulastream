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

#include <filesystem>
#include <format>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <string>
#include <system_error>
#include <utility>

#include <DataTypes/Schema.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>
#include <FileStore.hpp>
#include <FlushPolicy.hpp>
#include <MemoryStore.hpp>
#include <Store.hpp>
#include <StoreTransformationRegistry.hpp>

namespace NES::StoreManager
{

StoreRegistry& StoreRegistry::instance()
{
    static StoreRegistry registry;
    return registry;
}

void StoreRegistry::registerStore(const std::string& storeName, Store store)
{
    const std::unique_lock lock(mutex);
    stores.emplace(storeName, std::move(store));
}

void StoreRegistry::registerDefaultStore(const std::string& storeName, const Schema& schema, const std::string& schemaText)
{
    const std::unique_lock lock(mutex);
    NES_DEBUG("Registering default store with name {} and schema {}", storeName, schemaText);

    const auto storeDir = generateStoreDir(storeName);

    /// Validate that a transformation exists for MemoryStore -> FileStore
    auto transformation = StoreTransformationRegistry::instance().findTransformation("MemoryStore", "FileStore");
    PRECONDITION(transformation.has_value(), "No transformation registered for 'MemoryStore' -> 'FileStore'");

    /// Build the chain bottom-up: FileStore (tail) <- MemoryStore (head)
    auto fileStore
        = makeStore<FileStore>(FileStore::Config{.storeName = storeName, .storeDir = storeDir, .schemaText = schemaText}, schema);

    const FlushPolicy policy{.type = FlushPolicy::Type::SIZE_THRESHOLD};

    auto headStore = makeStore<MemoryStore>(schema, MemoryStore::Config{}, std::move(fileStore), policy);

    stores.emplace(storeName, headStore);
}

std::optional<Store> StoreRegistry::getStore(const std::string& storeName) const
{
    const std::shared_lock lock(mutex);
    NES_DEBUG("Getting store with name {}", storeName);
    auto it = stores.find(storeName);
    if (it != stores.end())
    {
        return it->second;
    }
    return std::nullopt;
}

void StoreRegistry::unregisterStore(const std::string& storeName)
{
    const std::unique_lock lock(mutex);
    NES_DEBUG("Unregistering store with the id {}", storeName);
    stores.erase(storeName);
}

void StoreRegistry::clear()
{
    const std::unique_lock lock(mutex);
    stores.clear();
}

void StoreRegistry::clearAndDeleteFiles()
{
    const std::unique_lock lock(mutex);
    for (auto& [name, store] : stores)
    {
        if (auto fileStore = store.tryGetAs<FileStore>())
        {
            fileStore->getMutable().removeFile();
        }
        store.close();
    }
    if (std::filesystem::exists(STORE_MANAGER_WORKING_DIR))
    {
        for (const auto& entry : std::filesystem::directory_iterator(STORE_MANAGER_WORKING_DIR))
        {
            if (entry.path().extension() == ".bin")
            {
                std::filesystem::remove(entry.path());
            }
        }
    }
    stores.clear();
}

std::string StoreRegistry::generateStoreDir(const std::string& storeName)
{
    const std::string storeDir = std::format("{}{}", STORE_MANAGER_WORKING_DIR, storeName);
    std::error_code err;
    std::filesystem::create_directories(storeDir, err);
    if (err)
    {
        throw StoreManagerInitFailure("Could not create " + storeName + " directory: " + err.message());
    }
    return storeDir;
}

}
