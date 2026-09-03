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

#include <StatisticStore/StatisticStoreRegistry.hpp>

#include <memory>
#include <mutex>
#include <string>
#include <StatisticStore/AbstractStatisticStore.hpp>
#include <Statistics/DefaultStatisticStore.hpp>

namespace NES
{

StatisticStoreRegistry& StatisticStoreRegistry::instance()
{
    static StatisticStoreRegistry registry;
    return registry;
}

std::shared_ptr<AbstractStatisticStore> StatisticStoreRegistry::getOrCreate(const std::string& name)
{
    const std::lock_guard lock(mutex);
    if (const auto existing = stores.find(name); existing != stores.end())
    {
        return existing->second;
    }
    auto store = std::make_shared<DefaultStatisticStore>();
    stores.emplace(name, store);
    return store;
}

void StatisticStoreRegistry::clear()
{
    const std::lock_guard lock(mutex);
    stores.clear();
}

}
