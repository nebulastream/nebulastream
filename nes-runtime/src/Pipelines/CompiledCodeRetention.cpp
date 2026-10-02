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

#include <Pipelines/CompiledCodeRetention.hpp>

#include <memory>
#include <mutex>
#include <unordered_map>
#include <utility>
#include <vector>
#include <QueryId.hpp>

namespace NES
{

namespace
{
struct RetainedCode
{
    std::mutex mutex;
    std::unordered_map<QueryId, std::vector<std::shared_ptr<void>>> byQuery;
};

/// A function-local static: it is created on the first retain(), after nautilus' JIT symbol registry, so it is destroyed before it.
RetainedCode& retainedCode()
{
    static RetainedCode retained;
    return retained;
}
}

void CompiledCodeRetention::retain(const QueryId& queryId, std::shared_ptr<void> compiledCode)
{
    auto& retained = retainedCode();
    const std::scoped_lock lock(retained.mutex);
    retained.byQuery[queryId].push_back(std::move(compiledCode));
}

void CompiledCodeRetention::release(const QueryId& queryId)
{
    std::vector<std::shared_ptr<void>> released;
    {
        auto& retained = retainedCode();
        const std::scoped_lock lock(retained.mutex);
        if (const auto it = retained.byQuery.find(queryId); it != retained.byQuery.end())
        {
            released = std::move(it->second);
            retained.byQuery.erase(it);
        }
    }
    /// The code is freed here, outside the lock.
}

}
