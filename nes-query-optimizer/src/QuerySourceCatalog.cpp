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

#include <QuerySourceCatalog.hpp>

#include <expected>
#include <mutex>
#include <unordered_set>
#include <utility>
#include <vector>
#include <Identifiers/Identifier.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/Sources/SourceNameLogicalOperator.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

namespace
{
/// Collects the names of all logical sources that the operator tree reads.
void collectReadSources(const LogicalOperator& op, std::unordered_set<Identifier>& readSources)
{
    if (const auto sourceName = op.tryGetAs<SourceNameLogicalOperator>())
    {
        readSources.insert(sourceName.value()->getLogicalSourceName());
    }
    for (const auto& child : op.getChildren())
    {
        collectReadSources(child, readSources);
    }
}
}

std::expected<void, Exception> QuerySourceCatalog::addQuerySource(const Identifier& logicalSourceName, LogicalOperator fragment)
{
    const std::scoped_lock lock(mutex);

    std::unordered_set<Identifier> readSources;
    collectReadSources(fragment, readSources);
    for (const auto& readSource : readSources)
    {
        if (readSource == logicalSourceName || reaches(readSource, logicalSourceName))
        {
            return std::unexpected{InvalidStatement(
                "The query source for logical source \"{}\" reads \"{}\", which creates a cycle", logicalSourceName, readSource)};
        }
    }

    querySources[logicalSourceName].push_back(std::move(fragment));
    return {};
}

std::vector<LogicalOperator> QuerySourceCatalog::getQuerySources(const Identifier& logicalSourceName) const
{
    const std::scoped_lock lock(mutex);
    if (const auto it = querySources.find(logicalSourceName); it != querySources.end())
    {
        return it->second;
    }
    return {};
}

bool QuerySourceCatalog::reaches(const Identifier& from, const Identifier& target) const
{
    std::unordered_set<Identifier> visited;
    std::vector<Identifier> toVisit{from};
    while (!toVisit.empty())
    {
        const auto current = toVisit.back();
        toVisit.pop_back();
        if (!visited.insert(current).second)
        {
            continue;
        }
        const auto it = querySources.find(current);
        if (it == querySources.end())
        {
            continue;
        }
        std::unordered_set<Identifier> readSources;
        for (const auto& fragment : it->second)
        {
            collectReadSources(fragment, readSources);
        }
        if (readSources.contains(target))
        {
            return true;
        }
        toVisit.insert(toVisit.end(), readSources.begin(), readSources.end());
    }
    return false;
}

}
