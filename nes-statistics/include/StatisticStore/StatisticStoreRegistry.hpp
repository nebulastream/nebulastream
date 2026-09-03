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

#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <unordered_map>
#include <StatisticStore/AbstractStatisticStore.hpp>

namespace NES
{

/// Process-global directory of statistic stores, keyed by name.
///
/// A physical operator is built by a lowering rule from nothing but a logical operator and a trait set, so
/// there is no way to inject a worker-owned object into one. This registry closes that gap the same way
/// 'InProcessFeedRegistry' does for source feeds: whoever needs the store looks it up by name.
///
/// The alternative is to thread the store through WorkerConfiguration -> NodeEngineBuilder -> NodeEngine ->
/// SingleNodeWorker -> QueryCompiler -> LowerToPhysicalOperators -> every lowering rule's apply(). That is
/// how it is done on the branch this is ported from, and the source there calls it "not ideal" itself. Going
/// through a registry instead leaves 'AbstractLoweringRule::apply(LogicalOperator)' and every existing
/// lowering rule untouched.
class StatisticStoreRegistry
{
public:
    /// The store every statistic operator uses until a name is carried in the operator config.
    static constexpr std::string_view DEFAULT_STORE_NAME = "default";

    static StatisticStoreRegistry& instance();

    StatisticStoreRegistry(const StatisticStoreRegistry&) = delete;
    StatisticStoreRegistry& operator=(const StatisticStoreRegistry&) = delete;
    StatisticStoreRegistry(StatisticStoreRegistry&&) = delete;
    StatisticStoreRegistry& operator=(StatisticStoreRegistry&&) = delete;

    /// Returns the store registered under 'name', creating an empty DefaultStatisticStore if it does not exist
    /// yet. The writer's handler, the reader's handler and the statistic interface all call this, so none of them has
    /// to be constructed first.
    std::shared_ptr<AbstractStatisticStore> getOrCreate(const std::string& name);

    /// Only used by tests, to keep the stores of one test case out of the next one.
    void clear();

private:
    StatisticStoreRegistry() = default;
    ~StatisticStoreRegistry() = default;

    mutable std::mutex mutex;
    std::unordered_map<std::string, std::shared_ptr<AbstractStatisticStore>> stores;
};

}
