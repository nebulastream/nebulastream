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
#include <cstddef>
#include <unordered_set>
#include <utility>
#include <vector>

#include <Config/Config.hpp>
#include <Discovery/TestDiscovery.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Model/RewrittenTest.hpp>
#include <Util/Pointers.hpp>
#include <SystestState.hpp>

namespace NES::Systest
{

/// Drops the cases a run did not select, keeping every one the selection depends on.
/// A case that has to follow the one above it needs that one to still be there, so the chain stays contiguous and the
/// dependency still points at it.
/// A differential block is one case covering both its query numbers, so selecting either number keeps the block.
/// An empty selection selects everything.
void keepSelectedQueries(RewrittenTest& runnable, const std::unordered_set<SystestQueryId>& selected);

/// The plan compiler between the rewriter and the runner.
/// It parses and partitions each test file, rewrites the statements upfront, and compiles the rewritten SQL into
/// optimized plans: the CREATE statements go into the invocation's shared catalogs, and each query becomes a
/// runnable plan that the runner submits and checks.
class SystestBinder
{
public:
    explicit SystestBinder(const SystestConfiguration& config);

    /// @return the loaded systest queries and the number of loaded files
    [[nodiscard]] std::pair<std::vector<SystestQuery>, size_t>
    loadOptimizeQueries(const std::vector<DiscoveredTestFile>& discoveredTestFiles);

    ~SystestBinder();

private:
    struct Impl;
    UniquePtr<Impl> impl;
};
}
