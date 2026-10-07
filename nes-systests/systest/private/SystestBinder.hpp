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
#include <utility>
#include <vector>

#include <Config/Config.hpp>
#include <Discovery/TestDiscovery.hpp>
#include <Util/Pointers.hpp>
#include <SystestState.hpp>

namespace NES
{

/// Parses and partitions each test file, rewrites the statements, and compiles the rewritten SQL into optimized plans.
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
