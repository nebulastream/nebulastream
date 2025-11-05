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
#include <filesystem>
#include <utility>
#include <vector>

#include <Util/Pointers.hpp>
#include <SystestConfiguration.hpp>
#include <SystestState.hpp>

namespace NES::Systest
{
/// The systest binder uses the SystestParser to create SystestQuery objects that contain everything to run and validate systest queries.
/// It has to do more than a traditional binder to be able to extract some information required for validation,
/// such as the file output schema of the sinks.
class SystestBinder
{
public:
    /// In the future we might need to inject a factory for the LegacyOptimizer when it requires more setup than the catalogs
    explicit SystestBinder(
        const std::filesystem::path& workingDir,
        const std::filesystem::path& testDataDir,
        const std::filesystem::path& configDir,
        SystestClusterConfiguration clusterConfig);

    /// @return the loaded systest queries and the number of loaded files
    [[nodiscard]] std::pair<std::vector<SystestQuery>, size_t> loadOptimizeQueries(const TestFileMap& discoveredTestFiles);

    ~SystestBinder();

private:
    struct Impl;
    UniquePtr<Impl> impl;
};
}
