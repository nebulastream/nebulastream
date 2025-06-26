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
#include <filesystem>
#include <memory>
#include <vector>
#include <experimental/propagate_const>
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
        const std::filesystem::path& workingDir, const std::filesystem::path& testDataDir, const std::filesystem::path& configDir);
    [[nodiscard]] std::vector<SystestQuery> loadOptimizeQueries(const TestFileMap& discoveredTestFiles);
    ~SystestBinder();

private:
    /// PIMPL pattern to hide implementation
    struct Impl;
    std::experimental::propagate_const<std::unique_ptr<Impl>> impl;
};
}
