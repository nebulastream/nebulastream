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
#include <string>
#include <vector>
#include <Configurations/BaseConfiguration.hpp>
#include <Configurations/Enums/EnumOption.hpp>
#include <Configurations/ScalarOption.hpp>
#include <Configurations/Validation/NumberValidation.hpp>
#include <Nautilus/NautilusBackend.hpp>
#include <Configurations/BaseOption.hpp>

namespace NES::Configurations
{

static constexpr auto DEFAULT_BUFFER_SIZE_BYTES = 4096;
static constexpr auto DEFAULT_PAGE_SIZE = 10240;

class QueryOptimizerConfiguration : public BaseConfiguration
{
public:
    QueryOptimizerConfiguration() = default;
    QueryOptimizerConfiguration(const std::string& name, const std::string& description) : BaseConfiguration(name, description) {};

    /// Sets the dump mode for the query compiler. This setting is only for the nautilus compiler
    EnumOption<QueryCompilation::DumpMode> queryCompilerDumpMode
        = {QUERY_COMPILER_DUMP_MODE,
           QueryCompilation::DumpMode::NONE,
           "If and where to dump query compilation details [NONE|CONSOLE|FILE|FILE_AND_CONSOLE]."};

    StringOption queryCompilerDumpPath = {QUERY_COMPILER_DUMP_PATH, "", "Path to dump query compilation details."};

    EnumOption<CompilationStrategy> compilationStrategy
        = {QUERY_COMPILER_COMPILATION_STRATEGY_CONFIG,
            CompilationStrategy::OPTIMIZE,
           "Optimization strategy for the query compiler [FAST|DEBUG|OPTIMIZE|PROXY_INLINING]."};

    EnumOption<QueryCompilation::NautilusBackend> nautilusBackend
        = {QUERY_COMPILER_NAUTILUS_BACKEND_CONFIG,
           QueryCompilation::NautilusBackend::COMPILER,
           "Nautilus backend for the nautilus query compiler "
           "[COMPILER|INTERPRETER]."};

    UIntOption pageSize
        = {WINDOW_OPERATOR_PAGE_SIZE_CONFIG,
           std::to_string(NES::Configurations::DEFAULT_PAGE_SIZE),
           "Page size of any paged data structure",
           {std::make_shared<NumberValidation>()}};

    UIntOption bufferSize
        = {BUFFERS_SIZE_IN_BYTES_CONFIG,
           std::to_string(NES::Configurations::DEFAULT_BUFFER_SIZE_BYTES),
           "Buffer size in bytes",
           {std::make_shared<NumberValidation>()}};

private:
    std::vector<BaseOption*> getOptions() override
    {
        return {
            &queryCompilerDumpMode,
            &queryCompilerDumpPath,
            &compilationStrategy,
            &nautilusBackend,
            &pageSize,
            &bufferSize
        };
    }
};

}
