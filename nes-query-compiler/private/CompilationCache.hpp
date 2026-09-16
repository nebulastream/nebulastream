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

#include <cstdint>
#include <optional>
#include <string>
#include <unordered_map>
#include <CompilationCacheConfiguration.hpp>

namespace nautilus::engine
{
class EngineOptions;
}

namespace NES
{
class LogicalPlan;
class QueryExecutionConfiguration;
struct Pipeline;
}

namespace NES::QueryCompilation
{

class CompilationCache final
{
public:
    explicit CompilationCache(CompilationCacheConfiguration configuration);

    [[nodiscard]] bool isEnabled() const;
    void prepareForQuery(const LogicalPlan& optimizedPlan, const QueryExecutionConfiguration& executionConfiguration);
    void configureEngineOptionsForPipeline(nautilus::engine::EngineOptions& options, const Pipeline& pipeline);

private:
    [[nodiscard]] uint64_t getStablePipelineOrdinal(const Pipeline& pipeline);
    [[nodiscard]] std::string createExplicitCacheKey(const Pipeline& pipeline);

    [[nodiscard]] static std::string createHandlerCacheSignature(const Pipeline& pipeline);

    CompilationCacheConfiguration configuration;
    std::optional<std::string> binaryFingerprint;
    std::string cacheKeySeed;
    std::unordered_map<const Pipeline*, uint64_t> pipelineToStableOrdinalMap;
    uint64_t nextStablePipelineOrdinal = 0;
};

}
