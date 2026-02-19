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
#include <memory>
#include <string>
#include <unordered_map>

namespace nautilus::engine
{
class Options;
}

namespace NES
{
class PhysicalPlan;
struct Pipeline;
}

namespace NES::QueryCompilation
{

class CompilationCache final
{
public:
    struct Settings final
    {
        bool enabled = false;
        std::string cacheDir;
    };

    explicit CompilationCache(Settings settings);

    [[nodiscard]] bool isEnabled() const;
    void prepareForQuery(const PhysicalPlan& physicalPlan);
    void resetPipelineOrdinals();

    void configureEngineOptionsForPipeline(nautilus::engine::Options& options, const std::shared_ptr<Pipeline>& pipeline);

private:
    [[nodiscard]] uint64_t getStablePipelineOrdinal(const std::shared_ptr<Pipeline>& pipeline);
    [[nodiscard]] std::string createExplicitCacheKey(const std::shared_ptr<Pipeline>& pipeline);

    [[nodiscard]] static std::string createCacheKeySeed(const PhysicalPlan& physicalPlan);
    [[nodiscard]] static std::string createHandlerCacheSignature(const Pipeline& pipeline);

    Settings settings;
    std::string cacheKeySeed;
    std::unordered_map<const Pipeline*, uint64_t> pipelineToStableOrdinalMap;
    uint64_t nextStablePipelineOrdinal = 0;
};

}
