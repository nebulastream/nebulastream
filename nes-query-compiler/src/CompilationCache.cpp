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

#include <CompilationCache.hpp>

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <optional>
#include <sstream>
#include <string>
#include <utility>

#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>
#include <PhysicalPlan.hpp>
#include <Pipeline.hpp>
#include <options.hpp>

namespace NES::QueryCompilation
{
namespace
{
std::optional<std::string> createBinaryFingerprint()
{
    try
    {
        const auto executablePath = std::filesystem::canonical("/proc/self/exe");
        const auto executableSize = std::filesystem::file_size(executablePath);
        const auto lastWriteTime = std::filesystem::last_write_time(executablePath);
        const auto lastWriteNs = std::chrono::duration_cast<std::chrono::nanoseconds>(lastWriteTime.time_since_epoch()).count();

        std::ostringstream fingerprint;
        fingerprint << executablePath << "|" << executableSize << "|" << lastWriteNs;
        return fingerprint.str();
    }
    catch (...)
    {
        return std::nullopt;
    }
}

}

CompilationCache::CompilationCache(Settings settings) : settings(std::move(settings)), binaryFingerprint(createBinaryFingerprint())
{
    if (!this->settings.enabled && !this->settings.cacheDir.empty())
    {
        NES_INFO("Compilation cache directory is set, but compilation cache is disabled.");
    }
    if (this->settings.enabled && !this->settings.cacheDir.empty() && !binaryFingerprint.has_value())
    {
        NES_WARNING("Could not determine binary fingerprint. Disabling compilation cache.");
        this->settings.enabled = false;
    }
}

bool CompilationCache::isEnabled() const
{
    return settings.enabled && !settings.cacheDir.empty() && binaryFingerprint.has_value();
}

void CompilationCache::prepareForQuery(const PhysicalPlan& physicalPlan)
{
    resetPipelineOrdinals();
    if (!isEnabled())
    {
        cacheKeySeed.clear();
        return;
    }
    cacheKeySeed = createCacheKeySeed(physicalPlan);
}

void CompilationCache::resetPipelineOrdinals()
{
    pipelineToStableOrdinalMap.clear();
    nextStablePipelineOrdinal = 0;
}

uint64_t CompilationCache::getStablePipelineOrdinal(const std::shared_ptr<Pipeline>& pipeline)
{
    const Pipeline* pipelinePtr = pipeline.get();
    if (const auto it = pipelineToStableOrdinalMap.find(pipelinePtr); it != pipelineToStableOrdinalMap.end())
    {
        return it->second;
    }
    const auto ordinal = nextStablePipelineOrdinal++;
    pipelineToStableOrdinalMap.emplace(pipelinePtr, ordinal);
    return ordinal;
}

std::string CompilationCache::createExplicitCacheKey(const std::shared_ptr<Pipeline>& pipeline)
{
    PRECONDITION(!cacheKeySeed.empty(), "Compilation cache requires a non-empty optimized logical plan seed");

    const auto pipelineOrdinal = getStablePipelineOrdinal(pipeline);

    std::ostringstream keyBuilder;
    keyBuilder << "nes:auto";
    if (binaryFingerprint.has_value())
    {
        keyBuilder << "|binary=" << *binaryFingerprint;
    }
    keyBuilder << ":q=" << cacheKeySeed;
    keyBuilder << ":o=" << pipelineOrdinal;
    return keyBuilder.str();
}

void CompilationCache::configureEngineOptionsForPipeline(nautilus::engine::Options& options, const std::shared_ptr<Pipeline>& pipeline)
{
    if (!isEnabled())
    {
        return;
    }

    options.setOption("engine.Blob.CacheDir", settings.cacheDir);
    options.setOption("engine.Blob.CacheKey", createExplicitCacheKey(pipeline));
}

std::string CompilationCache::createCacheKeySeed(const PhysicalPlan& physicalPlan)
{
    const auto& compilationCacheSeed = physicalPlan.getCompilationCacheSeed();
    PRECONDITION(
        compilationCacheSeed.has_value() && !compilationCacheSeed->empty(),
        "Compilation cache requires a non-empty optimized logical plan seed attached to the physical plan");
    return *compilationCacheSeed;
}

}
