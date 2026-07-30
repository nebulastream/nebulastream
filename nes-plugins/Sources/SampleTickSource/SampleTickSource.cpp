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

#include <SampleTickSource.hpp>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <format>
#include <ostream>
#include <stop_token>
#include <thread>
#include <unordered_map>
#include <utility>
#include <Configurations/Descriptor.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/Source.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>
#include <SourceRegistry.hpp>
#include <SourceValidationRegistry.hpp>

namespace NES
{

SampleTickSource::SampleTickSource(const SourceDescriptor& sourceDescriptor)
    : slotDurationMs(sourceDescriptor.getFromConfig(ConfigParametersSampleTick::SLOT_DURATION_MS))
    , maxRuntimeMs(sourceDescriptor.getFromConfig(ConfigParametersSampleTick::MAX_RUNTIME_MS))
    , maxTicks(sourceDescriptor.getFromConfig(ConfigParametersSampleTick::MAX_TICKS))
{
    NES_TRACE("Init SampleTickSource with slotDurationMs: {}", slotDurationMs);
}

void SampleTickSource::open(std::shared_ptr<AbstractBufferProvider>)
{
    this->startOfSlot = std::chrono::steady_clock::now();
    this->sourceStartTime = this->startOfSlot;
}

void SampleTickSource::close()
{
    NES_INFO("SampleTickSource emitted {} ticks. Closing.", this->generatedBuffers);
}

Source::FillTupleBufferResult SampleTickSource::fillTupleBuffer(TupleBuffer& tupleBuffer, const std::stop_token&)
{
    if (this->maxRuntimeMs >= 0)
    {
        const auto totalElapsed
            = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - this->sourceStartTime);
        if (totalElapsed.count() >= this->maxRuntimeMs)
        {
            NES_INFO("SampleTickSource reached max runtime of {}ms. Stopping.", this->maxRuntimeMs);
            return FillTupleBufferResult::eos();
        }
    }
    if (this->maxTicks >= 0 && static_cast<int64_t>(this->tickCounter) >= this->maxTicks)
    {
        NES_INFO("SampleTickSource reached max tick count of {}. Stopping.", this->maxTicks);
        return FillTupleBufferResult::eos();
    }

    const auto line = std::format("{}\n", this->tickCounter * this->slotDurationMs + 1);
    auto availableMemory = tupleBuffer.getAvailableMemoryArea<char>();
    PRECONDITION(line.size() <= availableMemory.size(), "SampleTickSource: tuple buffer is too small to hold a single tick record");
    std::ranges::copy(line, availableMemory.begin());

    ++this->tickCounter;
    ++this->generatedBuffers;

    /// Pace ourselves so the NEXT call to fillTupleBuffer() lands roughly 'slotDurationMs' after this one started.
    /// Mirrors GeneratorSource's elapsed-vs-interval check (GeneratorSource.cpp): if we're already behind,
    /// log a warning instead of sleeping a negative duration - this also gives a direct signal of the upper bound
    /// of how small a slot duration this source can realistically sustain.
    const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - this->startOfSlot);
    if (elapsed > std::chrono::milliseconds{this->slotDurationMs})
    {
        NES_WARNING(
            "SampleTickSource could not emit a tick within the configured slot duration of {}ms (took {}).", this->slotDurationMs, elapsed);
    }
    else
    {
        std::this_thread::sleep_for(std::chrono::milliseconds{this->slotDurationMs} - elapsed);
    }
    this->startOfSlot = std::chrono::steady_clock::now();

    return FillTupleBufferResult::withBytes(line.size());
}

std::ostream& SampleTickSource::toString(std::ostream& str) const
{
    str << std::format("\nSampleTickSource(slotDurationMs: {}, generatedBuffers: {})", this->slotDurationMs, this->generatedBuffers);
    return str;
}

DescriptorConfig::Config SampleTickSource::validateAndFormat(std::unordered_map<std::string, std::string> config)
{
    return DescriptorConfig::validateAndFormat<ConfigParametersSampleTick>(std::move(config), NAME);
}

SourceValidationRegistryReturnType RegisterSampleTickSourceValidation(SourceValidationRegistryArguments sourceConfig)
{
    return SampleTickSource::validateAndFormat(std::move(sourceConfig.config));
}

SourceRegistryReturnType SourceGeneratedRegistrar::RegisterSampleTickSource(SourceRegistryArguments sourceRegistryArguments)
{
    return std::make_unique<SampleTickSource>(sourceRegistryArguments.sourceDescriptor);
}

}
