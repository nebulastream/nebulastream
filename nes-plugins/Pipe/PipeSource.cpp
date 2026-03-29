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
#include <PipeSource.hpp>

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstring>
#include <memory>
#include <ostream>
#include <stop_token>
#include <string>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <variant>
#include <Configurations/Descriptor.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Util/Logger/Logger.hpp>
#include <PipeService.hpp>
#include <SourceRegistry.hpp>
#include <SourceValidationRegistry.hpp>

namespace NES
{

PipeSource::PipeSource(const SourceDescriptor& sourceDescriptor)
    : pipeName(sourceDescriptor.getFromConfig(ConfigParametersPipeSource::PIPE_NAME))
    , schema(sourceDescriptor.getLogicalSource().getSchema())
{
}

void PipeSource::open(std::shared_ptr<AbstractBufferProvider>)
{
    NES_INFO("PipeSource: opening for pipe '{}'", pipeName);
    queue = PipeService::instance().registerSource(pipeName, schema);
}

void PipeSource::close()
{
    NES_INFO("PipeSource: closing for pipe '{}'", pipeName);
    if (queue)
    {
        PipeService::instance().unregisterSource(pipeName, queue);
        queue.reset();
    }
}

Source::FillTupleBufferResult PipeSource::fillTupleBuffer(TupleBuffer& tupleBuffer, const std::stop_token& stopToken)
{
    PipeChannelMessage message;
    while (true)
    {
        if (queue->tryReadUntil(std::chrono::steady_clock::now() + std::chrono::milliseconds(100), message))
        {
            auto result = std::visit(
                [&](auto&& msg) -> FillTupleBufferResult
                {
                    using T = std::decay_t<decltype(msg)>;
                    if constexpr (std::is_same_v<T, PipeEoS>)
                    {
                        NES_INFO("PipeSource: received EoS on pipe '{}'", pipeName);
                        return FillTupleBufferResult::eos();
                    }
                    else if constexpr (std::is_same_v<T, PipeError>)
                    {
                        NES_ERROR("PipeSource: received error on pipe '{}': {}", pipeName, msg.message);
                        return FillTupleBufferResult::eos();
                    }
                    else
                    {
                        const TupleBuffer& srcBuffer = msg;
                        /// Copy data bytes from the shared pipe buffer into the fresh buffer
                        const auto srcMem = srcBuffer.getAvailableMemoryArea<std::byte>();
                        auto dstMem = tupleBuffer.getAvailableMemoryArea<std::byte>();
                        const auto bytesToCopy = std::min(srcMem.size_bytes(), dstMem.size_bytes());
                        std::memcpy(dstMem.data(), srcMem.data(), bytesToCopy);

                        /// Preserve metadata from the original buffer: watermark, sequence/chunk structure.
                        /// The SourceThread will set originId (always set regardless of addsMetadata).
                        tupleBuffer.setWatermark(srcBuffer.getWatermark());
                        tupleBuffer.setSequenceNumber(srcBuffer.getSequenceNumber());
                        tupleBuffer.setChunkNumber(srcBuffer.getChunkNumber());
                        tupleBuffer.setLastChunk(srcBuffer.isLastChunk());

                        lastDeliveredWasLastChunk = srcBuffer.isLastChunk();

                        /// Return numberOfTuples as the "byte count". The SourceThread always calls
                        /// setNumberOfTuples(getNumberOfBytes()), overwriting whatever we set.
                        /// By returning it as "bytes", the SourceThread sets numberOfTuples = our value.
                        return FillTupleBufferResult::withBytes(srcBuffer.getNumberOfTuples());
                    }
                },
                message);

            return result;
        }

        /// Queue read timed out — check if we can stop.
        /// Only stop at sequence boundaries to avoid delivering partial sequences to downstream operators.
        /// If we haven't started a sequence (initial state) or finished the last one, safe to stop.
        if (stopToken.stop_requested() && lastDeliveredWasLastChunk)
        {
            NES_INFO("PipeSource: stop requested at sequence boundary on pipe '{}'", pipeName);
            return FillTupleBufferResult::eos();
        }
    }
}

std::ostream& PipeSource::toString(std::ostream& str) const
{
    return str << "PipeSource(pipe_name=" << pipeName << ")";
}

DescriptorConfig::Config PipeSource::validateAndFormat(std::unordered_map<std::string, std::string> config)
{
    return DescriptorConfig::validateAndFormat<ConfigParametersPipeSource>(std::move(config), NAME);
}

SourceValidationRegistryReturnType RegisterPipeSourceValidation(SourceValidationRegistryArguments sourceConfig)
{
    return PipeSource::validateAndFormat(sourceConfig.config);
}

SourceRegistryReturnType SourceGeneratedRegistrar::RegisterPipeSource(SourceRegistryArguments sourceRegistryArguments)
{
    return std::make_unique<PipeSource>(sourceRegistryArguments.sourceDescriptor);
}

}
