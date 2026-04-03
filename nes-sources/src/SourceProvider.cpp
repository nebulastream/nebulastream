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

#include <Sources/SourceProvider.hpp>

#include <memory>
#include <optional>
#include <string>
#include <utility>

#include <Async/AsyncSourceHandle.hpp>
#include <Blocking/BlockingSourceHandle.hpp>

#include <Decoders/DecoderProvider.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Sources/NetworkSource.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Sources/SourceHandle.hpp>
#include <Util/Common.hpp>
#include <Util/Overloaded.hpp>
#include <Util/Strings.hpp>
#include <magic_enum/magic_enum.hpp>
#include <BackpressureChannel.hpp>
#include <ErrorHandling.hpp>
#include <SourceRegistry.hpp>

namespace NES
{

SourceProvider::SourceProvider(size_t defaultMaxInflightBuffers, std::shared_ptr<AbstractBufferProvider> bufferPool)
    : defaultMaxInflightBuffers(defaultMaxInflightBuffers), bufferPool(std::move(bufferPool))
{
}

std::unique_ptr<SourceHandle> SourceProvider::lower(
    OriginId originId,
    BackpressureListener backpressureListener,
    const SourceDescriptor& sourceDescriptor,
    const bool pinThreads,
    const size_t numberOfIOThreads) const
{
    /// Todo #241: Get the new source identfier from the source descriptor and pass it to SourceHandle.
    auto sourceArguments = SourceRegistryArguments(sourceDescriptor);
    if (auto source = SourceRegistry::instance().create(sourceDescriptor.getSourceType(), sourceArguments))
    {
        /// The source-specific configuration of maxInflightBuffers takes priority.
        /// If not specified (0), we take the NodeEngine-wide configuration.
        const auto maxInflightBuffers = (sourceDescriptor.getFromConfig(SourceDescriptor::MAX_INFLIGHT_BUFFERS) > 0)
            ? sourceDescriptor.getFromConfig(SourceDescriptor::MAX_INFLIGHT_BUFFERS)
            : defaultMaxInflightBuffers;
        const auto inputFormatterThreadingMode = sourceDescriptor.getInputFormatterDescriptor().getThreadingMode();
        SourceRuntimeConfiguration runtimeConfig{
            .inflightBufferLimit = maxInflightBuffers, .inputFormatterThreadingMode = inputFormatterThreadingMode};

        return std::visit(
            Overloaded{
                [&](std::unique_ptr<BlockingSource>&& sourceImpl) -> std::unique_ptr<SourceHandle>
                {
                    /// Create Decoder if provided in source descriptor parameters
                    if (const auto usedCodec = sourceDescriptor.getFromConfig(SourceDescriptor::CODEC); usedCodec != "None")
                    {
                        std::unique_ptr<Decoder> decoder = DecoderProvider::provideDecoder(usedCodec);
                        if (toUpperCase(sourceDescriptor.getSourceType()) == "NETWORK")
                        {
                            /// This is a special case, since the Network Source takes care of decoding directly instead of relying on the source runner to do it.
                            /// This is because the logic behind decoding a NES-internal buffer is vastly different than decoding a raw buffer
                            /// It is stateless and the buffer includes children that must be decoded aswell.
                            /// However, the source runner is not aware of the unique source types.
                            /// Therefore, we create a slightly "hacky" solution for this POC, where the Network Source directly obtains the decoder
                            /// as member and the Source Handle receives no decoder at all.
                            /// The Network Source will then pass the decoder to the TupleBufferBuilder when receiving a tuple buffer.
                            if (NetworkSource* raw = dynamic_cast<NetworkSource*>(sourceImpl.get()))
                            {
                                sourceImpl.release();
                                std::unique_ptr<NetworkSource> networkSource(raw);
                                networkSource->setDecoder(std::move(decoder));
                                return std::make_unique<BlockingSourceHandle>(
                                    std::move(backpressureListener),
                                    std::move(originId),
                                    std::move(runtimeConfig),
                                    bufferPool,
                                    std::move(networkSource),
                                    std::nullopt);
                            }
                            throw UnknownSourceType("Source Name provided was Network, but no Network Source was created");
                        }
                        return std::make_unique<BlockingSourceHandle>(
                            std::move(backpressureListener),
                            std::move(originId),
                            std::move(runtimeConfig),
                            bufferPool,
                            std::move(sourceImpl),
                            std::move(decoder));
                    }
                    return std::make_unique<BlockingSourceHandle>(
                        std::move(backpressureListener),
                        std::move(originId),
                        std::move(runtimeConfig),
                        bufferPool,
                        std::move(sourceImpl),
                        std::nullopt);
                },
                [&](std::unique_ptr<AsyncSource>&& sourceImpl) -> std::unique_ptr<SourceHandle>
                {
                    return std::make_unique<AsyncSourceHandle>(
                        std::move(backpressureListener),
                        std::move(originId),
                        std::move(runtimeConfig),
                        bufferPool,
                        std::move(sourceImpl),
                        pinThreads,
                        numberOfIOThreads);
                }},
            std::move(source.value()));
    }
    // return std::make_unique<SourceHandle>(
    //     std::move(backpressureListener), std::move(originId), std::move(runtimeConfig), bufferPool, std::move(source.value()));
    throw UnknownSourceType("unknown source descriptor type: {}", sourceDescriptor.getSourceType());
}

bool SourceProvider::contains(const std::string& sourceType) const ///NOLINT(readability-convert-member-functions-to-static)
{
    return SourceRegistry::instance().contains(sourceType);
}

}
