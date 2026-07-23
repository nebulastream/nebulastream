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
#include <optional>
#include <ostream>
#include <stop_token>
#include <string>
#include <string_view>
#include <unordered_map>
#include <Configurations/Descriptor.hpp>
#include <DataTypes/Schema.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/Source.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Util/Logger/Formatter.hpp>
#include <PipeService.hpp>

namespace NES
{

/// A source that reads TupleBuffers from a named pipe. Main-buffer data is copied into the provided TupleBuffer, while child buffers
/// are shared because downstream pipelines do not modify input buffers.
/// The source manages its own metadata: it assigns a unique origin ID, offsets sequence numbers from the first received buffer,
/// and preserves chunk boundaries from the original producer.
class PipeSource final : public Source
{
public:
    static constexpr std::string_view NAME = "Pipe";

    explicit PipeSource(const SourceDescriptor& sourceDescriptor);
    ~PipeSource() override = default;

    PipeSource(const PipeSource&) = delete;
    PipeSource& operator=(const PipeSource&) = delete;
    PipeSource(PipeSource&&) = delete;
    PipeSource& operator=(PipeSource&&) = delete;

    FillTupleBufferResult fillTupleBuffer(TupleBuffer& tupleBuffer, const std::stop_token& stopToken) override;

    void open(std::shared_ptr<AbstractBufferProvider> bufferProvider) override;
    void close() override;

    /// PipeSource manages sequence numbers and chunk metadata itself — it copies them from the
    /// incoming pipe buffer to preserve the original chunking structure.
    /// The SourceThread still sets the correct originId (always set regardless of addsMetadata).
    [[nodiscard]] bool addsMetadata() const override { return true; }

    [[nodiscard]] std::string_view getType() const override { return NAME; }

    static DescriptorConfig::Config validateAndFormat(std::unordered_map<std::string, std::string> config);

    [[nodiscard]] std::ostream& toString(std::ostream& str) const override;

private:
    std::string pipeName;
    size_t queueCapacity;
    std::shared_ptr<const PipeSchema> schema;
    std::shared_ptr<PipeQueue> queue;
    /// Tracks whether the last delivered buffer completed a sequence (lastChunk=true).
    /// Used to ensure stop only occurs at sequence boundaries — no partial sequences.
    bool lastDeliveredWasLastChunk = true;
    std::optional<SequenceNumber::Underlying> sequenceNumberOffset;
};

struct ConfigParametersPipeSource
{
    static inline const DescriptorConfig::ConfigParameter<std::string> PIPE_NAME{
        "PIPE_NAME",
        std::nullopt,
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(PIPE_NAME, config); }};

    static inline const DescriptorConfig::ConfigParameter<size_t> QUEUE_CAPACITY{
        "QUEUE_CAPACITY",
        PipeService::DEFAULT_QUEUE_CAPACITY,
        [](const std::unordered_map<std::string, std::string>& config) -> std::optional<size_t>
        {
            const auto capacity = DescriptorConfig::tryGet(QUEUE_CAPACITY, config);
            return capacity && *capacity > 0 ? capacity : std::nullopt;
        }};

    static inline std::unordered_map<std::string, DescriptorConfig::ConfigParameterContainer> parameterMap
        = DescriptorConfig::createConfigParameterContainerMap(SourceDescriptor::parameterMap, PIPE_NAME, QUEUE_CAPACITY);
};

}

FMT_OSTREAM(NES::PipeSource);
