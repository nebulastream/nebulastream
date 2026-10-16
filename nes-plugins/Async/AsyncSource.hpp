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
#include <memory>
#include <optional>
#include <ostream>
#include <stop_token>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>
#include <Configurations/Descriptor.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/AsyncFunction.hpp>
#include <Sources/Source.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Util/UUID.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/strand.hpp>
#include <HandoffChannel.hpp>

namespace NES
{

/// Consumer half of an asynchronously executed operator: takes the buffers the `Handoff` sink with the
/// same channel handed off, evaluates the async calls and fills in their result slots.
///
/// The calls run on one event loop shared by all Async sources; the source thread only waits for them.
///
/// The lifting rule reserves a slot per call in the handed-off rows, so input and output rows have
/// the same fields and one input buffer becomes exactly one output buffer. Everything that is not a
/// call result is copied byte for byte, VARSIZED included: the input's child buffers are stored on
/// the output in the same order, so the child indices in the copied fields stay valid.
class AsyncSource final : public Source
{
public:
    static constexpr std::string_view NAME = "Async";

    explicit AsyncSource(const SourceDescriptor& sourceDescriptor);

    FillTupleBufferResult fillTupleBuffer(TupleBuffer& tupleBuffer, const std::stop_token& stopToken) override;
    void open(std::shared_ptr<AbstractBufferProvider> bufferProvider) override;
    void close() override;

    /// Origin id, sequence and chunk number and watermark are taken over from the handed-off buffers.
    [[nodiscard]] bool addsMetadata() const override { return true; }

    static DescriptorConfig::Config validateAndFormat(std::unordered_map<std::string, std::string> config);

    [[nodiscard]] std::ostream& toString(std::ostream& str) const override;

private:
    /// A field copied unchanged from the input row to the output row.
    struct CopiedField
    {
        size_t inputOffset;
        size_t outputOffset;
        size_t size;
    };

    /// The input offset of a VARSIZED field, the index of an earlier call, or else a constant.
    struct BoundArgument
    {
        std::optional<size_t> inputOffset;
        std::optional<size_t> call;
        std::string constant;
    };

    struct BoundCall
    {
        std::vector<BoundArgument> arguments;
        /// Unset for a call whose result only feeds other calls.
        std::optional<size_t> outputOffset;
        /// 0 for calls that read no other call; otherwise one more than the highest level they read.
        size_t level = 0;
        /// One per worker coroutine, so an instance serves one call at a time.
        std::vector<std::unique_ptr<AsyncFunction>> instances;
    };

    std::vector<std::string>
    readArguments(const BoundCall& call, const TupleBuffer& input, size_t row, const std::vector<std::string>& results) const;
    /// Retries a failed call up to `maxRetries` times, unless it failed because the query stops.
    boost::asio::awaitable<std::string> callWithRetries(AsyncFunction& function, std::vector<std::string> arguments) const;
    boost::asio::awaitable<void> processRows(size_t worker, const TupleBuffer& input, TupleBuffer& output, size_t rows, size_t& nextRow);

    std::string channelId;
    size_t channelCapacity;
    size_t maxConcurrency;
    size_t maxRetries;
    size_t inputRowSize = 0;
    size_t outputRowSize = 0;
    std::vector<CopiedField> copiedFields;
    /// Ordered so that every call comes after the calls it reads.
    std::vector<BoundCall> calls;
    size_t levels = 0;

    std::shared_ptr<HandoffChannel> channel;
    std::shared_ptr<AbstractBufferProvider> bufferProvider;
    /// All coroutines of this source run here, so its state is never touched by two loop threads at once.
    boost::asio::strand<boost::asio::io_context::executor_type> strand;
};

/// NOLINTBEGIN(cert-err58-cpp)
struct ConfigParametersAsync
{
    static inline const DescriptorConfig::ConfigParameter<std::string> CHANNEL{
        "CHANNEL",
        std::nullopt,
        [](const std::unordered_map<std::string, std::string>& config) -> std::optional<std::string>
        {
            auto value = DescriptorConfig::tryGet(CHANNEL, config);
            if (value && !stringToUUID(*value))
            {
                return std::nullopt;
            }
            return value;
        }};

    /// JSON list of `AsyncCall`: the field each call writes, the async function's type, and per argument
    /// either the handed-off field it reads ({"field": ...}) or a constant ({"constant": ...}).
    static inline const DescriptorConfig::ConfigParameter<std::string> CALLS{
        "CALLS",
        std::nullopt,
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(CALLS, config); }};

    /// Comma-separated field order of the handed-off rows. They have the same fields as this source's
    /// output, possibly in a different order.
    static inline const DescriptorConfig::ConfigParameter<std::string> INPUT_FIELDS{
        "INPUT_FIELDS",
        std::nullopt,
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(INPUT_FIELDS, config); }};

    /// Rows whose calls are in flight at the same time.
    static inline const DescriptorConfig::ConfigParameter<size_t> MAX_CONCURRENCY{
        "MAX_CONCURRENCY",
        size_t{16},
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(MAX_CONCURRENCY, config); }};

    /// Only used if the source opens before the Handoff sink, which then finds the channel this source created.
    static inline const DescriptorConfig::ConfigParameter<size_t> CHANNEL_CAPACITY{
        "CHANNEL_CAPACITY",
        HandoffChannel::DEFAULT_CAPACITY,
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(CHANNEL_CAPACITY, config); }};

    /// Further attempts of a failed call, the n-th after 100 ms * 2^n.
    static inline const DescriptorConfig::ConfigParameter<size_t> MAX_RETRIES{
        "MAX_RETRIES",
        size_t{0},
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(MAX_RETRIES, config); }};

    static inline std::unordered_map<std::string, DescriptorConfig::ConfigParameterContainer> parameterMap
        = DescriptorConfig::createConfigParameterContainerMap(
            SourceDescriptor::parameterMap, CHANNEL, CALLS, INPUT_FIELDS, MAX_CONCURRENCY, CHANNEL_CAPACITY, MAX_RETRIES);
};
/// NOLINTEND(cert-err58-cpp)

}
