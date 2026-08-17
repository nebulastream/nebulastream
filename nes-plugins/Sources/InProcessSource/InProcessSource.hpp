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

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <ostream>
#include <stop_token>
#include <string>
#include <string_view>
#include <unordered_map>
#include <Configurations/Descriptor.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/InProcessFeed.hpp>
#include <Sources/Source.hpp>
#include <Sources/SourceDescriptor.hpp>

namespace NES
{

/// Reads text rows that another component of the same worker process pushed into a named 'InProcessFeed',
/// and hands them to the configured InputFormatter. The canonical producer is the worker's
/// 'TaskStatisticListener', which turns query engine events into CSV rows, making engine statistics a
/// stream that NebulaStream can query itself.
///
/// The source never signals end-of-stream while it is running: an empty feed only means that nothing has
/// happened yet, so it keeps waiting until the stop token fires.
class InProcessSource final : public Source
{
public:
    constexpr static std::string_view NAME = "InProcess";

    explicit InProcessSource(const SourceDescriptor& sourceDescriptor);
    ~InProcessSource() override = default;

    InProcessSource(const InProcessSource&) = delete;
    InProcessSource& operator=(const InProcessSource&) = delete;
    InProcessSource(InProcessSource&&) = delete;
    InProcessSource& operator=(InProcessSource&&) = delete;

    FillTupleBufferResult fillTupleBuffer(TupleBuffer& tupleBuffer, const std::stop_token& stopToken) override;

    void open(std::shared_ptr<AbstractBufferProvider> bufferProvider) override;
    void close() override;

    static DescriptorConfig::Config validateAndFormat(std::unordered_map<std::string, std::string> config);

protected:
    [[nodiscard]] std::ostream& toString(std::ostream& str) const override;

private:
    std::string feedName;
    size_t capacity;
    std::chrono::milliseconds flushInterval;
    std::shared_ptr<InProcessFeed> feed;
    /// A row that was popped from the feed but did not fit into the previous TupleBuffer.
    std::optional<std::string> pendingRow;
    size_t emittedRows{0};
};

struct ConfigParametersInProcess
{
    static inline const DescriptorConfig::ConfigParameter<std::string> FEED_NAME{
        "FEED_NAME",
        std::nullopt,
        [](const std::unordered_map<std::string, std::string>& config)
        {
            const auto value = DescriptorConfig::tryGet(FEED_NAME, config);
            return value.has_value() && not value->empty() ? value : std::nullopt;
        }};

    static inline const DescriptorConfig::ConfigParameter<uint64_t> FLUSH_INTERVAL_MS{
        "FLUSH_INTERVAL_MS",
        100,
        [](const std::unordered_map<std::string, std::string>& config)
        {
            const auto value = DescriptorConfig::tryGet(FLUSH_INTERVAL_MS, config);
            return value.has_value() && *value > 0 ? value : std::nullopt;
        }};

    /// Only takes effect if the source is the first one to touch the feed. The producer usually wins that
    /// race, because it creates its feed while the worker starts up, long before any query is submitted.
    static inline const DescriptorConfig::ConfigParameter<uint64_t> CAPACITY{
        "CAPACITY",
        8192,
        [](const std::unordered_map<std::string, std::string>& config)
        {
            const auto value = DescriptorConfig::tryGet(CAPACITY, config);
            return value.has_value() && *value > 0 ? value : std::nullopt;
        }};

    static inline std::unordered_map<std::string, DescriptorConfig::ConfigParameterContainer> parameterMap
        = DescriptorConfig::createConfigParameterContainerMap(SourceDescriptor::parameterMap, FEED_NAME, FLUSH_INTERVAL_MS, CAPACITY);
};

}
