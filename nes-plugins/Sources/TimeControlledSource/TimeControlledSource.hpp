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
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <fstream>
#include <memory>
#include <optional>
#include <ostream>
#include <stop_token>
#include <string>
#include <string_view>
#include <unordered_map>
#include <Configurations/Descriptor.hpp>
#include <Configurations/Enums/EnumWrapper.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/Source.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>

namespace NES
{
static constexpr std::string_view SYSTEST_FILE_PATH_PARAMETER = "file_path";

class TimeControlledSource : public Source
{
public:
    constexpr static std::string_view NAME = "TimeControlled";

    explicit TimeControlledSource(const SourceDescriptor& sourceDescriptor);
    ~TimeControlledSource() override = default;

    TimeControlledSource(const TimeControlledSource&) = delete;
    TimeControlledSource& operator=(const TimeControlledSource&) = delete;
    TimeControlledSource(TimeControlledSource&&) = delete;
    TimeControlledSource& operator=(TimeControlledSource&&) = delete;

    FillTupleBufferResult fillTupleBuffer(TupleBuffer& tupleBuffer, const std::stop_token& stopToken) override;
    [[nodiscard]] std::ostream& toString(std::ostream& str) const override;

    void open(std::shared_ptr<AbstractBufferProvider> bufferProvider) override;
    void close() override;

    static DescriptorConfig::Config validateAndFormat(std::unordered_map<std::string, std::string> config);

    enum class TimestampMetric : uint8_t
    {
        MILLISECOND,
        SECOND,
    };

private:
    std::ifstream inputFile;
    std::string filePath;
    std::atomic<size_t> totalNumBytesRead;
    TimestampMetric timestampMetric;
    std::string orphanedLine; /// stores read line that did not fit into buffer / not emitted before stop token
    uint32_t timestampColumnIdx;
    std::chrono::time_point<std::chrono::system_clock> openTime;
    std::chrono::time_point<std::chrono::system_clock> highestSeenTime{std::chrono::time_point<std::chrono::system_clock>::min()};
};

struct ConfigParametersTimeControlled
{
    static inline const DescriptorConfig::ConfigParameter<std::string> FILEPATH{
        std::string(SYSTEST_FILE_PATH_PARAMETER),
        std::nullopt,
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(FILEPATH, config); }};

    static inline const DescriptorConfig::ConfigParameter<uint32_t> TIMESTAMP_COLUMN_IDX{
        "timestamp_column_idx",
        -1,
        [](const std::unordered_map<std::string, std::string>& config)
        {
            const auto optTimestampColumnIdx = DescriptorConfig::tryGet(TIMESTAMP_COLUMN_IDX, config);
            if (not optTimestampColumnIdx.has_value())
            {
                throw InvalidConfigParameter("TimeControlledSource requires TIMESTAMP_COLUMN_IDX to be set!");
            }
            return optTimestampColumnIdx.value();
        }};

    static inline const DescriptorConfig::ConfigParameter<EnumWrapper, TimeControlledSource::TimestampMetric> TIMESTAMP_METRIC{
        "timestamp_metric",
        std::optional(EnumWrapper(TimeControlledSource::TimestampMetric::MILLISECOND)),
        [](const std::unordered_map<std::string, std::string>& config)
        {
            const auto optEmissionGranularity = DescriptorConfig::tryGet(TIMESTAMP_METRIC, config);
            if (not optEmissionGranularity.has_value()
                || not optEmissionGranularity.value().asEnum<TimeControlledSource::TimestampMetric>().has_value())
            {
                NES_ERROR("Cannot validate emission_granularity: {}!", config.at("emission_granularity"));
                throw InvalidConfigParameter("Cannot validate emission_granularity: {}!", config.at("emission_granularity"));
            }
            switch (optEmissionGranularity.value().asEnum<TimeControlledSource::TimestampMetric>().value())
            {
                case TimeControlledSource::TimestampMetric::MILLISECOND: {
                    return std::optional(EnumWrapper(TimeControlledSource::TimestampMetric::MILLISECOND));
                }
                case TimeControlledSource::TimestampMetric::SECOND: {
                    return std::optional(EnumWrapper(TimeControlledSource::TimestampMetric::SECOND));
                }
                default: {
                    NES_ERROR("Cannot validate emission_granularity: {}!", config.at("emission_granularity"));
                    throw InvalidConfigParameter("Cannot validate emission_granularity: {}!", config.at("emission_granularity"));
                }
            }
        }};

    static inline std::unordered_map<std::string, DescriptorConfig::ConfigParameterContainer> parameterMap
        = DescriptorConfig::createConfigParameterContainerMap(
            SourceDescriptor::parameterMap, FILEPATH, TIMESTAMP_COLUMN_IDX, TIMESTAMP_METRIC);
};

}
