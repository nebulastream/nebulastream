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
#include <expected>
#include <memory>
#include <optional>
#include <ostream>
#include <ranges>
#include <sstream>
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
#include <ErrorHandling.hpp>
#include <Generator.hpp>
#include <GeneratorFields.hpp>
#include <GeneratorRate.hpp>

namespace NES
{

class GeneratorSource : public Source
{
public:
    constexpr static std::string_view NAME = "Generator";

    explicit GeneratorSource(const SourceDescriptor& sourceDescriptor);
    ~GeneratorSource() override = default;

    GeneratorSource(const GeneratorSource&) = delete;
    GeneratorSource& operator=(const GeneratorSource&) = delete;
    GeneratorSource(GeneratorSource&&) = delete;
    GeneratorSource& operator=(GeneratorSource&&) = delete;

    FillTupleBufferResult fillTupleBuffer(TupleBuffer& tupleBuffer, const std::stop_token& stopToken) override;

    [[nodiscard]] std::ostream& toString(std::ostream& str) const override;

    void open(std::shared_ptr<AbstractBufferProvider> bufferProvider) override;
    void close() override;

    /// Validates the generator config without throwing: a malformed user config is reported via the returned error.
    [[nodiscard]] static std::expected<DescriptorConfig::Config, Exception>
    validateAndFormat(std::unordered_map<std::string, std::string> config);

    /// A rate config is valid if it parses as either a fixed or a sinus rate.
    [[nodiscard]] static std::expected<void, Exception> validateGeneratorRateConfig(std::string_view configString);

private:
    uint32_t seed;
    int32_t maxRuntime;
    uint64_t generatedTuplesCounter{0};
    uint64_t generatedBuffers{0};
    std::string generatorSchemaRaw;
    std::chrono::time_point<std::chrono::system_clock> generatorStartTime;
    Generator generator;
    std::stringstream tuplesStream;
    std::chrono::time_point<std::chrono::system_clock> startOfInterval;
    std::chrono::milliseconds flushInterval;
    std::unique_ptr<GeneratorRate> generatorRate;
};

struct ConfigParametersGenerator
{
    /// The validation lambdas must not throw on malformed user input: they return nullopt on invalid values,
    /// GeneratorSource::validateAndFormat produces the informative error for the user.
    static inline const DescriptorConfig::ConfigParameter<EnumWrapper, GeneratorStop> SEQUENCE_STOPS_GENERATOR{
        "STOP_GENERATOR_WHEN_SEQUENCE_FINISHES",
        std::nullopt,
        [](const std::unordered_map<std::string, std::string>& config)
        { return DescriptorConfig::tryGet(SEQUENCE_STOPS_GENERATOR, config); }};

    static inline const DescriptorConfig::ConfigParameter<uint32_t> SEED{
        "SEED",
        std::chrono::high_resolution_clock::now().time_since_epoch().count(),
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(SEED, config); }};

    static inline const DescriptorConfig::ConfigParameter<EnumWrapper, GeneratorRate::Type> GENERATOR_RATE_TYPE{
        "GENERATOR_RATE_TYPE",
        EnumWrapper{GeneratorRate::Type::FIXED},
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(GENERATOR_RATE_TYPE, config); }};

    static inline const DescriptorConfig::ConfigParameter<std::string> GENERATOR_RATE_CONFIG{
        "GENERATOR_RATE_CONFIG",
        "emit_rate 1000",
        [](const std::unordered_map<std::string, std::string>& config) -> std::optional<std::string>
        {
            auto configString = DescriptorConfig::tryGet(GENERATOR_RATE_CONFIG, config);
            if (configString.has_value() and GeneratorSource::validateGeneratorRateConfig(configString.value()).has_value())
            {
                return configString;
            }
            return std::nullopt;
        }};

    static inline const DescriptorConfig::ConfigParameter<std::string> GENERATOR_SCHEMA{
        "GENERATOR_SCHEMA",
        {},
        [](const std::unordered_map<std::string, std::string>& config) -> std::optional<std::string>
        {
            auto schema = DescriptorConfig::tryGet(GENERATOR_SCHEMA, config);
            if (schema.has_value() and GeneratorFields::validateSchema(schema.value()).has_value())
            {
                return schema;
            }
            return std::nullopt;
        }};

    static inline const NES::DescriptorConfig::ConfigParameter<uint64_t> FLUSH_INTERVAL_MS{
        "FLUSH_INTERVAL_MS",
        10,
        [](const std::unordered_map<std::string, std::string>& config)
        {
            const auto value = DescriptorConfig::tryGet(FLUSH_INTERVAL_MS, config);
            return value;
        }};

    /// @brief config option for setting the max runtime in ms, if set to -1 the source will run till stopped by another thread
    static inline const DescriptorConfig::ConfigParameter<int32_t> MAX_RUNTIME_MS{
        "MAX_RUNTIME_MS",
        -1,
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(MAX_RUNTIME_MS, config); }};

    static inline std::unordered_map<std::string, DescriptorConfig::ConfigParameterContainer> parameterMap
        = DescriptorConfig::createConfigParameterContainerMap(
            SourceDescriptor::parameterMap,
            SEED,
            GENERATOR_SCHEMA,
            MAX_RUNTIME_MS,
            SEQUENCE_STOPS_GENERATOR,
            GENERATOR_RATE_TYPE,
            GENERATOR_RATE_CONFIG,
            FLUSH_INTERVAL_MS);
};
}
