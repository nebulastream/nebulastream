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
#include <ranges>
#include <sstream>
#include <stop_token>
#include <string>
#include <string_view>
#include <unordered_map>
#include <Configurations/Descriptor.hpp>
#include <Configurations/Enums/EnumWrapper.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/Source.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Strings.hpp>
#include <ErrorHandling.hpp>
#include <FixedGeneratorRate.hpp>
#include <Generator.hpp>
#include <GeneratorFields.hpp>
#include <GeneratorRate.hpp>
#include <SinusGeneratorRate.hpp>

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

    size_t fillTupleBuffer(TupleBuffer& tupleBuffer, const std::stop_token& stopToken) override;

    [[nodiscard]] std::ostream& toString(std::ostream& str) const override;

    void open() override;
    void close() override;

    static DescriptorConfig::Config validateAndFormat(std::unordered_map<std::string, std::string> config);

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

    /// if inserting a set of generated tuples into the buffer would overflow it, this string saves them so it can be inserted into the next buffer
    std::string orphanTuples;
};

struct ConfigParametersGenerator
{
    static inline const DescriptorConfig::ConfigParameter<EnumWrapper, GeneratorStop> SEQUENCE_STOPS_GENERATOR{
        "stop_generator_when_sequence_finishes",
        std::nullopt,
        [](const std::unordered_map<std::string, std::string>& config)
        {
            const auto optToken = DescriptorConfig::tryGet(SEQUENCE_STOPS_GENERATOR, config);
            if (not optToken.has_value() || not optToken.value().asEnum<GeneratorStop>().has_value())
            {
                NES_ERROR("Cannot validate stop_generator_when_sequence_finishes: {}!", config.at("stop_generator_when_sequence_finishes"))
                throw InvalidConfigParameter(
                    "Cannot validate stop_generator_when_sequence_finishes: {}!", config.at("stop_generator_when_sequence_finishes"));
            }
            switch (optToken.value().asEnum<GeneratorStop>().value())
            {
                case GeneratorStop::ALL: {
                    return std::optional(EnumWrapper(GeneratorStop::ALL));
                }
                case GeneratorStop::ONE: {
                    return std::optional(EnumWrapper(GeneratorStop::ONE));
                }
                case GeneratorStop::NONE: {
                    return std::optional(EnumWrapper(GeneratorStop::NONE));
                }
                default: {
                    NES_ERROR(
                        "Cannot validate stop_generator_when_sequence_finishes: {}!", config.at("stop_generator_when_sequence_finishes"))
                    throw InvalidConfigParameter(
                        "Cannot validate stop_generator_when_sequence_finishes: {}!", config.at("stop_generator_when_sequence_finishes"));
                }
            }
        }};

    static inline const DescriptorConfig::ConfigParameter<uint32_t> SEED{
        "seed",
        std::chrono::high_resolution_clock::now().time_since_epoch().count(),
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(SEED, config); }};

    static inline const DescriptorConfig::ConfigParameter<EnumWrapper, GeneratorRate::Type> GENERATOR_RATE_TYPE{
        "generator_rate_type",
        EnumWrapper{GeneratorRate::Type::FIXED},
        [](const std::unordered_map<std::string, std::string>& config)
        {
            const auto optToken = DescriptorConfig::tryGet(GENERATOR_RATE_TYPE, config);
            if (not optToken.has_value() || not optToken.value().asEnum<GeneratorRate::Type>().has_value())
            {
                return std::optional<EnumWrapper>();
            }
            switch (optToken.value().asEnum<GeneratorRate::Type>().value())
            {
                case GeneratorRate::Type::FIXED:
                    return std::optional(EnumWrapper(GeneratorRate::Type::FIXED));
                case GeneratorRate::Type::SINUS:
                    return std::optional(EnumWrapper(GeneratorRate::Type::SINUS));
            }
            return std::optional<EnumWrapper>();
        }};

    static inline const DescriptorConfig::ConfigParameter<std::string> GENERATOR_RATE_CONFIG{
        "generator_rate_config",
        "emit_rate 1000",
        [](const std::unordered_map<std::string, std::string>& config)
        {
            const auto optToken = DescriptorConfig::tryGet(GENERATOR_RATE_CONFIG, config);
            if (not optToken.has_value())
            {
                return std::optional<std::string>();
            }
            if (SinusGeneratorRate::parseAndValidateConfigString(optToken.value()).has_value()
                or FixedGeneratorRate::parseAndValidateConfigString(optToken.value()).has_value())
            {
                return DescriptorConfig::tryGet(GENERATOR_RATE_CONFIG, config);
            }
            return std::optional<std::string>();
        }};

    static inline const DescriptorConfig::ConfigParameter<std::string> GENERATOR_SCHEMA{
        "generator_schema",
        {},
        [](const std::unordered_map<std::string, std::string>& config)
        {
            const std::string schema = DescriptorConfig::tryGet(GENERATOR_SCHEMA, config).value_or("");
            if (schema.empty())
            {
                NES_ERROR("Generator schema cannot be empty!")
                throw InvalidConfigParameter("Generator schema cannot be empty!");
            }


            for (const auto lines = splitOnMultipleDelimiters(schema, {',', '\n'}); auto line : lines)
            {
                line = trimWhiteSpaces(line);
                const auto foundIdentifier = line.substr(0, line.find_first_of(' '));
                bool validatorExists = false;
                for (const auto& [identifier, validator] : GeneratorFields::Validators)
                {
                    if (identifier == foundIdentifier)
                    {
                        validator(line);
                        validatorExists = true;
                        break;
                    }
                }
                if (not validatorExists)
                {
                    NES_ERROR("Cannot identify the type of field in \"{}\", does the field have a registered validator?", line);
                    throw InvalidConfigParameter(
                        "Cannot identify the type of field in \"{}\", does the field have a registered validator?", line);
                }
            }
            return DescriptorConfig::tryGet(GENERATOR_SCHEMA, config);
        }};

    static inline const NES::DescriptorConfig::ConfigParameter<uint64_t> FLUSH_INTERVAL_MS{
        "flush_interval_ms",
        10,
        [](const std::unordered_map<std::string, std::string>& config)
        {
            const auto value = DescriptorConfig::tryGet(FLUSH_INTERVAL_MS, config);
            return value;
        }};

    /// @brief config option for setting the max runtime in ms, if set to -1 the source will run till stopped by another thread
    static inline const DescriptorConfig::ConfigParameter<int32_t> MAX_RUNTIME_MS{
        "max_runtime_ms",
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
