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
#include <ErrorHandling.hpp>
#include <Generator.hpp>
#include <GeneratorFields.hpp>

namespace NES::Sources
{

class GeneratorSource : public Source
{
public:
    constexpr static std::string_view NAME = "GENERATOR";

    explicit GeneratorSource(const SourceDescriptor& sourceDescriptor);
    ~GeneratorSource() override = default;

    GeneratorSource(const GeneratorSource&) = delete;
    GeneratorSource& operator=(const GeneratorSource&) = delete;
    GeneratorSource(GeneratorSource&&) = delete;
    GeneratorSource& operator=(GeneratorSource&&) = delete;

    size_t
    fillTupleBuffer(NES::Memory::TupleBuffer& tupleBuffer, Memory::AbstractBufferProvider&, const std::stop_token& stopToken) override;

    [[nodiscard]] std::ostream& toString(std::ostream& str) const override;

    void open() override;
    void close() override;

    static NES::Configurations::DescriptorConfig::Config validateAndFormat(std::unordered_map<std::string, std::string> config);

private:
    uint32_t seed;
    int32_t maxRuntime;
    uint64_t generatedBuffers{0};
    std::string generatorSchemaRaw;
    std::chrono::time_point<std::chrono::system_clock> generatorStartTime;
    Generator generator;
    std::stringstream tuplesStream;
    /// if inserting a set of generated tuples into the buffer would overflow it, this string saves them so it can be inserted into the next buffer
    std::string orphanTuples;
};

struct ConfigParametersGenerator
{
    static inline const Configurations::DescriptorConfig::ConfigParameter<Configurations::EnumWrapper, GeneratorStop>
        SEQUENCE_STOPS_GENERATOR{
            "stopGeneratorWhenSequenceFinishes",
            std::nullopt,
            [](const std::unordered_map<std::string, std::string>& config)
            {
                const auto optToken = Configurations::DescriptorConfig::tryGet(SEQUENCE_STOPS_GENERATOR, config);
                if (!optToken.has_value() || !optToken.value().asEnum<GeneratorStop>().has_value())
                {
                    NES_ERROR("Cannot validate stopGeneratorWhenSequenceFinishes: {}!", config.at("stopGeneratorWhenSequenceFinishes"))
                    throw NES::InvalidConfigParameter(
                        "Cannot validate stopGeneratorWhenSequenceFinishes: {}!", config.at("stopGeneratorWhenSequenceFinishes"));
                }
                switch (optToken.value().asEnum<GeneratorStop>().value())
                {
                    case GeneratorStop::ALL: {
                        return std::optional(Configurations::EnumWrapper(GeneratorStop::ALL));
                    }
                    case GeneratorStop::ONE: {
                        return std::optional(Configurations::EnumWrapper(GeneratorStop::ONE));
                    }
                    default: {
                        NES_ERROR("Cannot validate stopGeneratorWhenSequenceFinishes: {}!", config.at("stopGeneratorWhenSequenceFinishes"))
                        throw NES::InvalidConfigParameter(
                            "Cannot validate stopGeneratorWhenSequenceFinishes: {}!", config.at("stopGeneratorWhenSequenceFinishes"));
                    }
                }
            }};

    static inline const Configurations::DescriptorConfig::ConfigParameter<uint32_t> SEED{
        "seed",
        std::chrono::high_resolution_clock::now().time_since_epoch().count(),
        [](const std::unordered_map<std::string, std::string>& config) { return Configurations::DescriptorConfig::tryGet(SEED, config); }};

    static inline const Configurations::DescriptorConfig::ConfigParameter<std::string> GENERATOR_SCHEMA{
        "generatorSchema",
        {},
        [](const std::unordered_map<std::string, std::string>& config)
        {
            const std::string schema = Configurations::DescriptorConfig::tryGet(GENERATOR_SCHEMA, config).value_or("");
            if (schema.empty())
            {
                NES_ERROR("Generator schema cannot be empty!")
                throw NES::InvalidConfigParameter("Generator schema cannot be empty!");
            }
            auto lines = schema | std::ranges::views::split('\n')
                | std::views::transform([](const auto& subView) { return std::string_view(subView); })
                | std::views::filter([](const auto& subView) { return !subView.empty(); });
            for (auto line : lines)
            {
                const auto foundIdentifer = line.substr(0, line.find_first_of(' '));
                bool validatorExists = false;
                for (const auto& [identifier, validator] : GeneratorFields::Validators)
                {
                    if (identifier == foundIdentifer)
                    {
                        validator(line);
                        validatorExists = true;
                        break;
                    }
                }
                if (!validatorExists)
                {
                    NES_ERROR("Cannot identify the type of field in \"{}\", does the field have a registered validator?", line);
                    throw NES::InvalidConfigParameter(
                        "Cannot identify the type of field in \"{}\", does the field have a registered validator?", line);
                }
            }
            return Configurations::DescriptorConfig::tryGet(GENERATOR_SCHEMA, config);
        }};

    /// @brief config option for setting the max runtime in ms, if set to -1 the source will run till stopped by another thread
    static inline const Configurations::DescriptorConfig::ConfigParameter<int32_t> MAX_RUNTIME_MS{
        "maxRuntimeMS",
        -1,
        [](const std::unordered_map<std::string, std::string>& config)
        { return Configurations::DescriptorConfig::tryGet(MAX_RUNTIME_MS, config); }};

    static inline std::unordered_map<std::string, Configurations::DescriptorConfig::ConfigParameterContainer> parameterMap
        = Configurations::DescriptorConfig::createConfigParameterContainerMap(
            SEED, GENERATOR_SCHEMA, MAX_RUNTIME_MS, SEQUENCE_STOPS_GENERATOR);
};

}
