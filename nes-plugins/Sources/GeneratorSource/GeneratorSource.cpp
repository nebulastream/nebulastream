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

#include <GeneratorSource.hpp>

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <expected>
#include <iostream>
#include <memory>
#include <stop_token>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <utility>
#include <Configurations/Descriptor.hpp>
#include <Configurations/Enums/EnumWrapper.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/Source.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Util/Logger/Logger.hpp>
#include <fmt/ranges.h>
#include <magic_enum/magic_enum.hpp>
#include <ErrorHandling.hpp>
#include <FixedGeneratorRate.hpp>
#include <Generator.hpp>
#include <GeneratorFields.hpp>
#include <GeneratorRate.hpp>
#include <SinusGeneratorRate.hpp>
#include <SourceRegistry.hpp>
#include <SourceValidationRegistry.hpp>

namespace NES
{

GeneratorSource::GeneratorSource(const SourceDescriptor& sourceDescriptor)
    : seed(sourceDescriptor.getFromConfig(ConfigParametersGenerator::SEED))
    , maxRuntime(sourceDescriptor.getFromConfig(ConfigParametersGenerator::MAX_RUNTIME_MS))
    , generatorSchemaRaw(sourceDescriptor.getFromConfig(ConfigParametersGenerator::GENERATOR_SCHEMA))
    , generator(
          seed,
          sourceDescriptor.getFromConfig(ConfigParametersGenerator::SEQUENCE_STOPS_GENERATOR),
          sourceDescriptor.getFromConfig(ConfigParametersGenerator::GENERATOR_SCHEMA))
    , flushInterval(std::chrono::milliseconds{sourceDescriptor.getFromConfig(ConfigParametersGenerator::FLUSH_INTERVAL_MS)})
{
    NES_TRACE("Init GeneratorSource.")
    switch (sourceDescriptor.getFromConfig(ConfigParametersGenerator::GENERATOR_RATE_TYPE))
    {
        case GeneratorRate::Type::FIXED:
            generatorRate
                = std::make_unique<FixedGeneratorRate>(sourceDescriptor.getFromConfig(ConfigParametersGenerator::GENERATOR_RATE_CONFIG));
            break;
        case GeneratorRate::Type::SINUS:
            /// We can assume that the parsing will work, as the config has been validated
            auto [amplitude, frequency] = SinusGeneratorRate::parseAndValidateConfigString(
                                              sourceDescriptor.getFromConfig(ConfigParametersGenerator::GENERATOR_RATE_CONFIG))
                                              .value();
            generatorRate = std::make_unique<SinusGeneratorRate>(frequency, amplitude);
            break;
    }
}

void GeneratorSource::open(std::shared_ptr<AbstractBufferProvider>)
{
    this->generatorStartTime = std::chrono::system_clock::now();
    this->startOfInterval = std::chrono::system_clock::now();
    this->tuplesStream.str("");
    NES_TRACE("Opening GeneratorSource.");
}

void GeneratorSource::close()
{
    auto totalElapsedTime = std::chrono::system_clock::now() - generatorStartTime;
    if (generatedBuffers == 0)
    {
        NES_WARNING("Generated {} buffers in {}. Closing GeneratorSource.", generatedBuffers, totalElapsedTime);
    }
    else
    {
        NES_INFO("Generated {} buffers in {}. Closing GeneratorSource.", generatedBuffers, totalElapsedTime);
    }
}

Source::FillTupleBufferResult GeneratorSource::fillTupleBuffer(TupleBuffer& tupleBuffer, const std::stop_token&)
{
    /// the generator indicated that it will not produce anymore data and the tupleStream is empty.
    if (this->generator.shouldStop() && tuplesStream.tellp() == 0)
    {
        return FillTupleBufferResult::eos();
    }

    try
    {
        const auto elapsedTime
            = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now() - generatorStartTime).count();
        if (maxRuntime >= 0 && elapsedTime >= maxRuntime)
        {
            NES_INFO("Reached max runtime! Stopping Source");
            return FillTupleBufferResult::eos();
        }

        /// Asking the generatorRate how many tuples we should generate for this interval [now, now + flushInterval].
        /// If we receive 0 tuples, we do not return but wait till another interval, as a return value of 0 tuples results in the query being terminated.
        uint64_t numberOfTuplesToGenerate = 0;
        uint64_t noIntervals = 1;
        while (numberOfTuplesToGenerate == 0)
        {
            const auto endOfInterval = startOfInterval + (flushInterval * noIntervals);
            numberOfTuplesToGenerate = generatorRate->calcNumberOfTuplesForInterval(startOfInterval, endOfInterval);
            NES_TRACE("numberOfTuplesToGenerate: {}", numberOfTuplesToGenerate);
            if (numberOfTuplesToGenerate == 0)
            {
                std::this_thread::sleep_for(std::chrono::microseconds{flushInterval});
                ++noIntervals;
            }
        }

        auto currentBuffer = tupleBuffer.getAvailableMemoryArea<std::ostream::char_type>();
        /// Generating the required number of tuples. Any tuples that do not fit into the tuple buffer, the content is persisted in the tupleStream.
        uint64_t curTupleCount = 0;
        size_t writtenBytes = 0;
        while (curTupleCount < numberOfTuplesToGenerate)
        {
            const size_t bytesRead = tuplesStream.readsome(currentBuffer.data(), static_cast<std::streamsize>(currentBuffer.size()));
            writtenBytes += bytesRead;
            currentBuffer = currentBuffer.subspan(bytesRead);
            if (currentBuffer.empty())
            {
                break;
            }

            tuplesStream.str("");
            if (this->generator.shouldStop())
            {
                break;
            }
            this->generator.generateTuple(tuplesStream);
            ++curTupleCount;
            tuplesStream.seekg(0, std::ios::beg);
        }

        ++generatedBuffers;

        /// Calculating how long to sleep. The whole method should take the duration of the flushInterval. If we have some time left, we
        /// sleep for the remaining duration. If there is no time left, we print a warning.
        const auto durationGeneratingTuples
            = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now() - startOfInterval);
        if (durationGeneratingTuples > (flushInterval * noIntervals))
        {
            NES_WARNING(
                "Can not produce all required tuples in the flushInterval of {} as it took us {}",
                (flushInterval * noIntervals),
                durationGeneratingTuples);
        }
        else
        {
            const auto sleepDuration = (flushInterval * noIntervals) - durationGeneratingTuples;
            std::this_thread::sleep_for(sleepDuration);
        }
        this->startOfInterval = std::chrono::system_clock::now();
        return FillTupleBufferResult::withBytes(writtenBytes);
    }
    catch (const std::exception& e)
    {
        NES_ERROR("Failed to fill the TupleBuffer. Error: {}", e.what());
        throw e;
    }
}

std::ostream& GeneratorSource::toString(std::ostream& str) const
{
    str << "\nGeneratorSource(";
    str << "\n\tgenerated buffers: " << this->generatedBuffers;
    str << "\n\tschema: " << this->generatorSchemaRaw;
    str << "\n\tseed: " << this->seed;
    str << ")\n";
    return str;
}

std::expected<void, Exception> GeneratorSource::validateGeneratorRateConfig(const std::string_view configString)
{
    if (SinusGeneratorRate::parseAndValidateConfigString(configString).has_value()
        or FixedGeneratorRate::parseAndValidateConfigString(configString).has_value())
    {
        return {};
    }
    return std::unexpected(InvalidConfigParameter(
        R"(Cannot validate {}: "{}"! Expected "emit_rate <rate>" or "amplitude <amplitude>, frequency <frequency>")",
        ConfigParametersGenerator::GENERATOR_RATE_CONFIG.name,
        configString));
}

namespace
{
/// Produces informative errors for the generator-specific parameters upfront; the generic
/// DescriptorConfig::tryValidateAndFormat afterwards only reports which parameter failed.
std::expected<void, Exception> validateGeneratorSpecificConfig(const std::unordered_map<std::string, std::string>& config)
{
    const auto schema = config.find(ConfigParametersGenerator::GENERATOR_SCHEMA);
    if (auto validSchema = GeneratorFields::validateSchema(schema != config.end() ? std::string_view{schema->second} : std::string_view{});
        not validSchema.has_value())
    {
        return validSchema;
    }
    if (const auto stop = config.find(ConfigParametersGenerator::SEQUENCE_STOPS_GENERATOR);
        stop != config.end() and not EnumWrapper{stop->second}.asEnum<GeneratorStop>().has_value())
    {
        constexpr auto stopBehaviors = magic_enum::enum_names<GeneratorStop>();
        return std::unexpected(InvalidConfigParameter(
            "Cannot validate {}: \"{}\"! Expected one of: {}",
            ConfigParametersGenerator::SEQUENCE_STOPS_GENERATOR.name,
            stop->second,
            fmt::join(stopBehaviors, ", ")));
    }
    if (const auto rateType = config.find(ConfigParametersGenerator::GENERATOR_RATE_TYPE);
        rateType != config.end() and not EnumWrapper{rateType->second}.asEnum<GeneratorRate::Type>().has_value())
    {
        constexpr auto rateTypes = magic_enum::enum_names<GeneratorRate::Type>();
        return std::unexpected(InvalidConfigParameter(
            "Cannot validate {}: \"{}\"! Expected one of: {}",
            ConfigParametersGenerator::GENERATOR_RATE_TYPE.name,
            rateType->second,
            fmt::join(rateTypes, ", ")));
    }
    if (const auto rateConfig = config.find(ConfigParametersGenerator::GENERATOR_RATE_CONFIG); rateConfig != config.end())
    {
        if (auto validRate = GeneratorSource::validateGeneratorRateConfig(rateConfig->second); not validRate.has_value())
        {
            return validRate;
        }
    }
    return {};
}
}

std::expected<DescriptorConfig::Config, Exception> GeneratorSource::validateAndFormat(std::unordered_map<std::string, std::string> config)
{
    return validateGeneratorSpecificConfig(config).and_then(
        [&config] { return DescriptorConfig::tryValidateAndFormat<ConfigParametersGenerator>(std::move(config), NAME); });
}

SourceValidationRegistryReturnType
///NOLINTNEXTLINE (performance-unnecessary-value-param)
RegisterGeneratorSourceValidation(SourceValidationRegistryArguments sourceConfig)
{
    /// The validation itself is non-throwing; the registry interface reports validation errors via
    /// exceptions, so the returned error is thrown at this single, visible boundary.
    auto validatedConfig = GeneratorSource::validateAndFormat(std::move(sourceConfig.config));
    if (not validatedConfig.has_value())
    {
        throw std::move(validatedConfig).error();
    }
    return std::move(validatedConfig).value();
}

///NOLINTNEXTLINE (performance-unnecessary-value-param)
SourceRegistryReturnType SourceGeneratedRegistrar::RegisterGeneratorSource(SourceRegistryArguments sourceRegistryArguments)
{
    return std::make_unique<GeneratorSource>(sourceRegistryArguments.sourceDescriptor);
}
}
