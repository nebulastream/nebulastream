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
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <expected>
#include <ios>
#include <memory>
#include <stop_token>
#include <string>
#include <thread>
#include <utility>
#include <variant>
#include <vector>

#include <Configurations/ConfigField.hpp>
#include <Configurations/ConfigValue.hpp>
#include <Configurations/Enums/EnumWrapper.hpp>
#include <Identifiers/Identifier.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/Source.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Optional.hpp>
#include <Util/Strings.hpp>
#include <Util/Variant.hpp>
#include <fmt/ranges.h>
#include <magic_enum/magic_enum.hpp>
#include <ErrorHandling.hpp>
#include <Generator.hpp>
#include <GeneratorFields.hpp>
#include <GeneratorRate.hpp>

namespace NES
{

namespace
{

/// Parsers for the rate config string. The outer optional signals "not this rate type", the inner
/// expected carries parse/validation errors for the matched type.
std::optional<std::expected<FixedGeneratorRateConfig, Exception>> parseValidateFixedRateConfigString(const std::string_view configString)
{
    if (const auto params = splitWithStringDelimiter<std::string_view>(configString, " "); params.size() == 2)
    {
        if (toLowerCase(params[0]) == "emit_rate")
        {
            return optionalToExpected<double, Exception>(from_chars<double>(params[1]), expectedType<double>())
                .transform([](const double rate) { return FixedGeneratorRateConfig{.emitRate = rate}; });
        }
    }
    return {};
}

std::optional<std::expected<SinusGeneratorRateConfig, Exception>> parseValidateSinusRateConfigString(const std::string_view configString)
{
    std::optional<std::expected<double, Exception>> amplitude = {};
    std::optional<std::expected<double, Exception>> frequency = {};

    std::vector<std::string_view> params;

    for (const auto& param : splitOnMultipleDelimiters(configString, {'\n', ','}))
    {
        params.emplace_back(trimWhiteSpaces(param));
    }

    if (params.size() == 2)
    {
        const auto amplitudeParams = splitWithStringDelimiter<std::string_view>(params[0], " ");
        if (toLowerCase(amplitudeParams[0]) == "amplitude")
        {
            amplitude = optionalToExpected<double, Exception>(from_chars<double>(amplitudeParams[1]), expectedType<double>());
        }

        const auto frequencyParams = splitWithStringDelimiter<std::string_view>(params[1], " ");
        if (toLowerCase(frequencyParams[0]) == "frequency")
        {
            frequency = optionalToExpected<double, Exception>(from_chars<double>(frequencyParams[1]), expectedType<double>());
        }
    }

    if ((!amplitude.has_value()) && (!frequency.has_value()))
    {
        return std::nullopt;
    }

    if (amplitude.has_value() != frequency.has_value())
    {
        return std::unexpected{InvalidConfigParameter("Amplitude and frequency must be specified together!")};
    }

    if (amplitude.value().has_value() and frequency.value().has_value())
    {
        return SinusGeneratorRateConfig{.amplitude = amplitude.value().value(), .frequency = frequency.value().value()};
    }
    if (!amplitude.value().has_value())
    {
        return std::unexpected{amplitude.value().error()};
    }
    return std::unexpected{frequency.value().error()};
}

uint64_t calcNumberOfTuplesForInterval(
    const FixedGeneratorRateConfig& rate,
    const std::chrono::time_point<std::chrono::system_clock>& start,
    const std::chrono::time_point<std::chrono::system_clock>& end)
{
    /// As the emit rate is fixed, we have to multiply the interval duration (or size) with the emit rate.
    const auto duration = std::chrono::duration<double>(end - start);
    const auto numberOfTuples = static_cast<uint64_t>(rate.emitRate * static_cast<double>(duration.count()));
    return numberOfTuples;
}

uint64_t calcNumberOfTuplesForInterval(
    const SinusGeneratorRateConfig& rate,
    const std::chrono::time_point<std::chrono::system_clock>& start,
    const std::chrono::time_point<std::chrono::system_clock>& end)
{
    /// To calculate the number of tuples for a non-negative sinus, we take the integral of the sinus from end to start
    /// As the sinus must be non-negative, the sin-function is emit_rate_at_x = amplitude * sin(freq * (x - phaseShift)) + amplitude
    /// Thus, the integral from startTimePoint to endTimePoint is the following:
    auto integralStartEnd = [](const double startTimePoint, const double endTimePoint, const double amplitude, const double frequency)
    {
        const auto firstCos = std::cos(frequency * startTimePoint);
        const auto secondCos = frequency * (startTimePoint - endTimePoint);
        const auto thirdCos = std::cos(endTimePoint * frequency);
        return (amplitude * (firstCos - secondCos - thirdCos)) / (2 * frequency);
    };

    /// Calculating the integral of end --> start, resulting in the required number of tuples
    /// As the interval of start and end might share the same seconds, we need to first cast it to milliseconds and then convert it to a
    /// double. Otherwise, we would have the exact same value for startTimePoint and endTimePoint, resulting in number of tuples to generate.
    const auto startTimePoint = std::chrono::duration<double>(start.time_since_epoch()).count();
    const auto endTimePoint = std::chrono::duration<double>(end.time_since_epoch()).count();
    const auto numberOfTuples = static_cast<uint64_t>(integralStartEnd(startTimePoint, endTimePoint, rate.amplitude, rate.frequency));
    return numberOfTuples;
}

/// Config fields of the generator source, shared by getConfigSchema (declaration) and
/// GeneratorSourceConfig::fromConfig (typed extraction).
/// NOLINTBEGIN(cert-err58-cpp)
const ConfigField<GeneratorStop> SEQUENCE_STOPS_GENERATOR{
    "STOP_GENERATOR_WHEN_SEQUENCE_FINISHES",
    [](const ConfigLiteral& literal)
    {
               return NES::tryGetOr<std::string>(literal, expectedType<std::string>())
                   .and_then(
                       [](std::string&& value) -> std::expected<GeneratorStop, Exception>
                       {
                           auto optToken = EnumWrapper{value}.asEnum<GeneratorStop>();
                           if (not optToken.has_value())
                           {
                               return std::unexpected{
                                   InvalidConfigParameter("Invalid value, must be ALL, ONE or NONE, but was: {}!", value)};
                           }
                           return optToken.value();
                       });
    }};

const ConfigField<int64_t> SEED{
    "SEED",
    [](const ConfigLiteral& config) { return NES::tryGetOr<int64_t>(config, expectedType<int64_t>()); },
    [] { return std::chrono::high_resolution_clock::now().time_since_epoch().count(); }};

static const ConfigField<GeneratorRate::Type> GENERATOR_RATE_TYPE{
    "GENERATOR_RATE_TYPE",
    [](const ConfigLiteral& config)
    {
               return NES::tryGetOr<std::string>(config, expectedType<std::string>())
                   .and_then(
                       [](std::string&& value) -> std::expected<GeneratorRate::Type, Exception>
                       {
                           auto enumOpt = EnumWrapper{value}.asEnum<GeneratorRate::Type>();
                           if (not enumOpt.has_value())
                           {
                               return std::unexpected{InvalidConfigParameter("Invalid value, must be FIXED or SINUS, but was: {}!", value)};
                           }
                           return enumOpt.value();
                       });
    },
    GeneratorRate::Type::FIXED};

static const ConfigField<GeneratorRateVariant> GENERATOR_RATE_CONFIG{
    "GENERATOR_RATE_CONFIG",
    [](const ConfigLiteral& config)
    {
               return NES::tryGetOr<std::string>(config, expectedType<std::string>())
                   .and_then(
                       [](const std::string& value) -> std::expected<GeneratorRateVariant, Exception>
                       {
                           if (const auto sinusGeneratorRate = parseValidateSinusRateConfigString(value))
                           {
                               return sinusGeneratorRate->transform([](const SinusGeneratorRateConfig& rate)
                                                                    { return GeneratorRateVariant{rate}; });
                           }
                           if (const auto fixedGeneratorRate = parseValidateFixedRateConfigString(value))
                           {
                               return fixedGeneratorRate->transform([](const FixedGeneratorRateConfig& rate)
                                                                    { return GeneratorRateVariant{rate}; });
                           }
                           return std::unexpected{
                               InvalidConfigParameter("Invalid value, must be a sinus or fixed generator rate, but was: {}!", value)};
                       });
    },
    GeneratorRateVariant{FixedGeneratorRateConfig{.emitRate = 1000}}};

static const ConfigField<std::string> GENERATOR_SCHEMA{
    "GENERATOR_SCHEMA",
    [](const ConfigLiteral& config)
    {
               return NES::tryGetOr<std::string>(config, expectedType<std::string>())
                   .and_then(
                       [](const std::string& value) -> std::expected<std::string, Exception>
                       {
                           if (value.empty())
                           {
                               return std::unexpected{InvalidConfigParameter("Generator schema cannot be empty!")};
                           }
                           std::vector<std::pair<std::string, Exception>> exceptions;
                           for (const auto lines = splitOnMultipleDelimiters(value, {',', '\n'}); auto line : lines)
                           {
                               line = trimWhiteSpaces(line);
                               const auto foundIdentifier = magic_enum::enum_cast<GeneratorFields::FieldIdentifier>(
                                   NES::toUpperCase(line.substr(0, line.find_first_of(' '))));
                               bool validatorExists = false;
                               for (const auto& [identifier, validator] : GeneratorFields::Validators)
                               {
                                   if (identifier == foundIdentifier)
                                   {
                                       try
                                       {
                                           validator(line);
                                       }
                                       catch (Exception& e)
                                       {
                                           exceptions.emplace_back(line, e);
                                       }
                                       validatorExists = true;
                                       break;
                                   }
                               }
                               if (not validatorExists)
                               {
                                   exceptions.emplace_back(
                                       line,
                                       InvalidConfigParameter(
                                           "Cannot identify the type of field in \"{}\", does the field have a registered validator?",
                                           line));
                               }
                           }
                           if (not exceptions.empty())
                           {
                               return std::unexpected{InvalidConfigParameter("Invalid Generator schema:\n{}", fmt::join(exceptions, ","))};
                           }
                           return value;
                       });
    }};

static const ConfigField<uint64_t> FLUSH_INTERVAL_MS{
    "FLUSH_INTERVAL_MS",
    [](const ConfigLiteral& config)
    { return NES::tryGetOr<int64_t>(config, expectedType<uint64_t>()).and_then(downcastConfigValue<int64_t, uint64_t>); },
    10};

/// Max runtime in ms; if set to -1 the source runs until stopped by another thread.
static const ConfigField<int64_t> MAX_RUNTIME_MS{
    "MAX_RUNTIME_MS", [](const ConfigLiteral& config) { return NES::tryGetOr<int64_t>(config, expectedType<int64_t>()); }, -1};
/// NOLINTEND(cert-err58-cpp)

}

Schema<QualifiedErasedConfigField, Ordered> GeneratorSource::getConfigSchema()
{
    return createConfigSchema(
        Identifier::parse("GENERATOR_SOURCE"),
        SEED,
        GENERATOR_SCHEMA,
        MAX_RUNTIME_MS,
        SEQUENCE_STOPS_GENERATOR,
        GENERATOR_RATE_TYPE,
        GENERATOR_RATE_CONFIG,
        FLUSH_INTERVAL_MS);
}

std::expected<GeneratorSourceConfig, Exception> GeneratorSourceConfig::fromConfig(const InstantiatedConfig& config)
{
    return GeneratorSourceConfig{
        .seed = static_cast<uint32_t>(config.get(SEED)),
        .maxRuntime = static_cast<int32_t>(config.get(MAX_RUNTIME_MS)),
        .generatorSchemaRaw = config.get(GENERATOR_SCHEMA),
        .stopGeneratorWhenSequenceFinishes = config.get(SEQUENCE_STOPS_GENERATOR),
        .flushInterval = std::chrono::milliseconds{config.get(FLUSH_INTERVAL_MS)},
        .generatorRateConfig = config.get(GENERATOR_RATE_CONFIG)};
}

GeneratorSource::GeneratorSource(const GeneratorSourceConfig& config)
    : config(config), generator(config.seed, config.stopGeneratorWhenSequenceFinishes, config.generatorSchemaRaw)
{
    NES_TRACE("Init GeneratorSource.")
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
        if (config.maxRuntime >= 0 && elapsedTime >= config.maxRuntime)
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
            const auto endOfInterval = startOfInterval + (config.flushInterval * noIntervals);
            numberOfTuplesToGenerate = std::visit(
                [&](const auto& rate) { return calcNumberOfTuplesForInterval(rate, startOfInterval, endOfInterval); },
                config.generatorRateConfig);
            NES_TRACE("numberOfTuplesToGenerate: {}", numberOfTuplesToGenerate);
            if (numberOfTuplesToGenerate == 0)
            {
                std::this_thread::sleep_for(std::chrono::microseconds{config.flushInterval});
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
        if (durationGeneratingTuples > (config.flushInterval * noIntervals))
        {
            NES_WARNING(
                "Can not produce all required tuples in the flushInterval of {} as it took us {}",
                (config.flushInterval * noIntervals),
                durationGeneratingTuples);
        }
        else
        {
            const auto sleepDuration = (config.flushInterval * noIntervals) - durationGeneratingTuples;
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
    str << "\n\tschema: " << this->config.generatorSchemaRaw;
    str << "\n\tseed: " << this->config.seed;
    str << ")\n";
    return str;
}

}
