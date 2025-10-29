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
#include <iostream>
#include <memory>
#include <stop_token>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <Configurations/Descriptor.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Util/Logger/Logger.hpp>
#include <FixedGeneratorRate.hpp>
#include <Generator.hpp>
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

void GeneratorSource::open()
{
    this->generatorStartTime = std::chrono::system_clock::now();
    this->startOfInterval = std::chrono::system_clock::now();
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

size_t GeneratorSource::fillTupleBuffer(TupleBuffer& tupleBuffer, const std::stop_token& stopToken)
{
    NES_DEBUG("Filling buffer in GeneratorSource.");
    try
    {
        const auto elapsedTime
            = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now() - generatorStartTime).count();
        if (maxRuntime >= 0 && elapsedTime >= maxRuntime)
        {
            NES_INFO("Reached max runtime! Stopping Source");
            return 0;
        }

        /// Asking the generatorRate how many tuples we should generate for this interval [now, now + flushInterval].
        /// If we receive 0 tuples, we do not return but wait till another interval, as a return value of 0 tuples results in the query being terminated.
        uint64_t numberOfTuplesToGenerate = 0;
        uint64_t noIntervals = 1;
        while (numberOfTuplesToGenerate == 0)
        {
            const auto endOfInterval = startOfInterval + (flushInterval * noIntervals);
            numberOfTuplesToGenerate = generatorRate->calcNumberOfTuplesForInterval(startOfInterval, endOfInterval);
            NES_DEBUG("numberOfTuplesToGenerate: {}", numberOfTuplesToGenerate);
            if (numberOfTuplesToGenerate == 0)
            {
                std::this_thread::sleep_for(std::chrono::microseconds{flushInterval});
                ++noIntervals;
            }
        }

        /// Generating the required number of tuples. Any tuples that do not fit into the tuple buffer, we add to the orphanTuples and emit
        /// a warning. Also, we first add the orphanTuples to the tuple buffer, before adding newly-created once.
        const size_t rawTBSize = tupleBuffer.getBufferSize();
        uint64_t curTupleCount = 0;
        size_t writtenBytes = 0;
        while (curTupleCount < numberOfTuplesToGenerate and not this->generator.shouldStop() and not stopToken.stop_requested())
        {
            auto insertedBytes = tuplesStream.tellp();
            if (not orphanTuples.empty())
            {
                tuplesStream << orphanTuples;
                orphanTuples.clear();
            }
            this->generator.generateTuple(tuplesStream);
            ++generatedTuplesCounter;
            insertedBytes = tuplesStream.tellp() - insertedBytes;
            if (writtenBytes + insertedBytes > rawTBSize)
            {
                this->orphanTuples = tuplesStream.str().substr(writtenBytes, tuplesStream.str().length() - writtenBytes);
                NES_WARNING("Not all required tuples fit into buffer of size {}. {} are left over", rawTBSize, tuplesStream.str().size());
                break;
            }
            writtenBytes += insertedBytes;
            ++curTupleCount;
        }
        tuplesStream.read(tupleBuffer.getAvailableMemoryArea<std::istream::char_type>().data(), writtenBytes);
        ++generatedBuffers;
        tuplesStream.str("");
        NES_DEBUG("Wrote {} bytes", writtenBytes);

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
        return writtenBytes;
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

DescriptorConfig::Config GeneratorSource::validateAndFormat(std::unordered_map<std::string, std::string> config)
{
    return DescriptorConfig::validateAndFormat<ConfigParametersGenerator>(std::move(config), NAME);
}

SourceValidationRegistryReturnType
///NOLINTNEXTLINE (performance-unnecessary-value-param)
RegisterGeneratorSourceValidation(SourceValidationRegistryArguments sourceConfig)
{
    return GeneratorSource::validateAndFormat(sourceConfig.config);
}

///NOLINTNEXTLINE (performance-unnecessary-value-param)
SourceRegistryReturnType SourceGeneratedRegistrar::RegisterGeneratorSource(SourceRegistryArguments sourceRegistryArguments)
{
    return std::make_unique<GeneratorSource>(sourceRegistryArguments.sourceDescriptor);
}
}
