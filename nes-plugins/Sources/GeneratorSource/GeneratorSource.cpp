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
#include <exception>
#include <iostream>
#include <memory>
#include <stop_token>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <Configurations/Descriptor.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Util/Logger/Logger.hpp>
#include <Generator.hpp>
#include <GeneratorDataRegistry.hpp>
#include <SourceRegistry.hpp>
#include <SourceValidationRegistry.hpp>

namespace NES::Sources
{

GeneratorSource::GeneratorSource(const SourceDescriptor& sourceDescriptor)
    : seed(sourceDescriptor.getFromConfig(ConfigParametersGenerator::SEED))
    , maxRuntime(sourceDescriptor.getFromConfig(ConfigParametersGenerator::MAX_RUNTIME_MS))
    , generatorSchemaRaw(sourceDescriptor.getFromConfig(ConfigParametersGenerator::GENERATOR_SCHEMA))
    , generator(
          seed,
          sourceDescriptor.getFromConfig(ConfigParametersGenerator::SEQUENCE_STOPS_GENERATOR),
          sourceDescriptor.getFromConfig(ConfigParametersGenerator::GENERATOR_SCHEMA))

{
    NES_TRACE("Init GeneratorSource.")
}

void GeneratorSource::open(std::shared_ptr<Memory::AbstractBufferProvider>)
{
    this->generatorStartTime = std::chrono::system_clock::now();
    NES_TRACE("Opening GeneratorSource.");
}

void GeneratorSource::close()
{
    NES_TRACE("Closing GeneratorSource.");
}

Source::FillTupleBufferResult GeneratorSource::fillTupleBuffer(NES::Memory::TupleBuffer& tupleBuffer, const std::stop_token& stopToken)
{
    NES_TRACE("Filling buffer in GeneratorSource.");
    try
    {
        size_t writtenBytes = 0;
        const auto elapsedTime
            = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now() - generatorStartTime).count();
        if (maxRuntime >= 0 && elapsedTime >= maxRuntime)
        {
            NES_INFO("Reached max runtime! Stopping Source");
            return FillTupleBufferResult();
        }
        const size_t rawTBSize = tupleBuffer.getBufferSize();
        while (writtenBytes < rawTBSize and not this->generator.shouldStop() and not stopToken.stop_requested())
        {
            auto insertedBytes = tuplesStream.tellp();
            if (!orphanTuples.empty())
            {
                tuplesStream << orphanTuples;
                orphanTuples.clear();
            }
            this->generator.generateTuple(tuplesStream);
            insertedBytes = tuplesStream.tellp() - insertedBytes;
            if (writtenBytes + insertedBytes > rawTBSize)
            {
                tuplesStream.read(tupleBuffer.getBuffer<char>(), writtenBytes);
                generatedBuffers++;
                this->orphanTuples = tuplesStream.str().substr(writtenBytes, tuplesStream.str().length() - writtenBytes);
                tuplesStream.str("");
                return FillTupleBufferResult(writtenBytes);
            }
            writtenBytes += insertedBytes;
        }
        tuplesStream.read(tupleBuffer.getBuffer<char>(), writtenBytes);
        ++generatedBuffers;
        tuplesStream.str("");
        NES_TRACE("Wrote {} bytes", writtenBytes);
        if (writtenBytes == 0)
        {
            return FillTupleBufferResult(); /// End of Stream
        }
        return FillTupleBufferResult(writtenBytes);
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
SourceValidationGeneratedRegistrar::RegisterGeneratorSourceValidation(SourceValidationRegistryArguments sourceConfig)
{
    return GeneratorSource::validateAndFormat(sourceConfig.config);
}

///NOLINTNEXTLINE (performance-unnecessary-value-param)
SourceRegistryReturnType SourceGeneratedRegistrar::RegisterGeneratorSource(SourceRegistryArguments sourceRegistryArguments)
{
    return std::make_unique<GeneratorSource>(sourceRegistryArguments.sourceDescriptor);
}

///NOLINTNEXTLINE (performance-unnecessary-value-param)
GeneratorDataRegistryReturnType
GeneratorDataGeneratedRegistrar::RegisterGeneratorGeneratorData(GeneratorDataRegistryArguments systestAdaptorArguments)
{
    return systestAdaptorArguments.physicalSourceConfig;
}
}
