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

#include <SystestTupleFileSource.hpp>

#include <charconv>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <format>
#include <fstream>
#include <ios>
#include <memory>
#include <optional>
#include <ostream>
#include <stop_token>
#include <string>
#include <unordered_map>
#include <utility>
#include <Configurations/Descriptor.hpp>
#include <Identifiers/Identifier.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/Source.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>
#include <FileDataRegistry.hpp>
#include <InlineDataRegistry.hpp>

namespace NES
{
namespace
{
const auto SystestFilePathParameter = Identifier::parse("FILE_PATH");

std::optional<uint64_t> readCrashFrequency()
{
    const auto* const value = std::getenv(SystestTupleFileSource::CrashEveryNTuplesVariable.data()); /// NOLINT(concurrency-mt-unsafe)
    if (value == nullptr)
    {
        return std::nullopt;
    }
    uint64_t crashEveryNTuples = 0;
    const std::string_view text{value};
    if (const auto [end, error] = std::from_chars(text.data(), text.data() + text.size(), crashEveryNTuples);
        error != std::errc{} or end != text.data() + text.size() or crashEveryNTuples == 0)
    {
        throw InvalidConfigParameter("{} must be a positive number, but is {}", SystestTupleFileSource::CrashEveryNTuplesVariable, text);
    }
    return crashEveryNTuples;
}
}

SystestTupleFileSource::SystestTupleFileSource(const SourceDescriptor& sourceDescriptor)
    : filePath(sourceDescriptor.getFromConfig(ConfigParametersSystestTupleFile::FILEPATH)), crashEveryNTuples(readCrashFrequency())
{
}

void SystestTupleFileSource::open(std::shared_ptr<AbstractBufferProvider>)
{
    inputFile = std::ifstream(filePath, std::ios::binary);
    if (not inputFile)
    {
        throw InvalidConfigParameter("Could not open {}", filePath);
    }
    if (resumeByteOffset > 0 and not inputFile.seekg(static_cast<std::streamoff>(resumeByteOffset)))
    {
        throw InvalidConfigParameter("Cannot resume reading {} at byte {}", filePath, resumeByteOffset);
    }
}

void SystestTupleFileSource::close()
{
    inputFile.close();
}

bool SystestTupleFileSource::resumeAtByteOffset(const uint64_t byteOffset)
{
    resumeByteOffset = byteOffset;
    return true;
}

Source::FillTupleBufferResult SystestTupleFileSource::fillTupleBuffer(TupleBuffer& tupleBuffer, const std::stop_token&)
{
    if (crashEveryNTuples and numberOfReadTuples == *crashEveryNTuples)
    {
        NES_INFO("Crashing after {} tuples, as the systest restarts the worker between tuples", numberOfReadTuples);
        Logger::getInstance()->forceFlush();
        std::_Exit(CrashExitCode);
    }

    std::string tuple;
    if (not std::getline(inputFile, tuple))
    {
        return FillTupleBufferResult::eos();
    }
    if (not inputFile.eof())
    {
        tuple.push_back('\n');
    }
    auto destination = tupleBuffer.getAvailableMemoryArea<char>();
    if (tuple.size() > destination.size())
    {
        throw InvalidConfigParameter(
            "A tuple of {} bytes in {} does not fit into a buffer of {} bytes", tuple.size(), filePath, destination.size());
    }
    std::memcpy(destination.data(), tuple.data(), tuple.size());
    ++numberOfReadTuples;
    return FillTupleBufferResult::withBytes(tuple.size());
}

DescriptorConfig::Config SystestTupleFileSource::validateAndFormat(std::unordered_map<std::string, std::string> config)
{
    return DescriptorConfig::validateAndFormat<ConfigParametersSystestTupleFile>(std::move(config), NAME);
}

std::ostream& SystestTupleFileSource::toString(std::ostream& str) const
{
    return str << std::format("\nSystestTupleFileSource(filepath: {}, numberOfReadTuples: {})", filePath, numberOfReadTuples);
}

InlineDataRegistryReturnType SystestTupleFileSource::provideInlineData(InlineDataRegistryArguments systestAdaptorArguments)
{
    if (systestAdaptorArguments.physicalSourceConfig.sourceConfig.contains(SystestFilePathParameter))
    {
        throw InvalidConfigParameter("Mock SystestTupleFile source cannot use given inline data if a 'file_path' is set");
    }
    systestAdaptorArguments.physicalSourceConfig.sourceConfig.try_emplace(
        SystestFilePathParameter, systestAdaptorArguments.testFilePath.string());
    if (std::ofstream testFile(systestAdaptorArguments.testFilePath); testFile.is_open())
    {
        for (const auto& tuple : systestAdaptorArguments.tuples)
        {
            testFile << tuple << "\n";
        }
        testFile.flush();
        return systestAdaptorArguments.physicalSourceConfig;
    }
    throw TestException("Could not open source file \"{}\"", systestAdaptorArguments.testFilePath);
}

FileDataRegistryReturnType SystestTupleFileSource::provideFileData(FileDataRegistryArguments systestAdaptorArguments)
{
    if (systestAdaptorArguments.physicalSourceConfig.sourceConfig.contains(SystestFilePathParameter))
    {
        throw InvalidConfigParameter("The mock SystestTupleFile source cannot be used if the file_path parameter is already set.");
    }
    systestAdaptorArguments.physicalSourceConfig.sourceConfig.emplace(
        SystestFilePathParameter, systestAdaptorArguments.testFilePath.string());
    return systestAdaptorArguments.physicalSourceConfig;
}

}
