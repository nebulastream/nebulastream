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

#include <cstdlib>
#include <cstring>
#include <format>
#include <fstream>
#include <ios>
#include <istream>
#include <memory>
#include <ostream>
#include <stop_token>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>

#include <Configurations/Descriptor.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/Source.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Util/Files.hpp>
#include <Util/SystestTupleCrashControl.hpp>
#include <ErrorHandling.hpp>
#include <FileDataRegistry.hpp>
#include <InlineDataRegistry.hpp>
#include <SourceRegistry.hpp>
#include <SourceValidationRegistry.hpp>

namespace NES
{

SystestTupleFileSource::SystestTupleFileSource(const SourceDescriptor& sourceDescriptor)
    : filePath(sourceDescriptor.getFromConfig(ConfigParametersSystestTupleFile::FILEPATH))
    , tupleDelimiter(sourceDescriptor.getParserConfig().tupleDelimiter)
{
    SystestTupleCrashControl::initializeFromEnvironment();
    crashEveryNTuples = SystestTupleCrashControl::getCrashFrequency();
    tuplesUntilCrash = crashEveryNTuples;
    if (tupleDelimiter.empty())
    {
        throw InvalidConfigParameter("SystestTupleFile source requires a non-empty tuple delimiter");
    }
}

void SystestTupleFileSource::open(std::shared_ptr<AbstractBufferProvider>)
{
    const auto realPath = std::unique_ptr<char, decltype(std::free)*>{realpath(this->filePath.c_str(), nullptr), std::free};
    this->inputFile = std::ifstream(realPath.get(), std::ios::binary);
    if (not this->inputFile)
    {
        throw InvalidConfigParameter("Could not determine absolute pathname: {} - {}", this->filePath.c_str(), getErrorMessageFromERRNO());
    }

    if (recoveryByteOffset > 0)
    {
        this->inputFile.seekg(static_cast<std::streamoff>(recoveryByteOffset), std::ios::beg);
        if (!this->inputFile)
        {
            throw InvalidConfigParameter("Could not seek to checkpoint recovery offset {} in {}", recoveryByteOffset, this->filePath);
        }
        totalNumBytesRead.store(recoveryByteOffset);
    }
    else
    {
        totalNumBytesRead.store(0);
    }

    crashBeforeNextRead = false;
    tuplesUntilCrash = crashEveryNTuples;
}

void SystestTupleFileSource::close()
{
    this->inputFile.close();
}

void SystestTupleFileSource::setCheckpointRecoveryByteOffset(const uint64_t byteOffset)
{
    recoveryByteOffset = byteOffset;
    totalNumBytesRead.store(byteOffset);
}

Source::FillTupleBufferResult SystestTupleFileSource::fillTupleBuffer(TupleBuffer& tupleBuffer, const std::stop_token&)
{
    SystestTupleCrashControl::beforeTuple();
    if (crashBeforeNextRead)
    {
        SystestTupleCrashControl::afterTuple();
    }

    const auto numBytesRead = fillBufferWithSingleTuple(tupleBuffer);
    totalNumBytesRead += numBytesRead;
    if (numBytesRead == 0)
    {
        return FillTupleBufferResult::eos();
    }

    if (!crashBeforeNextRead)
    {
        if (tuplesUntilCrash > 1)
        {
            --tuplesUntilCrash;
        }
        else
        {
            crashBeforeNextRead = true;
            tuplesUntilCrash = crashEveryNTuples;
        }
    }
    return FillTupleBufferResult::withBytes(numBytesRead);
}

size_t SystestTupleFileSource::fillBufferWithSingleTuple(TupleBuffer& tupleBuffer)
{
    std::string tupleBytes;
    tupleBytes.reserve(tupleBuffer.getBufferSize());

    char nextByte = 0;
    while (inputFile.get(nextByte))
    {
        tupleBytes.push_back(nextByte);
        if (tupleBytes.size() > tupleBuffer.getBufferSize())
        {
            throw InvalidConfigParameter(
                "Tuple in {} exceeded source buffer size {} while restart-between-tuples mode was enabled",
                filePath,
                tupleBuffer.getBufferSize());
        }
        if (tupleBytes.ends_with(tupleDelimiter))
        {
            break;
        }
    }

    if (tupleBytes.empty())
    {
        return 0;
    }

    auto destination = tupleBuffer.getAvailableMemoryArea<std::byte>();
    std::memcpy(destination.data(), tupleBytes.data(), tupleBytes.size());
    return tupleBytes.size();
}

DescriptorConfig::Config SystestTupleFileSource::validateAndFormat(std::unordered_map<std::string, std::string> config)
{
    return DescriptorConfig::validateAndFormat<ConfigParametersSystestTupleFile>(std::move(config), NAME);
}

std::ostream& SystestTupleFileSource::toString(std::ostream& str) const
{
    str << std::format("\nSystestTupleFileSource(filepath: {}, totalNumBytesRead: {})", filePath, totalNumBytesRead.load());
    return str;
}

SourceValidationRegistryReturnType RegisterSystestTupleFileSourceValidation(SourceValidationRegistryArguments sourceConfig)
{
    return SystestTupleFileSource::validateAndFormat(std::move(sourceConfig.config));
}

SourceRegistryReturnType SourceGeneratedRegistrar::RegisterSystestTupleFileSource(SourceRegistryArguments sourceRegistryArguments)
{
    return std::make_unique<SystestTupleFileSource>(sourceRegistryArguments.sourceDescriptor);
}

InlineDataRegistryReturnType
InlineDataGeneratedRegistrar::RegisterSystestTupleFileInlineData(InlineDataRegistryArguments systestAdaptorArguments)
{
    if (systestAdaptorArguments.physicalSourceConfig.sourceConfig.contains(std::string(SystestTupleFileSource::FILE_PATH_PARAMETER)))
    {
        throw InvalidConfigParameter("Mock SystestTupleFile source cannot use given inline data if a 'file_path' is set");
    }

    systestAdaptorArguments.physicalSourceConfig.sourceConfig.try_emplace(
        std::string(SystestTupleFileSource::FILE_PATH_PARAMETER), systestAdaptorArguments.testFilePath.string());

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

FileDataRegistryReturnType FileDataGeneratedRegistrar::RegisterSystestTupleFileFileData(FileDataRegistryArguments systestAdaptorArguments)
{
    if (systestAdaptorArguments.physicalSourceConfig.sourceConfig.contains(std::string(SystestTupleFileSource::FILE_PATH_PARAMETER)))
    {
        throw InvalidConfigParameter("The mock SystestTupleFile data source cannot be used if the file_path parameter is already set.");
    }

    systestAdaptorArguments.physicalSourceConfig.sourceConfig.emplace(
        std::string(SystestTupleFileSource::FILE_PATH_PARAMETER), systestAdaptorArguments.testFilePath.string());

    return systestAdaptorArguments.physicalSourceConfig;
}

}
