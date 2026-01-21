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

#include <FileSource.hpp>

#include <algorithm>
#include <cerrno>
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
#include <unordered_map>
#include <utility>
#include <zstd.h>
#include <Configurations/Descriptor.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/Source.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Util/Files.hpp>
#include <ErrorHandling.hpp>
#include <FileDataRegistry.hpp>
#include <InlineDataRegistry.hpp>
#include <SourceRegistry.hpp>
#include <SourceValidationRegistry.hpp>

namespace NES
{

namespace
{
CompressionType parseCompressionType(const std::string& compressionStr)
{
    if (compressionStr.empty() || compressionStr == "none")
    {
        return CompressionType::NONE;
    }
    if (compressionStr == "zstd")
    {
        return CompressionType::ZSTD;
    }
    throw InvalidConfigParameter("Unknown compression type: {}. Supported values: none, zstd", compressionStr);
}
}

FileSource::FileSource(const SourceDescriptor& sourceDescriptor)
    : filePath(sourceDescriptor.getFromConfig(ConfigParametersCSV::FILEPATH))
    , compressionType(parseCompressionType(sourceDescriptor.getFromConfig(ConfigParametersCSV::COMPRESSION)))
{
}

void FileSource::open(std::shared_ptr<AbstractBufferProvider>)
{
    const auto realCSVPath = std::unique_ptr<char, decltype(std::free)*>{realpath(this->filePath.c_str(), nullptr), std::free};
    this->inputFile = std::ifstream(realCSVPath.get(), std::ios::binary);
    if (not this->inputFile)
    {
        throw InvalidConfigParameter("Could not determine absolute pathname: {} - {}", this->filePath.c_str(), getErrorMessageFromERRNO());
    }

    if (this->compressionType == CompressionType::ZSTD)
    {
        this->zstdContext = ZSTD_createDCtx();
        if (this->zstdContext == nullptr)
        {
            throw CannotOpenSource("Failed to create zstd decompression context");
        }
        /// Allocate compressed buffer with size equal to zstd recommended input buffer size
        this->compressedBuffer.resize(ZSTD_DStreamInSize());
        this->compressedBufferPos = 0;
        this->compressedBufferSize = 0;
        this->reachedEof = false;
    }
}

void FileSource::close()
{
    this->inputFile.close();
    if (this->zstdContext != nullptr)
    {
        ZSTD_freeDCtx(this->zstdContext);
        this->zstdContext = nullptr;
    }
}

void FileSource::refillCompressedBuffer()
{
    if (this->reachedEof)
    {
        return;
    }
    this->inputFile.read(this->compressedBuffer.data(), static_cast<std::streamsize>(this->compressedBuffer.size()));
    this->compressedBufferSize = static_cast<size_t>(this->inputFile.gcount());
    this->compressedBufferPos = 0;
    if (this->compressedBufferSize == 0)
    {
        this->reachedEof = true;
    }
}

Source::FillTupleBufferResult FileSource::fillTupleBuffer(TupleBuffer& tupleBuffer, const std::stop_token&)
{
    if (this->compressionType == CompressionType::NONE)
    {
        /// Original uncompressed path
        this->inputFile.read(
            tupleBuffer.getAvailableMemoryArea<std::istream::char_type>().data(),
            static_cast<std::streamsize>(tupleBuffer.getBufferSize()));
        const auto numBytesRead = this->inputFile.gcount();
        this->totalNumBytesRead += numBytesRead;
        if (numBytesRead == 0)
        {
            return FillTupleBufferResult::eos();
        }
        return FillTupleBufferResult::withBytes(numBytesRead);
    }

    /// Zstd decompression path
    auto outputBuffer = tupleBuffer.getAvailableMemoryArea<char>();
    size_t totalDecompressed = 0;

    while (totalDecompressed < outputBuffer.size())
    {
        /// Refill compressed buffer if empty
        if (this->compressedBufferPos >= this->compressedBufferSize)
        {
            refillCompressedBuffer();
            if (this->compressedBufferSize == 0)
            {
                /// No more compressed data available
                break;
            }
        }

        ZSTD_inBuffer input = {
            this->compressedBuffer.data() + this->compressedBufferPos,
            this->compressedBufferSize - this->compressedBufferPos,
            0};

        ZSTD_outBuffer output = {outputBuffer.data() + totalDecompressed, outputBuffer.size() - totalDecompressed, 0};

        const size_t ret = ZSTD_decompressStream(this->zstdContext, &output, &input);
        if (ZSTD_isError(ret))
        {
            throw CannotFormatSourceData("Zstd decompression error: {}", ZSTD_getErrorName(ret));
        }

        this->compressedBufferPos += input.pos;
        totalDecompressed += output.pos;

        /// If output buffer is full or no progress was made (need more input), break
        if (output.pos == 0 && input.pos == 0 && this->reachedEof)
        {
            break;
        }
    }

    this->totalNumBytesRead += totalDecompressed;
    if (totalDecompressed == 0)
    {
        return FillTupleBufferResult::eos();
    }
    return FillTupleBufferResult::withBytes(totalDecompressed);
}

DescriptorConfig::Config FileSource::validateAndFormat(std::unordered_map<std::string, std::string> config)
{
    return DescriptorConfig::validateAndFormat<ConfigParametersCSV>(std::move(config), NAME);
}

std::ostream& FileSource::toString(std::ostream& str) const
{
    const char* compressionStr = this->compressionType == CompressionType::ZSTD ? "zstd" : "none";
    str << std::format(
        "\nFileSource(filepath: {}, compression: {}, totalNumBytesRead: {})", this->filePath, compressionStr, this->totalNumBytesRead.load());
    return str;
}

SourceValidationRegistryReturnType RegisterFileSourceValidation(SourceValidationRegistryArguments sourceConfig)
{
    return FileSource::validateAndFormat(std::move(sourceConfig.config));
}

SourceRegistryReturnType SourceGeneratedRegistrar::RegisterFileSource(SourceRegistryArguments sourceRegistryArguments)
{
    return std::make_unique<FileSource>(sourceRegistryArguments.sourceDescriptor);
}

InlineDataRegistryReturnType InlineDataGeneratedRegistrar::RegisterFileInlineData(InlineDataRegistryArguments systestAdaptorArguments)
{
    if (systestAdaptorArguments.physicalSourceConfig.sourceConfig.contains(std::string(SYSTEST_FILE_PATH_PARAMETER)))
    {
        throw InvalidConfigParameter("Mock FileSource cannot use given inline data if a 'file_path' is set");
    }

    systestAdaptorArguments.physicalSourceConfig.sourceConfig.try_emplace(
        std::string(SYSTEST_FILE_PATH_PARAMETER), systestAdaptorArguments.testFilePath.string());


    if (std::ofstream testFile(systestAdaptorArguments.testFilePath); testFile.is_open())
    {
        /// Write inline tuples to test file.
        for (const auto& tuple : systestAdaptorArguments.tuples)
        {
            testFile << tuple << "\n";
        }
        testFile.flush();
        return systestAdaptorArguments.physicalSourceConfig;
    }
    throw TestException("Could not open source file \"{}\"", systestAdaptorArguments.testFilePath);
}

FileDataRegistryReturnType FileDataGeneratedRegistrar::RegisterFileFileData(FileDataRegistryArguments systestAdaptorArguments)
{
    if (systestAdaptorArguments.physicalSourceConfig.sourceConfig.contains(std::string(SYSTEST_FILE_PATH_PARAMETER)))
    {
        throw InvalidConfigParameter("The mock file data source cannot be used if the file_path parameter is already set.");
    }

    systestAdaptorArguments.physicalSourceConfig.sourceConfig.emplace(
        std::string(SYSTEST_FILE_PATH_PARAMETER), systestAdaptorArguments.testFilePath.string());

    return systestAdaptorArguments.physicalSourceConfig;
}


}
