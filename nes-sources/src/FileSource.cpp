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

FileSource::FileSource(const SourceDescriptor& sourceDescriptor)
    : filePath(sourceDescriptor.getFromConfig(ConfigParametersCSV::FILEPATH)), hasTimestampColumn(false), timestampColunmIdx(-1)
{
    const auto realCSVPath = std::unique_ptr<char, decltype(std::free)*>{realpath(this->filePath.c_str(), nullptr), std::free};
    this->inputFile = std::ifstream(realCSVPath.get(), std::ios::binary);
    if (not this->inputFile)
    {
        throw InvalidConfigParameter("Could not determine absolute pathname: {} - {}", this->filePath.c_str(), getErrorMessageFromERRNO());
    }

    const auto logicalSourceFieldNames = sourceDescriptor.getLogicalSource().getSchema()->getFieldNames();
    const auto it = std::ranges::find_if(logicalSourceFieldNames, [](const std::string& name)
    {
        return name.ends_with("TIMESTAMP_SEC");
    });
    if (it != logicalSourceFieldNames.end())
    {
        this->timestampColunmIdx = std::distance(logicalSourceFieldNames.begin(), it);
        this->hasTimestampColumn = true;
    }

    this->openTime = std::chrono::system_clock::now();
}

void FileSource::close()
{
    this->inputFile.close();
}

Source::FillTupleBufferResult FileSource::fillTupleBuffer(TupleBuffer& tupleBuffer, const std::stop_token& stopToken)
{
    if (not hasTimestampColumn)
    {
        this->inputFile.read(
            tupleBuffer.getAvailableMemoryArea<std::istream::char_type>().data(), static_cast<std::streamsize>(tupleBuffer.getBufferSize()));
        const auto numBytesRead = this->inputFile.gcount();
        this->totalNumBytesRead += numBytesRead;
        if (numBytesRead == 0)
        {
            return FillTupleBufferResult::eos();
        }
        return FillTupleBufferResult::withBytes(numBytesRead);
    }

    const auto getScheduledTime = [&](const std::string& row) -> std::chrono::system_clock::time_point
    {
        INVARIANT(this->timestampColunmIdx >= 0, "Invalid timestampIdx {}", this->timestampColunmIdx);
        const auto fields = splitWithStringDelimiter<std::string_view>(row, ",");
        const auto timestamp = from_chars<long long>(fields[this->timestampColunmIdx]);
        if (!timestamp)
        {
            throw InvalidConfigParameter("Cannot parse a timestamp from the row {}", row);
        }

        return openTime + std::chrono::seconds{*timestamp};
    };

    const auto writeLineToTupleBuffer = [&tupleBuffer](const std::string& line, const size_t offset) -> bool
    {
        if (tupleBuffer.getAvailableMemoryArea<char>().size() < offset + line.size() + 1)
        {
            return false;
        }
        std::memcpy(tupleBuffer.getAvailableMemoryArea<char>().data() + offset, line.data(), line.size());
        tupleBuffer.getAvailableMemoryArea<char>()[offset + line.size()] = '\n';
        return true;
    };

    const auto sleepUntilScheduled = [&stopToken](std::chrono::system_clock::time_point scheduledTime) -> bool
    {
        const auto now = std::chrono::system_clock::now();
        if (scheduledTime <= now)
        {
            return true;
        }
        const auto sleepDuration = std::chrono::duration_cast<std::chrono::milliseconds>(scheduledTime - now);
        NES_INFO("Sleeping for  {} ms until scheduled time {}", sleepDuration.count(), scheduledTime);
        constexpr auto checkIntervall = std::chrono::milliseconds(100);
        auto remaingTime = sleepDuration;
        while (remaingTime > std::chrono::milliseconds(0) && !stopToken.stop_requested())
        {
            auto sleepTime = std::min(remaingTime, checkIntervall);
            std::this_thread::sleep_for(sleepTime);
            remaingTime -= sleepTime;
        }
        return !stopToken.stop_requested();
    };

    auto totalBytesWritten = 0;
    if (not orphanedLine.empty())
    {
        NES_INFO("Orphaned line is not empty!")
        const auto scheduledTime = getScheduledTime(orphanedLine);
        if (!sleepUntilScheduled(scheduledTime))
        {
            return FillTupleBufferResult::withBytes(0);
        }
        if (!writeLineToTupleBuffer(orphanedLine, 0))
        {
            throw InvalidConfigParameter(
                "Can not write a wellformed tuple with size {} into a tuple buffer of size {}",
                orphanedLine.size(), tupleBuffer.getBufferSize()
                );
        }
        totalBytesWritten += orphanedLine.size() + 1;
        orphanedLine.clear();
        NES_INFO("Wrote orphaned line with size {} into tuplebuffer.", totalBytesWritten);
    }

    std::string line;
    while (not stopToken.stop_requested() && std::getline(this->inputFile, line))
    {
        const auto scheduledTime = getScheduledTime(line);
        if (!sleepUntilScheduled(scheduledTime))
        {
            this->orphanedLine = line;
            break;
        }
        if (!writeLineToTupleBuffer(line, totalBytesWritten))
        {
            this->orphanedLine = line;
            return FillTupleBufferResult::withBytes(totalBytesWritten);
        }
        totalBytesWritten += line.size() + 1;
    }
    if (inputFile.eof() && totalBytesWritten == 0)
    {
        return FillTupleBufferResult::eos();
    }
    return FillTupleBufferResult::withBytes(totalBytesWritten);
}

void FileSource::open(std::shared_ptr<AbstractBufferProvider>)
{
    const auto realCSVPath = std::unique_ptr<char, decltype(std::free)*>{realpath(this->filePath.c_str(), nullptr), std::free};
    this->inputFile = std::ifstream(realCSVPath.get(), std::ios::binary);
    if (not this->inputFile)
    {
        throw InvalidConfigParameter("Could not determine absolute pathname: {} - {}", this->filePath.c_str(), getErrorMessageFromERRNO());
    }

}

DescriptorConfig::Config FileSource::validateAndFormat(std::unordered_map<std::string, std::string> config)
{
    return DescriptorConfig::validateAndFormat<ConfigParametersCSV>(std::move(config), NAME);
}

std::ostream& FileSource::toString(std::ostream& str) const
{
    str << std::format("\nFileSource(filepath: {}, totalNumBytesRead: {})", this->filePath, this->totalNumBytesRead.load());
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
