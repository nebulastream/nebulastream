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

#include <TimeControlledSource.hpp>

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <ios>
#include <memory>
#include <ostream>
#include <stop_token>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <utility>
#include <Configurations/Descriptor.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/Source.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Util/Files.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Strings.hpp>
#include <magic_enum/magic_enum.hpp>
#include <ErrorHandling.hpp>
#include <FileDataRegistry.hpp>
#include <InlineDataRegistry.hpp>
#include <SourceRegistry.hpp>
#include <SourceValidationRegistry.hpp>

namespace NES
{

TimeControlledSource::TimeControlledSource(const SourceDescriptor& sourceDescriptor)
    : filePath(sourceDescriptor.getFromConfig(ConfigParametersTimeControlled::FILEPATH))
    , timestampMetric(sourceDescriptor.getFromConfig(ConfigParametersTimeControlled::TIMESTAMP_METRIC))
    , timestampColumnIdx(sourceDescriptor.getFromConfig(ConfigParametersTimeControlled::TIMESTAMP_COLUMN_IDX))
{
    const auto realCSVPath = std::unique_ptr<char, decltype(std::free)*>{realpath(this->filePath.c_str(), nullptr), std::free};
    this->inputFile = std::ifstream(realCSVPath.get(), std::ios::binary);
    if (not this->inputFile)
    {
        throw InvalidConfigParameter("Could not determine absolute pathname: {} - {}", this->filePath.c_str(), getErrorMessageFromERRNO());
    }
    const auto logicalSourceFields = sourceDescriptor.getLogicalSource().getSchema()->getFields();
    if (logicalSourceFields.size() <= this->timestampColumnIdx)
    {
        throw InvalidConfigParameter(
            "Given TimestampColumnIdx {} > number of fields {}!", this->timestampColumnIdx, logicalSourceFields.size());
    }
    if (const auto& timestampField = logicalSourceFields[this->timestampColumnIdx]; not timestampField.dataType.isInteger())
    {
        throw InvalidConfigParameter("Timestamp Column {} needs to an integer type!", timestampField.name);
    }
}

void TimeControlledSource::open(std::shared_ptr<AbstractBufferProvider>)
{
    this->openTime = std::chrono::system_clock::now();
    NES_TRACE("Opening TimeControlledSource");
}

void TimeControlledSource::close(){NES_TRACE("Closing TimeControlledSource")}

Source::FillTupleBufferResult TimeControlledSource::fillTupleBuffer(TupleBuffer& tupleBuffer, const std::stop_token& stopToken)
{
    const auto getScheduledTime = [&](const std::string& row) -> std::chrono::system_clock::time_point
    {
        const auto fields = splitWithStringDelimiter<std::string_view>(row, ",");
        const auto timestamp = from_chars<int64_t>(fields[this->timestampColumnIdx]);
        if (!timestamp)
        {
            throw InvalidConfigParameter("Cannot parse a timestamp from the row {}", row);
        }
        switch (this->timestampMetric)
        {
            case TimestampMetric::MILLISECOND: {
                return openTime + std::chrono::milliseconds{*timestamp};
            }
            case TimestampMetric::SECOND: {
                return openTime + std::chrono::seconds{*timestamp};
            }
            default: {
                INVARIANT(
                    magic_enum::enum_contains<TimestampMetric>(this->timestampMetric),
                    "Unknown case {} for timestamp metric!",
                    static_cast<int>(timestampMetric));
                std::unreachable();
            }
        }
    };

    const auto checkAndUpdateHighestSeenTime = [&](const std::chrono::system_clock::time_point scheduledTime) -> void
    {
        if (this->highestSeenTime != std::chrono::time_point<std::chrono::system_clock>::min() && scheduledTime < this->highestSeenTime)
        {
            const auto highestMs = std::chrono::duration_cast<std::chrono::milliseconds>(this->highestSeenTime - this->openTime).count();
            const auto currentMs = std::chrono::duration_cast<std::chrono::milliseconds>(scheduledTime - this->openTime).count();
            NES_WARNING(
                "Out-of-order timestamp detected in source CSV! Current timestamp: {} ms, "
                "Highest seen timestamp: {} ms. Data is not chronologically ordered.",
                currentMs,
                highestMs);
        }
        if (scheduledTime > this->highestSeenTime)
        {
            this->highestSeenTime = scheduledTime;
        }
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
        constexpr auto checkIntervall = std::chrono::milliseconds(25);
        auto remainingTime = sleepDuration;
        while (remainingTime > std::chrono::milliseconds(0) && !stopToken.stop_requested())
        {
            auto sleepTime = std::min(remainingTime, checkIntervall);
            std::this_thread::sleep_for(sleepTime);
            remainingTime -= sleepTime;
        }
        return !stopToken.stop_requested();
    };

    uint32_t totalBytesWritten = 0;
    if (not orphanedLine.empty())
    {
        NES_INFO("Orphaned line is not empty!")
        const auto scheduledTime = getScheduledTime(orphanedLine);
        checkAndUpdateHighestSeenTime(scheduledTime);
        if (!sleepUntilScheduled(scheduledTime))
        {
            return FillTupleBufferResult::withBytes(0);
        }
        if (!writeLineToTupleBuffer(orphanedLine, 0))
        {
            throw TuplesTooLargeForPipelineBufferSize(
                "Can not write a wellformed tuple with size {} into a tuple buffer of size {}",
                orphanedLine.size(),
                tupleBuffer.getBufferSize());
        }
        totalBytesWritten += orphanedLine.size() + 1;
        orphanedLine.clear();
        NES_INFO("Wrote orphaned line with size {} into tuplebuffer.", totalBytesWritten);
    }

    std::string line;
    while (not stopToken.stop_requested() && std::getline(this->inputFile, line))
    {
        const auto scheduledTime = getScheduledTime(line);
        checkAndUpdateHighestSeenTime(scheduledTime);
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

std::ostream& TimeControlledSource::toString(std::ostream& str) const
{
    str << "\nTimeControlledSource(";
    str << "\n\tFilepath: " << this->filePath;
    str << "\n\ttimestampColumnIdx: " << this->timestampColumnIdx;
    str << "\n\topenTIme: " << this->openTime;
    str << ")\n";
    return str;
}

DescriptorConfig::Config TimeControlledSource::validateAndFormat(std::unordered_map<std::string, std::string> config)
{
    return DescriptorConfig::validateAndFormat<ConfigParametersTimeControlled>(std::move(config), NAME);
}

SourceValidationRegistryReturnType
///NOLINTNEXTLINE (performance-unnecessary-value-param)
RegisterTimeControlledSourceValidation(SourceValidationRegistryArguments sourceConfig)
{
    return TimeControlledSource::validateAndFormat(sourceConfig.config);
}

SourceRegistryReturnType
///NOLINTNEXTLINE (performance-unnecessary-value-param)
SourceGeneratedRegistrar::RegisterTimeControlledSource(SourceRegistryArguments sourceRegistryArguments)
{
    return std::make_unique<TimeControlledSource>(sourceRegistryArguments.sourceDescriptor);
}

InlineDataRegistryReturnType
InlineDataGeneratedRegistrar::RegisterTimeControlledInlineData(InlineDataRegistryArguments systestAdaptorArguments)
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

FileDataRegistryReturnType FileDataGeneratedRegistrar::RegisterTimeControlledFileData(FileDataRegistryArguments systestAdaptorArguments)
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
