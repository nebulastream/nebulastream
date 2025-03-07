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

#include <Execution/Operators/SliceStore/MemoryController/MemoryController.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES::Runtime::Execution
{

MemoryController::MemoryController(const MemoryController& other) : fileWriters(other.fileWriters), fileReaders(other.fileReaders)
{
}

MemoryController::MemoryController(MemoryController&& other) noexcept
    : fileWriters(std::move(other.fileWriters)), fileReaders(std::move(other.fileReaders))
{
}

MemoryController& MemoryController::operator=(const MemoryController& other)
{
    if (this == &other)
    {
        return *this;
    }
    fileWriters = other.fileWriters;
    fileReaders = other.fileReaders;
    return *this;
}

MemoryController& MemoryController::operator=(MemoryController&& other) noexcept
{
    if (this == &other)
    {
        return *this;
    }
    fileWriters = std::move(other.fileWriters);
    fileReaders = std::move(other.fileReaders);
    return *this;
}

std::optional<std::shared_ptr<FileWriter>>
MemoryController::getLeftFileWriter(const PipelineId pipelineId, const SliceEnd sliceEnd, const WorkerThreadId threadId)
{
    std::stringstream ss;
    ss << "left_" << pipelineId.getRawValue() << "_" << sliceEnd.getRawValue() << "_" << threadId.getRawValue() << ".dat";
    return getFileWriterFromMap(ss.str());
}

std::optional<std::shared_ptr<FileWriter>>
MemoryController::getRightFileWriter(const PipelineId pipelineId, const SliceEnd sliceEnd, const WorkerThreadId threadId)
{
    std::stringstream ss;
    ss << "right_" << pipelineId.getRawValue() << "_" << sliceEnd.getRawValue() << "_" << threadId.getRawValue() << ".dat";
    return getFileWriterFromMap(ss.str());
}

std::optional<std::shared_ptr<FileReader>>
MemoryController::getLeftFileReader(const PipelineId pipelineId, const SliceEnd sliceEnd, const WorkerThreadId threadId)
{
    std::stringstream ss;
    ss << "left_" << pipelineId.getRawValue() << "_" << sliceEnd.getRawValue() << "_" << threadId.getRawValue() << ".dat";
    return getFileReaderFromMap(ss.str());
}

std::optional<std::shared_ptr<FileReader>>
MemoryController::getRightFileReader(const PipelineId pipelineId, const SliceEnd sliceEnd, const WorkerThreadId threadId)
{
    std::stringstream ss;
    ss << "right_" << pipelineId.getRawValue() << "_" << sliceEnd.getRawValue() << "_" << threadId.getRawValue() << ".dat";
    return getFileReaderFromMap(ss.str());
}

std::optional<std::shared_ptr<FileWriter>> MemoryController::getFileWriterFromMap(const std::string& filePath)
{
    const auto readerIt = fileReaders.find(filePath);
    if (readerIt != fileReaders.end())
    {
        NES_ERROR("File {} is already opened for reading. Cannot open for writing.", filePath);
        return std::nullopt;
    }

    const auto it = fileWriters.find(filePath);
    if (it != fileWriters.end())
    {
        return std::make_optional(it->second);
    }
    fileWriters[filePath] = std::make_shared<FileWriter>(filePath);
    return std::make_optional(fileWriters[filePath]);
}

std::optional<std::shared_ptr<FileReader>> MemoryController::getFileReaderFromMap(const std::string& filePath)
{
    const auto it = fileReaders.find(filePath);
    if (it != fileReaders.end())
    {
        return std::make_optional(it->second);
    }

    const auto writerIt = fileWriters.find(filePath);
    if (writerIt != fileWriters.end())
    {
        fileWriters.erase(writerIt);
        fileReaders[filePath] = std::make_shared<FileReader>(filePath);
        return std::make_optional(fileReaders[filePath]);
    }

    return std::nullopt;
}

}
