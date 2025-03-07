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
#include <antlr4-runtime/Exceptions.h>

namespace NES::Runtime::Execution
{

MemoryController::MemoryController(const MemoryController& other) : fileWriters(other.fileWriters)
{
}

MemoryController::MemoryController(MemoryController&& other) noexcept : fileWriters(std::move(other.fileWriters))
{
}

MemoryController& MemoryController::operator=(const MemoryController& other)
{
    if (this == &other)
    {
        return *this;
    }
    fileWriters = other.fileWriters;
    return *this;
}

MemoryController& MemoryController::operator=(MemoryController&& other) noexcept
{
    if (this == &other)
    {
        return *this;
    }
    fileWriters = std::move(other.fileWriters);
    return *this;
}

std::shared_ptr<FileWriter>
MemoryController::getLeftFileWriter(const PipelineId pipelineId, const SliceEnd sliceEnd, const WorkerThreadId threadId)
{
    std::stringstream ss;
    ss << "memory_controller_left_" << pipelineId.getRawValue() << "_" << sliceEnd.getRawValue() << "_" << threadId.getRawValue() << ".dat";
    return getFileWriterFromMap(ss.str());
}

std::shared_ptr<FileWriter>
MemoryController::getRightFileWriter(const PipelineId pipelineId, const SliceEnd sliceEnd, const WorkerThreadId threadId)
{
    std::stringstream ss;
    ss << "memory_controller_right_" << pipelineId.getRawValue() << "_" << sliceEnd.getRawValue() << "_" << threadId.getRawValue() << ".dat";
    return getFileWriterFromMap(ss.str());
}

std::shared_ptr<FileReader>
MemoryController::getLeftFileReader(const PipelineId pipelineId, const SliceEnd sliceEnd, const WorkerThreadId threadId)
{
    std::stringstream ss;
    ss << "memory_controller_left_" << pipelineId.getRawValue() << "_" << sliceEnd.getRawValue() << "_" << threadId.getRawValue() << ".dat";
    return getFileReader(ss.str());
}

std::shared_ptr<FileReader>
MemoryController::getRightFileReader(const PipelineId pipelineId, const SliceEnd sliceEnd, const WorkerThreadId threadId)
{
    std::stringstream ss;
    ss << "memory_controller_right_" << pipelineId.getRawValue() << "_" << sliceEnd.getRawValue() << "_" << threadId.getRawValue() << ".dat";
    return getFileReader(ss.str());
}

std::shared_ptr<FileWriter> MemoryController::getFileWriterFromMap(const std::string& filePath)
{
    const auto it = fileWriters.find(filePath);
    if (it != fileWriters.end())
    {
        return it->second;
    }
    fileWriters[filePath] = std::make_shared<FileWriter>(filePath);
    return fileWriters[filePath];
}

std::shared_ptr<FileReader> MemoryController::getFileReader(const std::string& filePath)
{
    const auto writerIt = fileWriters.find(filePath);
    if (writerIt != fileWriters.end())
    {
        fileWriters.erase(writerIt);
        return std::make_shared<FileReader>(filePath);
    }
    std::stringstream ss;
    ss << "File " << filePath << " was not opened for writing or has already been read. Cannot open for reading.";
    //throw antlr4::IllegalArgumentException(ss.str());
    return nullptr;
}

}
