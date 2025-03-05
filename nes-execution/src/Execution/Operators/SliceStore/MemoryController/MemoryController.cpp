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

FileWriter& MemoryController::getLeftFileWriter(const SliceEnd sliceEnd, const WorkerThreadId threadId)
{
    std::stringstream ss;
    ss << "left_" << sliceEnd.getRawValue() << "_" << threadId.getRawValue() << ".dat";
    return getFileWriterFromMap(ss.str());
}

FileWriter& MemoryController::getRightFileWriter(const SliceEnd sliceEnd, const WorkerThreadId threadId)
{
    std::stringstream ss;
    ss << "right_" << sliceEnd.getRawValue() << "_" << threadId.getRawValue() << ".dat";
    return getFileWriterFromMap(ss.str());
}

FileReader& MemoryController::getLeftFileReader(const SliceEnd sliceEnd, const WorkerThreadId threadId)
{
    std::stringstream ss;
    ss << "left_" << sliceEnd.getRawValue() << "_" << threadId.getRawValue() << ".dat";
    return getFileReaderFromMap(ss.str());
}

FileReader& MemoryController::getRightFileReader(const SliceEnd sliceEnd, const WorkerThreadId threadId)
{
    std::stringstream ss;
    ss << "right_" << sliceEnd.getRawValue() << "_" << threadId.getRawValue() << ".dat";
    return getFileReaderFromMap(ss.str());
}

FileWriter& MemoryController::getFileWriterFromMap(const std::string& filePath)
{
    const auto it = fileWriters.find(filePath);
    if (it != fileWriters.end())
    {
        return *it->second;
    }
    fileWriters[filePath] = std::make_shared<FileWriter>(filePath);
    return *fileWriters[filePath];
}

FileReader& MemoryController::getFileReaderFromMap(const std::string& filePath)
{
    const auto it = fileReaders.find(filePath);
    if (it != fileReaders.end())
    {
        return *it->second;
    }
    fileReaders[filePath] = std::make_shared<FileReader>(filePath);
    return *fileReaders[filePath];
}

}
