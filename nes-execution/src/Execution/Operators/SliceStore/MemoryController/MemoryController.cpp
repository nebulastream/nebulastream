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

MemoryController::~MemoryController()
{
    // TODO investigate why destructor is never called
    for (auto it = fileWriters.begin(); it != fileWriters.end(); ++it)
    {
        removeFileSystem(it);
    }
}

std::shared_ptr<FileWriter> MemoryController::getFileWriter(
    const SliceEnd sliceEnd,
    const PipelineId pipelineId,
    const WorkerThreadId threadId,
    const QueryCompilation::JoinBuildSideType joinBuildSide)
{
    std::stringstream ss;
    ss << "memory_controller_";

    switch (joinBuildSide)
    {
        case QueryCompilation::JoinBuildSideType::Left: {
            ss << "left_";
            break;
        }
        case QueryCompilation::JoinBuildSideType::Right: {
            ss << "right_";
            break;
        }
    }

    if constexpr (USE_PIPELINE_ID)
    {
        ss << sliceEnd.getRawValue() << "_" << pipelineId.getRawValue() << "_" << threadId.getRawValue();
    }
    else
    {
        ss << sliceEnd.getRawValue() << "_" << threadId.getRawValue();
    }
    return getFileWriterFromMap(ss.str());
}

std::shared_ptr<FileReader> MemoryController::getFileReader(
    const SliceEnd sliceEnd,
    const PipelineId pipelineId,
    const WorkerThreadId threadId,
    const QueryCompilation::JoinBuildSideType joinBuildSide)
{
    std::stringstream ss;
    ss << "memory_controller_";

    switch (joinBuildSide)
    {
        case QueryCompilation::JoinBuildSideType::Left: {
            ss << "left_";
            break;
        }
        case QueryCompilation::JoinBuildSideType::Right: {
            ss << "right_";
            break;
        }
    }

    if constexpr (USE_PIPELINE_ID)
    {
        ss << sliceEnd.getRawValue() << "_" << pipelineId.getRawValue() << "_" << threadId.getRawValue();
    }
    else
    {
        ss << sliceEnd.getRawValue() << "_" << threadId.getRawValue();
    }
    return getFileReaderAndEraseWriter(ss.str());
}

void MemoryController::deleteSliceFiles(const SliceEnd sliceEnd, const PipelineId)
{
    for (const auto& prefixBase : {"memory_controller_left_", "memory_controller_right_"})
    {
        const auto prefix = prefixBase + std::to_string(sliceEnd.getRawValue()) + "_";
        const auto end = fileWriters.upper_bound(prefix + "\xFF");
        auto it = fileWriters.lower_bound(prefix);
        while (it != end)
        {
            removeFileSystem(it++);
        }
    }
}

std::shared_ptr<FileWriter> MemoryController::getFileWriterFromMap(const std::string& filePath)
{
    std::lock_guard lock(mutex_);

    /// Search for matching fileWriter to avoid attempting to open a file twice
    if (const auto it = fileWriters.find(filePath); it != fileWriters.end())
    {
        return it->second;
    }
    auto fileWriter = std::make_shared<FileWriter>(filePath);
    fileWriters[filePath] = fileWriter;
    return fileWriter;
}

std::shared_ptr<FileReader> MemoryController::getFileReaderAndEraseWriter(const std::string& filePath)
{
    std::lock_guard lock(mutex_);

    /// Erase matching fileWriter as the data must not be amended after being read. This also enforces reading data only once
    if (const auto it = fileWriters.find(filePath); it != fileWriters.end())
    {
        fileWriters.erase(it);
        return std::make_shared<FileReader>(filePath);
    }
    return nullptr;
}

void MemoryController::removeFileSystem(const std::map<std::string, std::shared_ptr<FileWriter>>::iterator it)
{
    /// Files are deleted in FileReader destructor
    const auto filePath = it->first;
    fileWriters.erase(it);
    FileReader fileReader(filePath);
}

}
