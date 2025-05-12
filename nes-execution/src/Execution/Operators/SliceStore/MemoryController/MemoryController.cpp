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

MemoryController::MemoryController(
    const size_t bufferSize,
    const uint64_t numWriteBuffers,
    const uint64_t numReadBuffers,
    std::filesystem::path workingDir,
    const QueryId queryId,
    const OriginId originId)
    : bufferSize(bufferSize), workingDir(std::move(workingDir)), queryId(queryId), originId(originId)
{
    if (bufferSize > 0)
    {
        const auto writePoolSize = std::min(numWriteBuffers * POOL_SIZE_MULTIPLIER, POOL_SIZE_MULTIPLIER);
        const auto readPoolSize = std::min(numReadBuffers * POOL_SIZE_MULTIPLIER, POOL_SIZE_MULTIPLIER);

        writeMemoryPool.resize(bufferSize * writePoolSize);
        for (size_t i = 0; i < writePoolSize; ++i)
        {
            freeWriteBuffers.push_back(writeMemoryPool.data() + i * bufferSize);
        }
        readMemoryPool.resize(bufferSize * readPoolSize);
        for (size_t i = 0; i < readPoolSize; ++i)
        {
            freeReadBuffers.push_back(readMemoryPool.data() + i * bufferSize);
        }
    }
}

MemoryController::~MemoryController()
{
    // TODO investigate why destructor is never called
    const std::lock_guard writersLock(fileWritersMutex);
    for (auto it = fileWriters.begin(); it != fileWriters.end(); ++it)
    {
        removeFileSystem(it);
    }

    const std::lock_guard layoutsLock(fileLayoutsMutex);
    for (auto it = fileLayouts.begin(); it != fileLayouts.end(); ++it)
    {
        fileLayouts.erase(it);
    }
}

std::shared_ptr<FileWriter> MemoryController::getFileWriter(
    const SliceEnd sliceEnd, const WorkerThreadId threadId, const QueryCompilation::JoinBuildSideType joinBuildSide)
{
    /// TODO don't construct path every time, just use std::tuple for map
    const auto filePath = constructFilePath(sliceEnd, threadId, joinBuildSide);

    /// Search for matching fileWriter to avoid attempting to open a file twice
    const std::lock_guard lock(fileWritersMutex);
    if (const auto it = fileWriters.find(filePath); it != fileWriters.end())
    {
        return it->second;
    }

    auto fileWriter = std::make_shared<FileWriter>(
        filePath, [this] { return allocateWriteBuffer(); }, [this](char* buf) { deallocateWriteBuffer(buf); }, bufferSize);
    fileWriters[filePath] = fileWriter;
    return fileWriter;
}

std::shared_ptr<FileReader> MemoryController::getFileReader(
    const SliceEnd sliceEnd, const WorkerThreadId threadId, const QueryCompilation::JoinBuildSideType joinBuildSide)
{
    const auto filePath = constructFilePath(sliceEnd, threadId, joinBuildSide);

    /// Erase matching fileWriter as the data must not be amended after being read. This also enforces reading data only once
    const std::lock_guard lock(fileWritersMutex);
    if (const auto it = fileWriters.find(filePath); it != fileWriters.end())
    {
        fileWriters.erase(it);
        return std::make_shared<FileReader>(
            filePath, [this] { return allocateReadBuffer(); }, [this](char* buf) { deallocateReadBuffer(buf); }, bufferSize);
    }

    return nullptr;
}

void MemoryController::setFileLayout(
    const SliceEnd sliceEnd,
    const WorkerThreadId threadId,
    const QueryCompilation::JoinBuildSideType joinBuildSide,
    const FileLayout fileLayout)
{
    const auto filePath = constructFilePath(sliceEnd, threadId, joinBuildSide);

    /// Insert or override
    const std::lock_guard lock(fileLayoutsMutex);
    fileLayouts.insert_or_assign({sliceEnd, threadId, joinBuildSide}, fileLayout);
}

std::optional<FileLayout> MemoryController::getFileLayout(
    const SliceEnd sliceEnd, const WorkerThreadId threadId, const QueryCompilation::JoinBuildSideType joinBuildSide)
{
    const auto filePath = constructFilePath(sliceEnd, threadId, joinBuildSide);

    const std::lock_guard lock(fileLayoutsMutex);
    if (const auto it = fileLayouts.find({sliceEnd, threadId, joinBuildSide}); it != fileLayouts.end())
    {
        return it->second;
    }

    return {};
}

void MemoryController::deleteFileLayout(
    const SliceEnd sliceEnd, const WorkerThreadId threadId, const QueryCompilation::JoinBuildSideType joinBuildSide)
{
    const auto filePath = constructFilePath(sliceEnd, threadId, joinBuildSide);

    const std::lock_guard lock(fileLayoutsMutex);
    if (const auto it = fileLayouts.find({sliceEnd, threadId, joinBuildSide}); it != fileLayouts.end())
    {
        fileLayouts.erase(it);
    }
}

void MemoryController::deleteSliceFiles(const SliceEnd sliceEnd)
{
    // TODO delete from layout map as well
    const std::lock_guard lock(fileWritersMutex);

    for (const auto& joinBuildSide : {QueryCompilation::JoinBuildSideType::Left, QueryCompilation::JoinBuildSideType::Right})
    {
        const auto filePathPrefix = constructFilePath(sliceEnd, joinBuildSide);
        const auto end = fileWriters.upper_bound(filePathPrefix + "\xFF");
        auto it = fileWriters.lower_bound(filePathPrefix);
        while (it != end)
        {
            removeFileSystem(it++);
        }
    }
}

std::string MemoryController::constructFilePath(const SliceEnd sliceEnd, const QueryCompilation::JoinBuildSideType joinBuildSide) const
{
    const std::string sideStr = joinBuildSide == QueryCompilation::JoinBuildSideType::Left ? "left" : "right";
    return (workingDir
            / std::filesystem::path(fmt::format(
                "memory_controller_{}_{}_{}_{}_", queryId.getRawValue(), originId.getRawValue(), sideStr, sliceEnd.getRawValue())))
        .string();
}

std::string MemoryController::constructFilePath(
    const SliceEnd sliceEnd, const WorkerThreadId threadId, const QueryCompilation::JoinBuildSideType joinBuildSide) const
{
    return constructFilePath(sliceEnd, joinBuildSide) + std::to_string(threadId.getRawValue());
}

void MemoryController::removeFileSystem(const std::map<std::string, std::shared_ptr<FileWriter>>::iterator it)
{
    /// Files are deleted in FileReader destructor
    const auto filePath = it->first;
    fileWriters.erase(it);
    const FileReader fileReader(filePath, [] { return nullptr; }, [](char*) {}, 0);
}

char* MemoryController::allocateReadBuffer()
{
    if (bufferSize == 0)
    {
        return nullptr;
    }

    std::unique_lock lock(readMemoryPoolMutex);
    readMemoryPoolCondition.wait(lock, [this] { return !freeReadBuffers.empty(); });

    char* buffer = freeReadBuffers.back();
    freeReadBuffers.pop_back();
    return buffer;
}

void MemoryController::deallocateReadBuffer(char* buffer)
{
    if (bufferSize == 0 || buffer == nullptr
        || !(readMemoryPool.data() <= buffer && buffer < readMemoryPool.data() + readMemoryPool.size()))
    {
        return;
    }

    const std::lock_guard lock(readMemoryPoolMutex);
    freeReadBuffers.push_back(buffer);
    readMemoryPoolCondition.notify_one();
}

char* MemoryController::allocateWriteBuffer()
{
    if (bufferSize == 0)
    {
        return nullptr;
    }

    std::unique_lock lock(writeMemoryPoolMutex);
    writeMemoryPoolCondition.wait(lock, [this] { return !freeWriteBuffers.empty(); });

    char* buffer = freeWriteBuffers.back();
    freeWriteBuffers.pop_back();
    return buffer;
}

void MemoryController::deallocateWriteBuffer(char* buffer)
{
    if (bufferSize == 0 || buffer == nullptr
        || !(writeMemoryPool.data() <= buffer && buffer < writeMemoryPool.data() + writeMemoryPool.size()))
    {
        return;
    }

    const std::lock_guard lock(writeMemoryPoolMutex);
    freeWriteBuffers.push_back(buffer);
    writeMemoryPoolCondition.notify_one();
}

}
