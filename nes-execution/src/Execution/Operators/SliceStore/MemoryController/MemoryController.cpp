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

MemoryController::MemoryController(const std::filesystem::path& workingDir, const QueryId queryId, const OriginId originId)
    : bufferSize(BUFFER_SIZE), poolSize(POOL_SIZE), workingDir(workingDir), queryId(queryId), originId(originId)
{
    if (bufferSize != 0)
    {
        readBuffer.resize(bufferSize * NUM_READ_BUFFERS);
        memoryPool.resize(bufferSize * poolSize);
        for (size_t i = 0; i < poolSize; ++i)
        {
            freeBuffers.push_back(memoryPool.data() + i * bufferSize);
        }
    }
}

MemoryController::MemoryController(MemoryController& other)
    : readBuffer(other.readBuffer)
    , readBufFlag(other.readBufFlag.load())
    , memoryPool(other.memoryPool)
    , bufferSize(other.bufferSize)
    , poolSize(other.poolSize)
    , workingDir(other.workingDir)
    , queryId(other.queryId)
    , originId(other.originId)
{
    const std::lock_guard writerLock(fileWritersMutex);
    const std::lock_guard memoryLock(memoryPoolMutex);
    const std::lock_guard otherWriterLock(other.fileWritersMutex);
    const std::lock_guard otherMemoryLock(other.memoryPoolMutex);
    fileWriters = other.fileWriters;
    freeBuffers = other.freeBuffers;
}

MemoryController::MemoryController(MemoryController&& other) noexcept
    : readBuffer(std::move(other.readBuffer))
    , readBufFlag(std::move(other.readBufFlag.load()))
    , memoryPool(std::move(other.memoryPool))
    , bufferSize(std::move(other.bufferSize))
    , poolSize(std::move(other.poolSize))
    , workingDir(std::move(other.workingDir))
    , queryId(std::move(other.queryId))
    , originId(std::move(other.originId))
{
    const std::lock_guard writerLock(fileWritersMutex);
    const std::lock_guard memoryLock(memoryPoolMutex);
    const std::lock_guard otherWriterLock(other.fileWritersMutex);
    const std::lock_guard otherMemoryLock(other.memoryPoolMutex);
    fileWriters = std::move(other.fileWriters);
    freeBuffers = std::move(other.freeBuffers);
}

MemoryController& MemoryController::operator=(MemoryController& other)
{
    if (this == &other)
    {
        return *this;
    }
    const std::lock_guard writerLock(fileWritersMutex);
    const std::lock_guard memoryLock(memoryPoolMutex);
    const std::lock_guard otherWriterLock(other.fileWritersMutex);
    const std::lock_guard otherMemoryLock(other.memoryPoolMutex);
    readBuffer = other.readBuffer;
    readBufFlag = other.readBufFlag.load();
    memoryPool = other.memoryPool;
    freeBuffers = other.freeBuffers;
    bufferSize = other.bufferSize;
    poolSize = other.poolSize;
    fileWriters = other.fileWriters;
    workingDir = other.workingDir;
    queryId = other.queryId;
    originId = other.originId;
    return *this;
}

MemoryController& MemoryController::operator=(MemoryController&& other) noexcept
{
    if (this == &other)
    {
        return *this;
    }
    const std::lock_guard writerLock(fileWritersMutex);
    const std::lock_guard memoryLock(memoryPoolMutex);
    const std::lock_guard otherWriterLock(other.fileWritersMutex);
    const std::lock_guard otherMemoryLock(other.memoryPoolMutex);
    readBuffer = std::move(other.readBuffer);
    readBufFlag = std::move(other.readBufFlag.load());
    memoryPool = std::move(other.memoryPool);
    freeBuffers = std::move(other.freeBuffers);
    bufferSize = std::move(other.bufferSize);
    poolSize = std::move(other.poolSize);
    fileWriters = std::move(other.fileWriters);
    workingDir = std::move(other.workingDir);
    queryId = std::move(other.queryId);
    originId = std::move(other.originId);
    return *this;
}

MemoryController::~MemoryController()
{
    // TODO investigate why destructor is never called
    const std::lock_guard lock(fileWritersMutex);
    for (auto it = fileWriters.begin(); it != fileWriters.end(); ++it)
    {
        removeFileSystem(it);
    }
}

std::shared_ptr<FileWriter> MemoryController::getFileWriter(
    const SliceEnd sliceEnd, const WorkerThreadId threadId, const PipelineId, const QueryCompilation::JoinBuildSideType joinBuildSide)
{
    const auto filePath = constructFilePath(sliceEnd, threadId, joinBuildSide);

    /// Search for matching fileWriter to avoid attempting to open a file twice
    const std::lock_guard lock(fileWritersMutex);
    if (const auto it = fileWriters.find(filePath); it != fileWriters.end())
    {
        return it->second;
    }

    auto fileWriter = std::make_shared<FileWriter>(
        filePath, [this] { return allocateBuffer(); }, [this](char* buf) { deallocateBuffer(buf); }, bufferSize);
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
        return std::make_shared<FileReader>(filePath, [this] { return allocateReadBuffer(); }, [](char*) {}, bufferSize);
    }

    return nullptr;
}

void MemoryController::deleteSliceFiles(const SliceEnd sliceEnd)
{
    const std::lock_guard lock(fileWritersMutex);

    for (const auto joinBuildSide : {QueryCompilation::JoinBuildSideType::Left, QueryCompilation::JoinBuildSideType::Right})
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
            / std::filesystem::path(
                fmt::format(
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

    return readBufFlag.exchange(!readBufFlag) ? readBuffer.data() : readBuffer.data() + bufferSize;
}

char* MemoryController::allocateBuffer()
{
    if (bufferSize == 0)
    {
        return nullptr;
    }

    std::unique_lock lock(memoryPoolMutex);
    memoryPoolCondition.wait(lock, [this] { return !freeBuffers.empty(); });

    char* buffer = freeBuffers.back();
    freeBuffers.pop_back();
    return buffer;
}

void MemoryController::deallocateBuffer(char* buffer)
{
    if (bufferSize == 0 || buffer == nullptr || (memoryPool.data() <= buffer && buffer < memoryPool.data() + memoryPool.size()))
    {
        return;
    }

    const std::lock_guard lock(memoryPoolMutex);
    freeBuffers.push_back(buffer);
    memoryPoolCondition.notify_one();
}

}
