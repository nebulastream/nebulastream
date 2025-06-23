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

#include <SliceStore/MemoryController/MemoryController.hpp>

namespace NES
{

MemoryController::MemoryController(
    const size_t bufferSize,
    const uint64_t numWorkerThreads,
    std::filesystem::path workingDir,
    const QueryId queryId,
    const OriginId originId)
    : bufferSize(bufferSize), workingDir(std::move(workingDir)), queryId(queryId), originId(originId)
{
    if (bufferSize > 0)
    {
        const auto writePoolSize = std::max(numWorkerThreads * POOL_SIZE_MULTIPLIER, POOL_SIZE_MULTIPLIER);
        const auto readPoolSize = std::max(numWorkerThreads * POOL_SIZE_MULTIPLIER, POOL_SIZE_MULTIPLIER);

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

    allFileWriters.resize(numWorkerThreads + 1);
    fileWriters.resize(numWorkerThreads + 1);
    fileWriterMutexes = std::vector<std::mutex>(numWorkerThreads + 1);
}

MemoryController::~MemoryController()
{
    // TODO investigate why destructor is never called
    for (auto i = 0UL; i < fileWriters.size(); ++i)
    {
        for (auto mapIt = fileWriters[i].begin(); mapIt != fileWriters[i].end(); ++mapIt)
        {
            removeFileSystem(mapIt, WorkerThreadId(i));
        }
    }
}

std::shared_ptr<FileWriter> MemoryController::getFileWriter(
    const SliceEnd sliceEnd,
    const WorkerThreadId threadId,
    const JoinBuildSideType joinBuildSide,
    boost::asio::io_context& ioCtx,
    const bool checkExistence)
{
    /// Search for matching fileWriter to avoid attempting to open a file twice
    auto& allWritersMap = allFileWriters[threadId.getRawValue()];
    auto& writerMap = fileWriters[threadId.getRawValue()];
    const std::scoped_lock lock(fileWriterMutexes[threadId.getRawValue()]);
    if (const auto it = writerMap.find({sliceEnd, joinBuildSide}); it != writerMap.end())
    {
        return it->second;
    }

    const auto& filePath = constructFilePath(sliceEnd, threadId, joinBuildSide);
    if (checkExistence and allWritersMap.contains(filePath))
    {
        throw std::runtime_error("FileWriter previously existed");
    }
    auto fileWriter = std::make_shared<FileWriter>(
        ioCtx, filePath, [this] { return allocateWriteBuffer(); }, [this](char* buf) { deallocateWriteBuffer(buf); }, bufferSize);

    allWritersMap[filePath] = true;
    writerMap[{sliceEnd, joinBuildSide}] = fileWriter;
    return fileWriter;
}

std::shared_ptr<FileReader>
MemoryController::getFileReader(const SliceEnd sliceEnd, const WorkerThreadId threadId, const JoinBuildSideType joinBuildSide)
{
    /// Erase matching fileWriter as the data must not be amended after being read. This also enforces reading data only once
    auto& writerMap = fileWriters[threadId.getRawValue()];
    const std::scoped_lock lock(fileWriterMutexes[threadId.getRawValue()]);
    if (const auto it = writerMap.find({sliceEnd, joinBuildSide}); it != writerMap.end())
    {
        writerMap.erase(it);
        const auto& filePath = constructFilePath(sliceEnd, threadId, joinBuildSide);
        return std::make_shared<FileReader>(
            filePath, [this] { return allocateReadBuffer(); }, [this](char* buf) { deallocateReadBuffer(buf); }, bufferSize);
    }

    return nullptr;
}

void MemoryController::deleteSliceFiles(const SliceEnd sliceEnd)
{
    for (auto i = 0UL; i < fileWriters.size(); ++i)
    {
        auto& writerMap = fileWriters[i];
        const std::scoped_lock lock(fileWriterMutexes[i]);
        for (const auto& joinBuildSide : {JoinBuildSideType::Left, JoinBuildSideType::Right})
        {
            if (const auto it = writerMap.find({sliceEnd, joinBuildSide}); it != writerMap.end())
            {
                removeFileSystem(it, WorkerThreadId(i));
            }
        }
    }
}

std::string
MemoryController::constructFilePath(const SliceEnd sliceEnd, const WorkerThreadId threadId, const JoinBuildSideType joinBuildSide) const
{
    const std::string sideStr = joinBuildSide == JoinBuildSideType::Left ? "left" : "right";
    return (workingDir
            / std::filesystem::path(fmt::format(
                "memory_controller_{}_{}_{}_{}_{}",
                queryId.getRawValue(),
                originId.getRawValue(),
                sideStr,
                sliceEnd.getRawValue(),
                threadId.getRawValue())))
        .string();
}

void MemoryController::removeFileSystem(
    const std::map<std::pair<SliceEnd, JoinBuildSideType>, std::shared_ptr<FileWriter>>::iterator it, const WorkerThreadId threadId)
{
    /// Files are deleted in FileReader destructor
    const auto filePath = constructFilePath(it->first.first, threadId, it->first.second);
    fileWriters[threadId.getRawValue()].erase(it);
    const FileReader fileReader(filePath, [] { return nullptr; }, [](char*) {}, 0);
}

char* MemoryController::allocateReadBuffer()
{
    if (bufferSize == 0)
    {
        return nullptr;
    }

    const std::scoped_lock lock(readMemoryPoolMutex);
    if (freeReadBuffers.empty())
    {
        /// This should not happen as we deallocate buffers after updating slices
        throw std::runtime_error("No read buffers available for allocation!");
    }

    char* buffer = freeReadBuffers.back();
    freeReadBuffers.pop_back();
    return buffer;
}

void MemoryController::deallocateReadBuffer(char* buffer)
{
    if (bufferSize == 0 || buffer == nullptr || readMemoryPool.data() > buffer || buffer >= readMemoryPool.data() + readMemoryPool.size())
    {
        return;
    }

    const std::scoped_lock lock(readMemoryPoolMutex);
    freeReadBuffers.push_back(buffer);
}

char* MemoryController::allocateWriteBuffer()
{
    if (bufferSize == 0)
    {
        return nullptr;
    }

    const std::scoped_lock lock(writeMemoryPoolMutex);
    if (freeWriteBuffers.empty())
    {
        /// This should not happen as we deallocate buffers after updating slices
        throw std::runtime_error("No write buffers available for allocation!");
    }

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

    const std::scoped_lock lock(writeMemoryPoolMutex);
    freeWriteBuffers.push_back(buffer);
}

}
