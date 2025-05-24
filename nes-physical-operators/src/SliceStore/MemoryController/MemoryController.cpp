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
        const auto writePoolSize = std::min(numWorkerThreads * POOL_SIZE_MULTIPLIER, POOL_SIZE_MULTIPLIER);
        const auto readPoolSize = std::min(numWorkerThreads * POOL_SIZE_MULTIPLIER, POOL_SIZE_MULTIPLIER);

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

    fileWriters.resize(numWorkerThreads + 1);
    fileWriterMutexes = std::vector<std::mutex>(numWorkerThreads + 1);
    fileLayouts.resize(numWorkerThreads);
    fileLayoutMutexes = std::vector<std::mutex>(numWorkerThreads);
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
    const SliceEnd sliceEnd, const WorkerThreadId threadId, const JoinBuildSideType joinBuildSide, boost::asio::io_context& ioCtx)
{
    /// Search for matching fileWriter to avoid attempting to open a file twice
    auto& writerMap = fileWriters[threadId.getRawValue()];
    const std::scoped_lock lock(fileWriterMutexes[threadId.getRawValue()]);
    if (const auto it = writerMap.find({sliceEnd, joinBuildSide}); it != writerMap.end())
    {
        return it->second;
    }

    const auto& filePath = constructFilePath(sliceEnd, threadId, joinBuildSide);
    auto fileWriter = std::make_shared<FileWriter>(
        ioCtx, filePath, [this] { return allocateWriteBuffer(); }, [this](char* buf) { deallocateWriteBuffer(buf); }, bufferSize);
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

void MemoryController::setFileLayout(
    const SliceEnd sliceEnd, const WorkerThreadId threadId, const JoinBuildSideType joinBuildSide, const FileLayout fileLayout)
{
    auto& layoutMap = fileLayouts[threadId.getRawValue()];
    const std::scoped_lock lock(fileLayoutMutexes[threadId.getRawValue()]);
    layoutMap[{sliceEnd, joinBuildSide}] = fileLayout;
}

std::optional<FileLayout>
MemoryController::getFileLayout(const SliceEnd sliceEnd, const WorkerThreadId threadId, const JoinBuildSideType joinBuildSide)
{
    const auto& layoutMap = fileLayouts[threadId.getRawValue()];
    const std::scoped_lock lock(fileLayoutMutexes[threadId.getRawValue()]);
    if (const auto it = layoutMap.find({sliceEnd, joinBuildSide}); it != layoutMap.end())
    {
        return it->second;
    }
    return {};
}

void MemoryController::deleteFileLayout(const SliceEnd sliceEnd, const WorkerThreadId threadId, const JoinBuildSideType joinBuildSide)
{
    auto& layoutMap = fileLayouts[threadId.getRawValue()];
    const std::scoped_lock lock(fileLayoutMutexes[threadId.getRawValue()]);
    if (const auto it = layoutMap.find({sliceEnd, joinBuildSide}); it != layoutMap.end())
    {
        layoutMap.erase(it);
    }
}

void MemoryController::deleteSliceFiles(const SliceEnd sliceEnd)
{
    // TODO delete from layout map as well
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

    const std::scoped_lock lock(readMemoryPoolMutex);
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

    const std::scoped_lock lock(writeMemoryPoolMutex);
    freeWriteBuffers.push_back(buffer);
    writeMemoryPoolCondition.notify_one();
}

}
