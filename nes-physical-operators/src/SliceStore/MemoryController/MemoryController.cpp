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

#include <iostream>
#include <ranges>

#include <SliceStore/MemoryController/MemoryController.hpp>
#include <sys/statvfs.h>

namespace NES
{

MemoryController::MemoryController(
    MemoryControllerInfo memoryControllerInfo, const uint64_t numWorkerThreads, const uint64_t minNumFileDescriptorsPerWorker)
    : memoryControllerInfo(std::move(memoryControllerInfo))
    , memoryPool(this->memoryControllerInfo.fileDescriptorBufferSize, this->memoryControllerInfo.numberOfBuffersPerWorker, numWorkerThreads)
{
    /// Memory pool adjusts the buffer size to account for separate key and payload buffers
    this->memoryControllerInfo.fileDescriptorBufferSize = memoryPool.getFileDescriptorBufferSize();

    /// Create file writer map per thread to reduce the time waiting for mutexes
    threadWriters = std::vector<ThreadLocalWriters>(numWorkerThreads);

    /// Update maxNumFileDescriptors according to the system's supported maximum number of file descriptors and create limiter
    if (this->memoryControllerInfo.maxNumFileDescriptors > 0)
    {
        this->memoryControllerInfo.maxNumFileDescriptors = setFileDescriptorLimit(this->memoryControllerInfo.maxNumFileDescriptors);
        /// Each worker thread must be able to have minNumFileDescriptorsPerWorker many file writers and readers at once
        assert(this->memoryControllerInfo.maxNumFileDescriptors > 2 * numWorkerThreads * minNumFileDescriptorsPerWorker);
        /// Subtract number of worker threads multiplied by number of descriptors per worker to be able to create file readers at any time
        this->memoryControllerInfo.maxNumFileDescriptors -= numWorkerThreads * minNumFileDescriptorsPerWorker;
        /// Divide by number of descriptors per worker as each created file writer potentially needs to open this amount of descriptors
        this->memoryControllerInfo.maxNumFileDescriptors /= minNumFileDescriptorsPerWorker;
        /// Divide by number of worker threads as we enforce a limit on each worker separately
        this->memoryControllerInfo.maxNumFileDescriptors /= numWorkerThreads;
    }
    else
    {
        /// Set the descriptor limit to the system's maximum amount while preserving the value of maxNumFileDescriptors to disable limiter
        setFileDescriptorLimit(UINT64_MAX);
    }
    std::cout << fmt::format("Number of file descriptors: {}\n", this->memoryControllerInfo.maxNumFileDescriptors);
}

MemoryController::~MemoryController()
{
    // TODO investigate why destructor is never called
    for (auto i = 0UL; i < threadWriters.size(); ++i)
    {
        auto& [writers, lru, openCount, mutex] = threadWriters[i];
        for (auto mapIt = writers.begin(); mapIt != writers.end(); ++mapIt)
        {
            deleteFileWriter(writers, lru, openCount, mapIt->first);
        }
    }
}

std::shared_ptr<FileWriter> MemoryController::getFileWriter(
    boost::asio::io_context& ioCtx, const SliceEnd sliceEnd, const WorkerThreadId threadId, const JoinBuildSideType joinBuildSide)
{
    auto& [writers, lru, openCount, mutex] = threadWriters[threadId.getRawValue()];
    const auto key = std::make_pair(sliceEnd, joinBuildSide);
    //mutex.lock();
    const std::scoped_lock lock(mutex);

    /// Search for matching writer to avoid attempting to open a file twice and update LRU if needed
    if (const auto it = writers.find(key); it != writers.end())
    {
        if (memoryControllerInfo.maxNumFileDescriptors > 0)
        {
            if (const auto pos = std::ranges::find(lru, key); pos != lru.end())
            {
                lru.erase(pos);
            }
            lru.push_back(key);
        }
        return it->second;
    }

    if (memoryControllerInfo.maxNumFileDescriptors > 0)
    {
        /// Close file descriptor if needed
        if (openCount >= memoryControllerInfo.maxNumFileDescriptors and not lru.empty())
        {
            const auto& evictKey = lru.front();
            deleteFileWriter(writers, lru, openCount, evictKey);
        }
        lru.push_back(key);
        //++openCount;
    }

    //std::cout << fmt::format(
    //    "Number of file descriptors for thread {}: {}\n", threadId.getRawValue(), 1048576 / threadWriters.size() - openCount);
    auto writer = std::make_shared<FileWriter>(
        ioCtx,
        constructFilePath(sliceEnd, threadId, joinBuildSide),
        [this, threadId](const FileWriter* obj) { return memoryPool.allocateWriteBuffer(threadWriters, obj, threadId); },
        [this, threadId](char* buf) { memoryPool.deallocateWriteBuffer(buf, threadId); },
        memoryControllerInfo.fileDescriptorBufferSize);

    writers[key] = writer;
    ++openCount;
    return writer;
}

void MemoryController::unlockFileWriter(const WorkerThreadId threadId)
{
    threadWriters[threadId.getRawValue()].mutex.unlock();
}

std::shared_ptr<FileReader>
MemoryController::getFileReader(const SliceEnd sliceEnd, const WorkerThreadId threadId, const JoinBuildSideType joinBuildSide)
{
    auto& [writers, lru, openCount, mutex] = threadWriters[threadId.getRawValue()];
    const auto key = std::make_pair(sliceEnd, joinBuildSide);
    const std::scoped_lock lock(mutex);

    /// Erase matching fileWriter as the data must not be amended after being read. This also enforces reading data only once
    if (const auto it = writers.find(key); it != writers.end())
    {
        if (memoryControllerInfo.maxNumFileDescriptors > 0)
        {
            if (const auto pos = std::ranges::find(lru, key); pos != lru.end())
            {
                lru.erase(pos);
            }
            //openCount--;
        }
        writers.erase(it);
        openCount--;

        return std::make_shared<FileReader>(
            constructFilePath(sliceEnd, threadId, joinBuildSide),
            [this] { return memoryPool.allocateReadBuffer(); },
            [this](char* buf) { memoryPool.deallocateReadBuffer(buf); },
            memoryControllerInfo.fileDescriptorBufferSize);
    }

    return nullptr;
}

void MemoryController::deleteSliceFiles(const SliceEnd sliceEnd)
{
    std::vector<std::shared_ptr<FileWriter>> writersToDelete;
    for (auto i = 0UL; i < threadWriters.size(); ++i)
    {
        auto& [writers, lru, openCount, mutex] = threadWriters[i];
        for (const auto& joinBuildSide : {JoinBuildSideType::Left, JoinBuildSideType::Right})
        {
            const std::scoped_lock lock(mutex);
            const auto& writer = deleteFileWriter(writers, lru, openCount, {sliceEnd, joinBuildSide});
            if (writer.has_value())
            {
                writersToDelete.emplace_back(writer.value());
            }
        }
    }

    for (const auto& writer : writersToDelete)
    {
        for (const auto& filePath : writer->getFilePaths())
        {
            std::filesystem::remove(filePath);
        }
    }
}

std::string
MemoryController::constructFilePath(const SliceEnd sliceEnd, const WorkerThreadId threadId, const JoinBuildSideType joinBuildSide) const
{
    const std::string sideStr = joinBuildSide == JoinBuildSideType::Left ? "left" : "right";
    return (memoryControllerInfo.workingDir
            / std::filesystem::path(
                fmt::format(
                    "memory_controller_{}_{}_{}_{}_{}",
                    memoryControllerInfo.queryId.getRawValue(),
                    memoryControllerInfo.outputOriginId.getRawValue(),
                    sideStr,
                    sliceEnd.getRawValue(),
                    threadId.getRawValue())))
        .string();
}

std::optional<std::shared_ptr<FileWriter>> MemoryController::deleteFileWriter(
    std::map<std::pair<SliceEnd, JoinBuildSideType>, std::shared_ptr<FileWriter>>& writers,
    std::deque<std::pair<SliceEnd, JoinBuildSideType>>& lru,
    uint64_t& openCount,
    const std::pair<SliceEnd, JoinBuildSideType>& key) const
{
    if (const auto it = writers.find(key); it != writers.end())
    {
        if (memoryControllerInfo.maxNumFileDescriptors > 0)
        {
            if (const auto pos = std::ranges::find(lru, key); pos != lru.end())
            {
                lru.erase(pos);
            }
            //openCount--;
        }
        writers.erase(it);
        openCount--;
        return it->second;
    }
    return {};
}

MemoryPool::MemoryPool(const uint64_t bufferSize, const uint64_t numBuffersPerWorker, const uint64_t numWorkerThreads)
    : fileDescriptorBufferSize(bufferSize / POOL_SIZE_MULTIPLIER)
{
    if (fileDescriptorBufferSize > 0)
    {
        const auto writePoolSize = std::max(1UL, numBuffersPerWorker) * numWorkerThreads * POOL_SIZE_MULTIPLIER;
        const auto readPoolSize = std::max(1UL, numBuffersPerWorker) * numWorkerThreads * POOL_SIZE_MULTIPLIER;

        writeMemoryPool.reserve(fileDescriptorBufferSize * writePoolSize);
        writeMemoryPoolCondition = std::vector<std::condition_variable>(numWorkerThreads);
        writeMemoryPoolMutex = std::vector<std::mutex>(numWorkerThreads);
        freeWriteBuffers.reserve(numWorkerThreads);
        for (size_t i = 0; i < writePoolSize; ++i)
        {
            freeWriteBuffers[i % numWorkerThreads].push_back(writeMemoryPool.data() + i * fileDescriptorBufferSize);
        }

        readMemoryPool.reserve(fileDescriptorBufferSize * readPoolSize);
        freeReadBuffers.reserve(numWorkerThreads);
        for (size_t i = 0; i < readPoolSize; ++i)
        {
            freeReadBuffers.push_back(readMemoryPool.data() + i * fileDescriptorBufferSize);
        }
    }
}

char* MemoryPool::allocateWriteBuffer(std::vector<ThreadLocalWriters>& threadWriters, const FileWriter* writer, WorkerThreadId threadId)
{
    if (fileDescriptorBufferSize == 0)
    {
        return nullptr;
    }

    //std::cout << "Getting write buffer\n";
    std::unique_lock lock(writeMemoryPoolMutex[threadId.getRawValue()]);
    /*std::cout << fmt::format(
        "Number of available write buffers for thread {} before free: {}\n",
        threadId.getRawValue(),
        freeWriteBuffers[threadId.getRawValue()].size());*/
    if (freeWriteBuffers[threadId.getRawValue()].empty())
    {
        lock.unlock();
        auto& [writers, lru, openCount, mutex] = threadWriters[threadId.getRawValue()];
        const std::scoped_lock innerLock(mutex);

        if (not lru.empty())
        {
            const auto& key = lru.front();
            if (const auto it = writers.find(key); it != writers.end())
            {
                //std::cout << "Freeing...\n";
                it->second->flushAndDeallocateBuffers();
            }
            else
            {
                //std::cout << "No file descriptor found\n";
            }
        }
        else
        {
            //std::cout << "Try Freeing...\n";
            for (const auto& val : writers | std::views::values)
            {
                if (val.get() != writer)
                {
                    //std::cout << "Freeing...\n";
                    val->flushAndDeallocateBuffers();
                    //std::cout << "Done freeing...\n";
                    //break;
                }
            }
        }
        lock.lock();
    }
    /*std::cout << fmt::format(
        "Number of available write buffers for thread {} after free: {}\n",
        threadId.getRawValue(),
        freeWriteBuffers[threadId.getRawValue()].size());*/
    writeMemoryPoolCondition[threadId.getRawValue()].wait(
        lock, [this, threadId] { return !freeWriteBuffers[threadId.getRawValue()].empty(); });

    char* buffer = freeWriteBuffers[threadId.getRawValue()].back();
    freeWriteBuffers[threadId.getRawValue()].pop_back();
    //std::cout << "Got write buffer\n";
    return buffer;
}

void MemoryPool::deallocateWriteBuffer(char* buffer, WorkerThreadId threadId)
{
    if (fileDescriptorBufferSize == 0 or buffer == nullptr or writeMemoryPool.data() > buffer
        or buffer >= writeMemoryPool.data() + writeMemoryPool.capacity())
    {
        return;
    }

    //std::cout << "Deallocating write buffer\n";
    const std::scoped_lock lock(writeMemoryPoolMutex[threadId.getRawValue()]);
    freeWriteBuffers[threadId.getRawValue()].push_back(buffer);
    //std::cout << "Deallocated write buffer\n";
    writeMemoryPoolCondition[threadId.getRawValue()].notify_one();
}

char* MemoryPool::allocateReadBuffer()
{
    if (fileDescriptorBufferSize == 0)
    {
        return nullptr;
    }

    //std::cout << "Getting read buffer\n";
    std::unique_lock lock(readMemoryPoolMutex);
    readMemoryPoolCondition.wait(lock, [this] { return !freeReadBuffers.empty(); });

    char* buffer = freeReadBuffers.back();
    freeReadBuffers.pop_back();
    //std::cout << "Got read buffer\n";
    return buffer;
}

void MemoryPool::deallocateReadBuffer(char* buffer)
{
    if (fileDescriptorBufferSize == 0 or buffer == nullptr or readMemoryPool.data() > buffer
        or buffer >= readMemoryPool.data() + readMemoryPool.capacity())
    {
        return;
    }

    const std::scoped_lock lock(readMemoryPoolMutex);
    freeReadBuffers.push_back(buffer);
    readMemoryPoolCondition.notify_one();
}

uint64_t MemoryPool::getFileDescriptorBufferSize() const
{
    return fileDescriptorBufferSize;
}

}
