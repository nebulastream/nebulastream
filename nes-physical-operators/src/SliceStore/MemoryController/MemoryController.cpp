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
#include <absl/strings/internal/str_format/extension.h>
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
    for (const auto& [writers, lru, mutex] : threadWriters)
    {
        for (const auto& [writer, _] : writers | std::views::values)
        {
            writer->deleteAllFiles();
        }
    }
}

std::shared_ptr<FileWriter> MemoryController::getFileWriter(
    boost::asio::io_context& ioCtx, const SliceEnd sliceEnd, const WorkerThreadId threadId, const JoinBuildSideType joinBuildSide)
{
    auto& [writers, lruQueue, mutex] = threadWriters[threadId.getRawValue()];
    const auto key = std::make_pair(sliceEnd, joinBuildSide);
    //mutex.lock();
    const std::scoped_lock lock(mutex);

    /// Search for matching writer to avoid attempting to open a file twice and update LRU if needed
    if (const auto it = writers.find(key); it != writers.end() and it->second.first != nullptr)
    {
        const auto sizeOfLRU = lruQueue.size();
        auto& [writer, listIt] = it->second;
        if (memoryControllerInfo.maxNumFileDescriptors > 0)
        {
            //if (const auto pos = std::ranges::find(lru, key); pos != lru.end())
            {
                //lru.erase(pos);
            }
            //else
            {
                //std::cout << fmt::format("ERROR: Size of LRU for thread {}: {}\n", threadId.getRawValue(), lru.size());
            }
            lruQueue.erase(listIt);
            lruQueue.push_front(key);
            listIt = lruQueue.begin();
        }
        if (writers.find(key)->second.second != lruQueue.begin())
            throw std::runtime_error("UPS!!");
        if (sizeOfLRU != lruQueue.size())
            throw std::runtime_error("FUUUUCK");
        return writer;
    }

    /*const auto totalNumDescriptors
        = std::distance(std::filesystem::directory_iterator("/proc/self/fd"), std::filesystem::directory_iterator{});
    std::cout << fmt::format(
        "Number of file descriptors for thread {} is {} and total num used is {}\n",
        threadId.getRawValue(),
        memoryControllerInfo.maxNumFileDescriptors - lru.size(),
        totalNumDescriptors);*/
    auto writer = std::make_shared<FileWriter>(
        ioCtx,
        constructFilePath(sliceEnd, threadId, joinBuildSide),
        [this, threadId](const FileWriter* obj) { return memoryPool.allocateWriteBuffer(threadWriters, obj, threadId); },
        [this, threadId](char* buf) { memoryPool.deallocateWriteBuffer(buf, threadId); },
        memoryControllerInfo.fileDescriptorBufferSize);

    lruQueue.push_front(key);
    writers[key] = std::make_pair(writer, lruQueue.begin());
    return writer;
}

std::shared_ptr<FileReader>
MemoryController::getFileReader(const SliceEnd sliceEnd, const WorkerThreadId threadId, const JoinBuildSideType joinBuildSide)
{
    auto& [writers, lruQueue, mutex] = threadWriters[threadId.getRawValue()];
    const auto key = std::make_pair(sliceEnd, joinBuildSide);
    const std::scoped_lock lock(mutex);

    /// Erase matching fileWriter as the data must not be amended after being read. This also enforces reading data only once
    if (const auto it = writers.find(key); it != writers.end())
    {
        if (memoryControllerInfo.maxNumFileDescriptors > 0 and it->second.second != lruQueue.end())
        {
            //if (const auto pos = std::ranges::find(lru, key); pos != lru.end())
            {
                //lru.erase(it->second.second);
            }
            if (it->second.first.use_count() != 1)
                throw std::runtime_error("FileWriter should not be used anymore!");
            lruQueue.erase(it->second.second);
        }
        //it->second.first->flushAndDeallocateBuffers();
        writers.erase(it);

        return std::make_shared<FileReader>(
            constructFilePath(sliceEnd, threadId, joinBuildSide),
            [this] { return memoryPool.allocateReadBuffer(); },
            [this](char* buf) { memoryPool.deallocateReadBuffer(buf); },
            memoryControllerInfo.fileDescriptorBufferSize);
    }

    return nullptr;
}

void MemoryController::closeFileDescriptors(const WorkerThreadId threadId, const uint64_t numFileDescriptorsToClose)
{
    auto& [writers, lruQueue, mutex] = threadWriters[threadId.getRawValue()];

    if (memoryControllerInfo.maxNumFileDescriptors > 0)
    {
        const std::scoped_lock lock(mutex);
        /// Close file descriptor if needed
        //if (lruQueue.size() >= memoryControllerInfo.maxNumFileDescriptors)
        {
            std::cout << fmt::format("Number of file descriptors before deleting: {}\n", lruQueue.size());
            for (auto i = 0UL; i < std::min(numFileDescriptorsToClose, lruQueue.size()); ++i)
            //while (not lruQueue.empty())
            {
                //std::cout << fmt::format(
                //    "Closing oldest descriptor for thread {} and size of LRU {}...\n", threadId.getRawValue(), lru.size());
                const auto evictKey = lruQueue.back();
                /*if (evictKey == key)
                {
                    throw std::runtime_error("Fucking hell shit1");
                }*/
                const auto& local = deleteFileWriter(threadWriters[threadId.getRawValue()], evictKey);
                if (not local.has_value())
                {
                    throw std::runtime_error("Fucking hell shit");
                }
                writers[evictKey] = {nullptr, lruQueue.end()};
                /*std::cout << fmt::format(
                    "Closed oldest descriptor for thread {} and size of LRU {} and useCount {}...\n",
                    threadId.getRawValue(),
                    lru.size(),
                    local.value().use_count());*/
            }
            std::cout << fmt::format("Number of file descriptors after deleting: {}\n", lruQueue.size());
        }
    }
}

boost::asio::awaitable<void> MemoryController::closeFileDescriptorsAsync(const WorkerThreadId threadId, uint64_t numFileDescriptorsToClose)
{
    auto& [writers, lruQueue, mutex] = threadWriters[threadId.getRawValue()];

    if (memoryControllerInfo.maxNumFileDescriptors > 0)
    {
        /// Close file descriptor if needed
        //if (lruQueue.size() >= memoryControllerInfo.maxNumFileDescriptors)
        {
            uint64_t queueSize;
            {
                const std::scoped_lock lock(mutex);
                queueSize = lruQueue.size();
            }
            //std::cout << fmt::format("Number of file descriptors before deleting: {}\n", lruQueue.size());
            numFileDescriptorsToClose = std::min(numFileDescriptorsToClose, queueSize);
            for (auto i = 0UL; i < numFileDescriptorsToClose; ++i)
            {
                std::shared_ptr<FileWriter> writer;
                {
                    const std::scoped_lock lock(mutex);
                    const auto evictKey = lruQueue.back();
                    writer = deleteFileWriter(threadWriters[threadId.getRawValue()], evictKey).value();
                    writers[evictKey] = {nullptr, lruQueue.end()};
                }
                co_await writer->flush();
            }
            //if (numFileDescriptorsToClose > 0)
                //std::cout << fmt::format("Closed {} file descriptors\n", numFileDescriptorsToClose);
            //std::cout << fmt::format("Number of file descriptors after deleting: {}\n", lruQueue.size());
        }
    }
}

boost::asio::awaitable<uint64_t>
MemoryController::closeFileDescriptorsIfNeededAsync(const WorkerThreadId threadId, const uint64_t numFileDescriptorsToOpen)
{
    auto& [writers, lruQueue, mutex] = threadWriters[threadId.getRawValue()];

    if (memoryControllerInfo.maxNumFileDescriptors > 0)
    {
        /// Close file descriptor if needed
        //if (lruQueue.size() >= memoryControllerInfo.maxNumFileDescriptors)
        {
            //std::cout << fmt::format("Number of file descriptors before deleting: {}\n", lruQueue.size());

            const auto numAvailableFileDescriptors = getNumAvailableFileDescriptors(threadId);
            const auto numFileDescriptorsNeededToClose
                = numFileDescriptorsToOpen > numAvailableFileDescriptors ? numFileDescriptorsToOpen - numAvailableFileDescriptors : 0UL;
            const auto numFileDescriptorsToClose = std::min(numFileDescriptorsNeededToClose, memoryControllerInfo.maxNumFileDescriptors);
            for (auto i = 0UL; i < numFileDescriptorsToClose; ++i)
            {
                std::shared_ptr<FileWriter> writer;
                {
                    const std::scoped_lock lock(mutex);
                    const auto evictKey = lruQueue.back();
                    writer = deleteFileWriter(threadWriters[threadId.getRawValue()], evictKey).value();
                    writers[evictKey] = {nullptr, lruQueue.end()};
                }
                co_await writer->flush();
            }
            if (numFileDescriptorsToClose > 0)
                //std::cout << fmt::format("Closed {} file descriptors\n", numFileDescriptorsToClose);
                //std::cout << fmt::format("Number of file descriptors after deleting: {}\n", lruQueue.size());
                std::cout << fmt::format(
                    "Number of file descriptors to open: {}, number of file descriptors needed to close: {}, number of file descriptors "
                    "closed: {}\n",
                    numFileDescriptorsToOpen,
                    numFileDescriptorsNeededToClose,
                    numFileDescriptorsToClose);
            co_return numFileDescriptorsNeededToClose > numFileDescriptorsToClose ? numFileDescriptorsToClose : numFileDescriptorsToOpen;
        }
    }
    co_return 0;
}

uint64_t MemoryController::getNumAvailableFileDescriptors(const WorkerThreadId threadId)
{
    auto& [_, lruQueue, mutex] = threadWriters[threadId.getRawValue()];
    const std::scoped_lock lock(mutex);
    return memoryControllerInfo.maxNumFileDescriptors - lruQueue.size();
}

void MemoryController::deleteSliceFiles(const SliceEnd sliceEnd)
{
    std::vector<std::shared_ptr<FileWriter>> writersToDelete;
    for (auto& local : threadWriters)
    {
        const std::scoped_lock lock(local.mutex);
        for (const auto& joinBuildSide : {JoinBuildSideType::Left, JoinBuildSideType::Right})
        {
            const auto& writer = deleteFileWriter(local, {sliceEnd, joinBuildSide});
            if (writer.has_value())
            {
                writersToDelete.emplace_back(writer.value());
            }
        }
    }

    for (const auto& writer : writersToDelete)
    {
        writer->deleteAllFiles();
    }
}

std::string
MemoryController::constructFilePath(const SliceEnd sliceEnd, const WorkerThreadId threadId, const JoinBuildSideType joinBuildSide) const
{
    const std::string sideStr = joinBuildSide == JoinBuildSideType::Left ? "left" : "right";
    return (memoryControllerInfo.workingDir
            / std::filesystem::path(fmt::format(
                "memory_controller_{}_{}_{}_{}_{}",
                memoryControllerInfo.queryId.getRawValue(),
                memoryControllerInfo.outputOriginId.getRawValue(),
                sideStr,
                sliceEnd.getRawValue(),
                threadId.getRawValue())))
        .string();
}

std::optional<std::shared_ptr<FileWriter>>
MemoryController::deleteFileWriter(ThreadLocalWriters& local, const std::pair<SliceEnd, JoinBuildSideType>& key)
{
    auto& [writers, lruQueue, _] = local;
    if (const auto it = writers.find(key); it != writers.end())
    {
        const auto [writer, listIt] = it->second;
        //if (memoryControllerInfo.maxNumFileDescriptors > 0)
        {
            //if (const auto pos = std::ranges::find(lru, key); pos != lru.end())
            {
                //lru.erase(dequeIt);
            }
            //else
            {
                //std::cout << fmt::format("ERROR: Size of LRU: {}\n", lru.size());
            }
        }
        //const auto oldWriter = writer;
        lruQueue.erase(listIt);
        writers.erase(it);
        //std::cout << fmt::format("Pointer use count: {}\n", writer.use_count());
        return writer;
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
        //writeMemoryPoolCondition = std::vector<std::condition_variable>(numWorkerThreads);
        writeMemoryPoolMutex = std::vector<std::mutex>(numWorkerThreads);
        freeWriteBuffers.resize(numWorkerThreads);
        for (size_t i = 0; i < writePoolSize; ++i)
        {
            const auto idx = numWorkerThreads == 1 ? 0 : i % numWorkerThreads;
            freeWriteBuffers[idx].push_back(writeMemoryPool.data() + i * fileDescriptorBufferSize);
        }

        readMemoryPool.reserve(fileDescriptorBufferSize * readPoolSize);
        freeReadBuffers.reserve(numWorkerThreads);
        for (size_t i = 0; i < readPoolSize; ++i)
        {
            freeReadBuffers.push_back(readMemoryPool.data() + i * fileDescriptorBufferSize);
        }
    }
}

char* MemoryPool::allocateWriteBuffer(std::vector<ThreadLocalWriters>&, const FileWriter*, const WorkerThreadId threadId)
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
        /*lock.unlock();
        auto& [writers, lruQueue, mutex] = threadWriters[threadId.getRawValue()];
        const std::scoped_lock innerLock(mutex);

        if (not lruQueue.empty())
        {
            const auto& key = lruQueue.back();
            if (const auto it = writers.find(key); it != writers.end())
            {
                std::cout << "Freeing...\n";
                it->second->flushAndDeallocateBuffers();
            }
            else
            {
                std::cout << "No file descriptor found\n";
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
        lock.lock();*/
        std::cout << fmt::format("No buffers available for thread {}\n", threadId.getRawValue());
    }
    /*std::cout << fmt::format(
        "Number of available write buffers for thread {} after free: {}\n",
        threadId.getRawValue(),
        freeWriteBuffers[threadId.getRawValue()].size());*/
    //writeMemoryPoolCondition[threadId.getRawValue()].wait(
    //    lock, [this, threadId] { return !freeWriteBuffers[threadId.getRawValue()].empty(); });

    char* buffer = freeWriteBuffers[threadId.getRawValue()].back();
    freeWriteBuffers[threadId.getRawValue()].pop_back();
    //std::cout << "Got write buffer\n";
    return buffer;
}

void MemoryPool::deallocateWriteBuffer(char* buffer, const WorkerThreadId threadId)
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
    //writeMemoryPoolCondition[threadId.getRawValue()].notify_one();
}

char* MemoryPool::allocateReadBuffer()
{
    if (fileDescriptorBufferSize == 0)
    {
        return nullptr;
    }

    //std::cout << "Getting read buffer\n";
    std::unique_lock lock(readMemoryPoolMutex);
    if (freeReadBuffers.empty())
    {
        std::cout << "No read buffers available\n";
    }
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
