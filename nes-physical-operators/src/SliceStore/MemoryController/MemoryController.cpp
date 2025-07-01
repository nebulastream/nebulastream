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

#include <ranges>
#include <SliceStore/MemoryController/MemoryController.hpp>

namespace NES
{

MemoryController::MemoryController(
    MemoryControllerInfo memoryControllerInfo,
    const uint64_t numWorkerThreads,
    const uint64_t minNumFileDescriptorsPerWorker,
    const uint64_t memoryPoolSizeMultiplier)
    : memoryControllerInfo(std::move(memoryControllerInfo))
    , memoryPool(
          this->memoryControllerInfo.fileDescriptorBufferSize,
          this->memoryControllerInfo.numberOfBuffersPerWorker,
          numWorkerThreads,
          memoryPoolSizeMultiplier)
{
    /// Memory pool adjusts the buffer size to account for separate key and payload buffers
    this->memoryControllerInfo.fileDescriptorBufferSize = memoryPool.getFileDescriptorBufferSize();

    /// Create file writer map per thread thus reducing resource contention
    threadWriters = std::vector<ThreadLocalWriters>(numWorkerThreads);

    /// Update maxNumFileDescriptors according to the system's supported maximum number of file descriptors and create limiter
    if (this->memoryControllerInfo.maxNumFileDescriptors > 0)
    {
        this->memoryControllerInfo.maxNumFileDescriptors = setFileDescriptorLimit(this->memoryControllerInfo.maxNumFileDescriptors);
        /// Each worker thread must be allowed to have minNumFileDescriptorsPerWorker many file writers and readers at once
        if (this->memoryControllerInfo.maxNumFileDescriptors < 2 * numWorkerThreads * minNumFileDescriptorsPerWorker)
        {
            throw std::runtime_error("Not enough file descriptors available for the specified number of worker threads");
        }

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
}

MemoryController::~MemoryController()
{
    // TODO investigate why destructor is never called
    for (auto& [writers, _, mutex] : threadWriters)
    {
        const std::scoped_lock lock(mutex);
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
    const std::scoped_lock lock(mutex);

    /// Search for matching writer to avoid attempting to open a file twice and update LRU
    if (const auto it = writers.find(key); it != writers.end())
    {
        if (auto& [writer, listIt] = it->second; writer != nullptr)
        {
            lruQueue.erase(listIt);
            lruQueue.push_front(key);
            listIt = lruQueue.begin();
            return writer;
        }
    }

    /// Close file descriptors if needed
    if (memoryControllerInfo.maxNumFileDescriptors > 0 and lruQueue.size() >= memoryControllerInfo.maxNumFileDescriptors)
    {
        for (auto i = 0UL; i < std::min(1UL, lruQueue.size()); ++i)
        {
            const auto evictKey = lruQueue.back();
            deleteFileWriter(threadWriters[threadId.getRawValue()], evictKey);
            writers[evictKey] = {nullptr, lruQueue.end()};
        }
    }

    /*const auto totalNumDescriptors
        = std::distance(std::filesystem::directory_iterator("/proc/self/fd"), std::filesystem::directory_iterator{});
    std::cout << fmt::format(
        "Number of file descriptors for thread {} is {} and total num used is {}\n",
        threadId.getRawValue(),
        lruQueue.size(),
        totalNumDescriptors);*/

    const auto writer = std::make_shared<FileWriter>(
        ioCtx,
        constructFilePath(sliceEnd, threadId, joinBuildSide),
        [this, threadId](const FileWriter* fw)
        { return memoryPool.allocateWriteBuffer(threadId, threadWriters[threadId.getRawValue()], fw); },
        [this, threadId](char* buf) { memoryPool.deallocateWriteBuffer(threadId, buf); },
        memoryControllerInfo.fileDescriptorBufferSize);

    lruQueue.push_front(key);
    writers[key] = {writer, lruQueue.begin()};
    return writer;
}

std::optional<std::shared_ptr<FileReader>>
MemoryController::getFileReader(const SliceEnd sliceEnd, const WorkerThreadId threadToRead, const WorkerThreadId workerThread, const JoinBuildSideType joinBuildSide)
{
    auto& [writers, lruQueue, mutex] = threadWriters[threadToRead.getRawValue()];
    const auto key = std::make_pair(sliceEnd, joinBuildSide);
    const std::scoped_lock lock(mutex);

    /// Erase matching fileWriter as the data must not be amended after being read. This also enforces reading data only once
    auto foundWriter = false;
    if (const auto it = writers.find(key); it != writers.end())
    {
        if (auto& [writer, listIt] = it->second; writer != nullptr)
        {
            lruQueue.erase(listIt);
        }
        writers.erase(it);
        foundWriter = true;
    }

    /// Create reader after writer is out of scope to ensure staying below the given file descriptor limit
    return foundWriter ? std::make_optional(
                             std::make_shared<FileReader>(
                                 constructFilePath(sliceEnd, threadToRead, joinBuildSide),
                                 //memoryPool.allocateReadBuffer(threadId, false),
                                 //memoryPool.allocateReadBuffer(threadId, true),
                                 [this, workerThread] { return memoryPool.allocateReadBuffer(workerThread, false); },
                                 [this, workerThread](char* buf) { memoryPool.deallocateReadBuffer(workerThread, buf); },
                                 memoryControllerInfo.fileDescriptorBufferSize))
                       : std::nullopt;
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
    return (memoryControllerInfo.workingDir
            / std::filesystem::path(
                fmt::format(
                    "memory_controller_{}_{}_{}_{}_{}",
                    memoryControllerInfo.queryId.getRawValue(),
                    memoryControllerInfo.outputOriginId.getRawValue(),
                    magic_enum::enum_name(joinBuildSide),
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
        lruQueue.erase(listIt);
        writers.erase(it);
        return writer;
    }
    return {};
}

MemoryPool::MemoryPool(
    const uint64_t bufferSize, const uint64_t numBuffersPerWorker, const uint64_t numWorkerThreads, const uint64_t poolSizeMultiplier)
    : fileDescriptorBufferSize(bufferSize / poolSizeMultiplier), poolSizeMultiplier(poolSizeMultiplier)
{
    (void)this->poolSizeMultiplier;
    if (numBuffersPerWorker == 0)
    {
        fileDescriptorBufferSize = 0;
    }
    if (fileDescriptorBufferSize > 0)
    {
        const auto writePoolSize = numBuffersPerWorker * numWorkerThreads * poolSizeMultiplier;
        writeMemoryPool.reserve(fileDescriptorBufferSize * writePoolSize);
        writeMemoryPoolMutexes = std::vector<std::mutex>(numWorkerThreads);
        freeWriteBuffers.resize(numWorkerThreads);

        for (size_t i = 0; i < writePoolSize; ++i)
        {
            freeWriteBuffers[i % numWorkerThreads].push_back(writeMemoryPool.data() + i * fileDescriptorBufferSize);
        }

        /// We only need poolSizeMultiplier many read buffers per worker as we always destroy the reader immediately after use
        const auto readPoolSize = numWorkerThreads * poolSizeMultiplier;
        readMemoryPool.reserve(fileDescriptorBufferSize * readPoolSize);
        readMemoryPoolConditions = std::vector<std::condition_variable>(numWorkerThreads);
        readMemoryPoolMutexes = std::vector<std::mutex>(numWorkerThreads);
        freeReadBuffers.resize(numWorkerThreads);
        for (size_t i = 0; i < readPoolSize; ++i)
        {
            freeReadBuffers[i % numWorkerThreads].push_back(readMemoryPool.data() + i * fileDescriptorBufferSize);
            //freeReadBuffers.push_back(readMemoryPool.data() + i * fileDescriptorBufferSize);
        }
    }
}

char* MemoryPool::allocateWriteBuffer(const WorkerThreadId threadId, ThreadLocalWriters& threadWriters, const FileWriter* writer)
{
    if (fileDescriptorBufferSize == 0)
    {
        return nullptr;
    }

    std::unique_lock lock(writeMemoryPoolMutexes[threadId.getRawValue()]);
    if (freeWriteBuffers[threadId.getRawValue()].empty())
    {
        lock.unlock();
        auto& [writers, lruQueue, mutex] = threadWriters;
        const std::scoped_lock innerLock(mutex);

        auto numWritersToDeallocate = 1UL;
        for (auto keyIt = lruQueue.rbegin(); keyIt != lruQueue.rend(); ++keyIt)
        {
            if (const auto it = writers.find(*keyIt); it != writers.end())
            {
                const auto& [curWriter, _] = it->second;
                if (curWriter->hasBuffer() and curWriter.get() != writer)
                {
                    curWriter->flushAndDeallocateBuffers();
                    if (--numWritersToDeallocate <= 0)
                    {
                        break;
                    }
                }
            }
        }
        lock.lock();
        //std::cout << fmt::format(
        //    "Number of buffers available for thread {}: {}\n", threadId.getRawValue(), freeWriteBuffers[threadId.getRawValue()].size());
    }

    char* buffer = freeWriteBuffers[threadId.getRawValue()].back();
    freeWriteBuffers[threadId.getRawValue()].pop_back();
    return buffer;
}

void MemoryPool::deallocateWriteBuffer(const WorkerThreadId threadId, char* buffer)
{
    if (fileDescriptorBufferSize == 0 or buffer == nullptr or writeMemoryPool.data() > buffer
        or buffer >= writeMemoryPool.data() + writeMemoryPool.capacity())
    {
        return;
    }

    const std::scoped_lock lock(writeMemoryPoolMutexes[threadId.getRawValue()]);
    freeWriteBuffers[threadId.getRawValue()].push_back(buffer);
}

char* MemoryPool::allocateReadBuffer(const WorkerThreadId threadId, const bool keyBuffer)
{
    (void)threadId;
    (void)keyBuffer;
    if (fileDescriptorBufferSize == 0)
    {
        return nullptr;
    }

    //std::cout << "Getting read buffer\n";
    std::unique_lock lock(readMemoryPoolMutexes[threadId.getRawValue()]);
    if (freeReadBuffers[threadId.getRawValue()].empty())
    {
        std::cout << fmt::format("No read buffers available for thread {}\n", threadId.getRawValue());
    }
    readMemoryPoolConditions[threadId.getRawValue()].wait(
        lock, [this, threadId] { return !freeReadBuffers[threadId.getRawValue()].empty(); });

    char* buffer = freeReadBuffers[threadId.getRawValue()].back();
    freeReadBuffers[threadId.getRawValue()].pop_back();
    //std::cout << "Got read buffer\n";
    return buffer;
    //auto* const buffer = readMemoryPool.data() + threadId.getRawValue() * fileDescriptorBufferSize * poolSizeMultiplier;
    //return keyBuffer ? buffer + fileDescriptorBufferSize : buffer;
}

void MemoryPool::deallocateReadBuffer(const WorkerThreadId threadId, char* buffer)
{
    if (fileDescriptorBufferSize == 0 or buffer == nullptr or readMemoryPool.data() > buffer
        or buffer >= readMemoryPool.data() + readMemoryPool.capacity())
    {
        return;
    }

    const std::scoped_lock lock(readMemoryPoolMutexes[threadId.getRawValue()]);
    freeReadBuffers[threadId.getRawValue()].push_back(buffer);
    readMemoryPoolConditions[threadId.getRawValue()].notify_one();
}

uint64_t MemoryPool::getFileDescriptorBufferSize() const
{
    return fileDescriptorBufferSize;
}

}
