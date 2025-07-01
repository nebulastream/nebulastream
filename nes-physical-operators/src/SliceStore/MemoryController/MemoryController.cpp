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
    MemoryControllerInfo memoryControllerInfo, const uint64_t numWorkerThreads, const uint64_t minNumFileDescriptorsPerWorker)
    : memoryControllerInfo(std::move(memoryControllerInfo))
    , memoryPool(this->memoryControllerInfo.fileDescriptorBufferSize, this->memoryControllerInfo.numberOfBuffersPerWorker, numWorkerThreads)
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
        [this, threadId](const FileWriter* fw) { return memoryPool.allocateWriteBuffer(threadWriters, fw, threadId); },
        [this, threadId](char* buf) { memoryPool.deallocateWriteBuffer(buf, threadId); },
        memoryControllerInfo.fileDescriptorBufferSize);

    lruQueue.push_front(key);
    writers[key] = {writer, lruQueue.begin()};
    return writer;
}

std::optional<std::shared_ptr<FileReader>>
MemoryController::getFileReader(const SliceEnd sliceEnd, const WorkerThreadId threadId, const JoinBuildSideType joinBuildSide)
{
    auto& [writers, lruQueue, mutex] = threadWriters[threadId.getRawValue()];
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

    /// Create reader after writer is out of scope to ensure staying within the given file descriptor limit
    return foundWriter ? std::make_optional(
                             std::make_shared<FileReader>(
                                 constructFilePath(sliceEnd, threadId, joinBuildSide),
                                 [this] { return memoryPool.allocateReadBuffer(); },
                                 [this](char* buf) { memoryPool.deallocateReadBuffer(buf); },
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
            / std::filesystem::path(fmt::format(
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

char* MemoryPool::allocateWriteBuffer(
    std::vector<ThreadLocalWriters>& threadWriters, const FileWriter* writer, const WorkerThreadId threadId)
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
    //auto freeBuffers = [&lock, threadId, threadWriters, this, writer]()
    {
        lock.unlock();
        auto& [writers, lruQueue, mutex] = threadWriters[threadId.getRawValue()];
        const std::scoped_lock innerLock(mutex);

        //if (not lruQueue.empty())
        if (false)
        {
            //for (auto i = 0UL; i < std::min(UINT64_MAX, lruQueue.size()); ++i)
            for (const auto key : lruQueue)
            {
                //const auto key = lruQueue.back();
                if (const auto it = writers.find(key); it != writers.end())
                {
                    const auto& [curWriter, _] = it->second;
                    if (curWriter.get() != writer)
                        continue;
                    std::cout << "Freeing...\n";
                    curWriter->flushAndDeallocateBuffers();
                }
                else
                {
                    std::cout << "No file descriptor found\n";
                }
            }
        }
        else
        {
            std::cout << "Try Freeing...\n";
            for (const auto& [curWriter, _] : writers | std::views::values)
            {
                if (curWriter != nullptr and curWriter.get() != writer)
                //if (curWriter.get() != writer)
                {
                    //std::cout << "Freeing...\n";
                    curWriter->flushAndDeallocateBuffers();
                    //std::cout << "Done freeing...\n";
                    //break;
                }
            }
        }
        lock.lock();
        std::cout << fmt::format(
            "Number of buffers available for thread {}: {}\n", threadId.getRawValue(), freeWriteBuffers[threadId.getRawValue()].size());
    };
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
        if (buffer != nullptr)
            std::cout << "Shit fuck\n";
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
