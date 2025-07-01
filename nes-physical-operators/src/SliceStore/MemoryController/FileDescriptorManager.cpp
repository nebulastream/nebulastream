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
#include <Configurations/Worker/QueryOptimizerConfiguration.hpp>
#include <SliceStore/FileDescriptorManager/FileDescriptorManager.hpp>

namespace NES
{

FileDescriptorManager::FileDescriptorManager(
    FileDescriptorManagerInfo memoryControllerInfo,
    const uint64_t numWorkerThreads,
    const uint64_t minNumFileDescriptorsPerWorker,
    const uint64_t memoryPoolSizeMultiplier)
    : fileDescriptorManagerInfo(std::move(memoryControllerInfo))
    , memoryPool(
          this->fileDescriptorManagerInfo.fileDescriptorBufferSize,
          this->fileDescriptorManagerInfo.numberOfBuffersPerWorker,
          numWorkerThreads,
          memoryPoolSizeMultiplier)
{
    /// Memory pool adjusts the buffer size to account for separate key and payload buffers
    this->fileDescriptorManagerInfo.fileDescriptorBufferSize = memoryPool.getFileDescriptorBufferSize();

    /// Create file writer map per thread thus reducing resource contention
    threadWriters = std::vector<ThreadLocalWriters>(numWorkerThreads);

    /// Update maxNumFileDescriptors according to the system's supported maximum number of file descriptors and create limiter
    if (this->fileDescriptorManagerInfo.maxNumFileDescriptors > 0)
    {
        this->fileDescriptorManagerInfo.maxNumFileDescriptors
            = setAndGetFileDescriptorLimit(this->fileDescriptorManagerInfo.maxNumFileDescriptors);
        /// Each worker thread must be allowed to have minNumFileDescriptorsPerWorker many file writers and readers at once
        if (this->fileDescriptorManagerInfo.maxNumFileDescriptors < 2 * numWorkerThreads * minNumFileDescriptorsPerWorker)
        {
            throw std::runtime_error("Not enough file descriptors available for the specified number of worker threads");
        }

        /// Subtract number of worker threads multiplied by number of descriptors per worker to be able to create file readers at any time
        this->fileDescriptorManagerInfo.maxNumFileDescriptors -= numWorkerThreads * minNumFileDescriptorsPerWorker;
        /// Divide by number of descriptors per worker as each created file writer potentially needs to open this amount of descriptors
        this->fileDescriptorManagerInfo.maxNumFileDescriptors /= minNumFileDescriptorsPerWorker;
        /// Divide by number of worker threads as we enforce a limit on each worker separately
        this->fileDescriptorManagerInfo.maxNumFileDescriptors /= numWorkerThreads;
    }
    else
    {
        /// Set the descriptor limit to the system's maximum amount while preserving the value of maxNumFileDescriptors to disable limiter
        setAndGetFileDescriptorLimit(UINT64_MAX);
    }
}

FileDescriptorManager::~FileDescriptorManager()
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

std::shared_ptr<FileWriter> FileDescriptorManager::getFileWriter(
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
    if (fileDescriptorManagerInfo.maxNumFileDescriptors > 0 and lruQueue.size() >= fileDescriptorManagerInfo.maxNumFileDescriptors)
    {
        // TODO close files in batches?
        for (auto i = 0UL; i < std::min(1UL, lruQueue.size()); ++i)
        {
            const auto evictKey = lruQueue.back();
            deleteFileWriter(threadWriters[threadId.getRawValue()], evictKey);
            writers[evictKey] = {nullptr, lruQueue.end()};
        }
    }

    const auto writer = std::make_shared<FileWriter>(
        ioCtx,
        constructFilePath(sliceEnd, threadId, joinBuildSide),
        [this, threadId](const FileWriter* fw)
        { return memoryPool.allocateWriteBuffer(threadId, threadWriters[threadId.getRawValue()], fw); },
        [this, threadId](char* buf) { memoryPool.deallocateWriteBuffer(threadId, buf); },
        fileDescriptorManagerInfo.fileDescriptorBufferSize);

    lruQueue.push_front(key);
    writers[key] = {writer, lruQueue.begin()};
    return writer;
}

std::optional<std::shared_ptr<FileReader>> FileDescriptorManager::getFileReader(
    const SliceEnd sliceEnd, const WorkerThreadId threadToRead, const WorkerThreadId workerThread, const JoinBuildSideType joinBuildSide)
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
    return foundWriter ? std::make_optional(std::make_shared<FileReader>(
                             constructFilePath(sliceEnd, threadToRead, joinBuildSide),
                             memoryPool.getReadBufferForThread(workerThread, false),
                             memoryPool.getReadBufferForThread(workerThread, true),
                             fileDescriptorManagerInfo.fileDescriptorBufferSize))
                       : std::nullopt;
}

void FileDescriptorManager::deleteSliceFiles(const SliceEnd sliceEnd)
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

std::string FileDescriptorManager::constructFilePath(
    const SliceEnd sliceEnd, const WorkerThreadId threadId, const JoinBuildSideType joinBuildSide) const
{
    return (fileDescriptorManagerInfo.workingDir
            / std::filesystem::path(fmt::format(
                "memory_controller_{}_{}_{}_{}_{}",
                fileDescriptorManagerInfo.queryId.getRawValue(),
                fileDescriptorManagerInfo.outputOriginId.getRawValue(),
                magic_enum::enum_name(joinBuildSide),
                sliceEnd.getRawValue(),
                threadId.getRawValue())))
        .string();
}

std::optional<std::shared_ptr<FileWriter>>
FileDescriptorManager::deleteFileWriter(ThreadLocalWriters& local, const std::pair<SliceEnd, JoinBuildSideType>& key)
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

uint64_t FileDescriptorManager::setAndGetFileDescriptorLimit(uint64_t limit)
{
    /// Reserve descriptors for any file that was already opened and add a buffer as insurance for future descriptors that might be opened
    const auto numOpenDescriptors
        = std::distance(std::filesystem::directory_iterator("/proc/self/fd"), std::filesystem::directory_iterator{}) + 10UL;

    rlimit rlp;
    if (getrlimit(RLIMIT_NOFILE, &rlp) == -1)
    {
        std::cerr << "Failed to get the file descriptor limit.\n";
        return NES::Configurations::QueryOptimizerConfiguration().maxNumFileDescriptors.getDefaultValue() - numOpenDescriptors;
    }
    limit = std::min(rlp.rlim_max, limit);

    rlp.rlim_cur = limit;
    if (setrlimit(RLIMIT_NOFILE, &rlp) == -1)
    {
        std::cerr << "Failed to set the file descriptor limit.\n";
        return NES::Configurations::QueryOptimizerConfiguration().maxNumFileDescriptors.getDefaultValue() - numOpenDescriptors;
    }

    return limit - numOpenDescriptors;
}

MemoryPool::MemoryPool(
    const uint64_t bufferSize, const uint64_t numBuffersPerWorker, const uint64_t numWorkerThreads, const uint64_t poolSizeMultiplier)
    : fileDescriptorBufferSize(bufferSize / poolSizeMultiplier), poolSizeMultiplier(poolSizeMultiplier)
{
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

        /// We only need poolSizeMultiplier many read buffers per worker thread as we always destroy the reader immediately after use
        const auto readPoolSize = numWorkerThreads * poolSizeMultiplier;
        readMemoryPool.reserve(fileDescriptorBufferSize * readPoolSize);
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

        // TODO deallocate buffers in batches?
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

char* MemoryPool::getReadBufferForThread(const WorkerThreadId threadId, const bool keyBuffer)
{
    if (fileDescriptorBufferSize == 0)
    {
        return nullptr;
    }

    auto* const buffer = readMemoryPool.data() + threadId.getRawValue() * fileDescriptorBufferSize * poolSizeMultiplier;
    return keyBuffer ? buffer + fileDescriptorBufferSize : buffer;
}

uint64_t MemoryPool::getFileDescriptorBufferSize() const
{
    return fileDescriptorBufferSize;
}

}
