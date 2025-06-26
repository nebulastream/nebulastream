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
#include <SliceStore/MemoryController/MemoryController.hpp>
#include <sys/statvfs.h>

namespace NES
{

MemoryController::MemoryController(MemoryControllerInfo memoryControllerInfo, const uint64_t numWorkerThreads)
    : memoryControllerInfo(std::move(memoryControllerInfo))
    , memoryPool(this->memoryControllerInfo.fileDescriptorBufferSize, this->memoryControllerInfo.numberOfBuffersPerWorker, numWorkerThreads)
{
    /// Memory pool adjusts the buffer size to account for separate key and payload buffers
    this->memoryControllerInfo.fileDescriptorBufferSize = memoryPool.getFileDescriptorBufferSize();

    /// We need one additional thread for initializing the slice store, i.e. measuring execution times
    allFileWriters.resize(numWorkerThreads + 1);
    fileWriters.resize(numWorkerThreads + 1);
    fileWriterMutexes = std::vector<std::mutex>(numWorkerThreads + 1);

    /// Update maxNumFileDescriptors according to the systems supported maximum number of file descriptors and create limiter
    if (this->memoryControllerInfo.maxNumFileDescriptors > 0)
    {
        /// Subtract number of worker threads to be able to create file readers at any time
        this->memoryControllerInfo.maxNumFileDescriptors
            = setFileDescriptorLimit(this->memoryControllerInfo.maxNumFileDescriptors - numWorkerThreads);
    }
    limiter = std::make_unique<AtomicLimiter>(this->memoryControllerInfo.maxNumFileDescriptors);
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
    boost::asio::io_context& ioCtx,
    const SliceEnd sliceEnd,
    const WorkerThreadId threadId,
    const JoinBuildSideType joinBuildSide,
    const bool forceWrite,
    const bool checkExistence)
{
    /// Return immediately if there is no capacity to create a new file descriptor
    if (not(forceWrite ? limiter->acquireToken() : limiter->tryAcquireToken()))
    {
        return nullptr;
    }

    /*if (fileWriterCount % 1000 == 0)
    {
        std::cout << fmt::format("File descriptor count: {}\n", fileWriterCount.load());
    }*/
    /*if (std::cmp_greater_equal(fileWriterCount.load(), fileDescriptorLimit))
    {
        std::cout << "File descriptor limit is reached\n";
        return nullptr;
    }*/
    /// Search for matching fileWriter to avoid attempting to open a file twice
    auto& allWritersMap = allFileWriters[threadId.getRawValue()];
    auto& writerMap = fileWriters[threadId.getRawValue()];
    const std::scoped_lock lock(fileWriterMutexes[threadId.getRawValue()]);
    if (const auto it = writerMap.find({sliceEnd, joinBuildSide}); it != writerMap.end())
    {
        return it->second;
    }

    //++fileWriterCount;
    const auto& filePath = constructFilePath(sliceEnd, threadId, joinBuildSide);
    if (checkExistence and allWritersMap.contains(filePath))
    {
        //throw std::runtime_error("Requested FileWriter is not unique");
        //--fileWriterCount;
        std::cout << "Requested FileWriter is not unique\n";
        return nullptr;
    }
    /*{
        struct statvfs buffer;
        if (statvfs(filePath.c_str(), &buffer) != 0)
        {
            --fileWriterCount;
            std::cout << "Error getting file system information\n";
            return nullptr;
        }
        std::cout << fmt::format("Number of free inodes: {}\n", buffer.f_ffree);
    }*/
    auto fileWriter = std::make_shared<FileWriter>(
        ioCtx,
        filePath,
        [this] { return memoryPool.allocateWriteBuffer(); },
        [this](char* buf) { memoryPool.deallocateWriteBuffer(buf); },
        [this] { limiter->decrementTokenCounter(); },
        memoryControllerInfo.fileDescriptorBufferSize);
    if (not fileWriter->initialize())
    {
        //--fileWriterCount;
        return nullptr;
    }
    allWritersMap.emplace(filePath);
    writerMap[{sliceEnd, joinBuildSide}] = fileWriter;
    return fileWriter;
}

std::shared_ptr<FileReader> MemoryController::getFileReader(
    const SliceEnd sliceEnd, const WorkerThreadId threadId, const JoinBuildSideType joinBuildSide, const bool forceRead)
{
    (void)forceRead;

    /// Erase matching fileWriter as the data must not be amended after being read. This also enforces reading data only once
    auto& writerMap = fileWriters[threadId.getRawValue()];
    const std::scoped_lock lock(fileWriterMutexes[threadId.getRawValue()]);
    if (const auto it = writerMap.find({sliceEnd, joinBuildSide}); it != writerMap.end())
    {
        /// Return immediately if there is no capacity to create a new file descriptor
        /*if (not(forceRead ? limiter->acquireToken() : limiter->tryAcquireToken()))
        {
            return nullptr;
        }*/

        writerMap.erase(it);
        const auto& filePath = constructFilePath(sliceEnd, threadId, joinBuildSide);
        return std::make_shared<FileReader>(
            filePath,
            [this] { return memoryPool.allocateReadBuffer(); },
            [this](char* buf) { memoryPool.deallocateReadBuffer(buf); },
            //[this] { limiter->decrementTokenCounter(); },
            [] {},
            memoryControllerInfo.fileDescriptorBufferSize);
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

void MemoryController::removeFileSystem(
    const std::map<std::pair<SliceEnd, JoinBuildSideType>, std::shared_ptr<FileWriter>>::iterator it, const WorkerThreadId threadId)
{
    //limiter->acquireToken();

    /// Files are deleted in FileReader destructor
    const auto filePath = constructFilePath(it->first.first, threadId, it->first.second);
    fileWriters[threadId.getRawValue()].erase(it);
    const FileReader fileReader(
        filePath,
        [] { return nullptr; },
        [](char*) {},
        //[this] { limiter->decrementTokenCounter(); },
        [] {},
        0);
}

MemoryPool::MemoryPool(const uint64_t bufferSize, const uint64_t numBuffersPerWorker, const uint64_t numWorkerThreads)
    : fileDescriptorBufferSize(bufferSize / POOL_SIZE_MULTIPLIER)
{
    if (fileDescriptorBufferSize > 0)
    {
        // TODO add option for pool size
        const auto writePoolSize = std::max(1UL, numBuffersPerWorker) * numWorkerThreads * POOL_SIZE_MULTIPLIER;
        const auto readPoolSize = std::max(1UL, numBuffersPerWorker) * numWorkerThreads * POOL_SIZE_MULTIPLIER;

        writeMemoryPool.reserve(fileDescriptorBufferSize * writePoolSize);
        for (size_t i = 0; i < writePoolSize; ++i)
        {
            freeWriteBuffers.push_back(writeMemoryPool.data() + i * fileDescriptorBufferSize);
        }
        readMemoryPool.reserve(fileDescriptorBufferSize * readPoolSize);
        for (size_t i = 0; i < readPoolSize; ++i)
        {
            freeReadBuffers.push_back(readMemoryPool.data() + i * fileDescriptorBufferSize);
        }
    }
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

char* MemoryPool::allocateWriteBuffer()
{
    if (fileDescriptorBufferSize == 0)
    {
        return nullptr;
    }

    //std::cout << "Getting write buffer\n";
    std::unique_lock lock(writeMemoryPoolMutex);
    //std::cout << fmt::format("Number of available write buffers: {}\n", freeWriteBuffers.size());
    writeMemoryPoolCondition.wait(lock, [this] { return !freeWriteBuffers.empty(); });

    char* buffer = freeWriteBuffers.back();
    freeWriteBuffers.pop_back();
    //std::cout << "Got write buffer\n";
    return buffer;
}

void MemoryPool::deallocateWriteBuffer(char* buffer)
{
    if (fileDescriptorBufferSize == 0 or buffer == nullptr or writeMemoryPool.data() > buffer
        or buffer >= writeMemoryPool.data() + writeMemoryPool.capacity())
    {
        return;
    }

    //std::cout << "Deallocating write buffer\n";
    const std::scoped_lock lock(writeMemoryPoolMutex);
    freeWriteBuffers.push_back(buffer);
    //std::cout << "Deallocated write buffer\n";
    writeMemoryPoolCondition.notify_one();
}

uint64_t MemoryPool::getFileDescriptorBufferSize() const
{
    return fileDescriptorBufferSize;
}

RateLimiter::RateLimiter(const uint64_t rate) : rate(rate), tokens(rate), lastUpdate(std::chrono::high_resolution_clock::now())
{
}

bool RateLimiter::acquireToken()
{
    if (rate == 0)
    {
        return true;
    }
    const std::scoped_lock lock(mutex);

    while (true)
    {
        const auto now = std::chrono::high_resolution_clock::now();
        const auto timeElapsed = std::chrono::duration_cast<std::chrono::milliseconds>(now - lastUpdate).count() / 1000.0;
        tokens += timeElapsed * rate;
        tokens = std::min(tokens, static_cast<double>(rate));
        lastUpdate = now;

        if (tokens >= 1)
        {
            tokens -= 1;
            return true;
        }
        auto sleepTime = std::chrono::milliseconds(static_cast<int64_t>((1 - tokens) / rate * 1000.0));
        std::this_thread::sleep_for(sleepTime);
    }
}

bool RateLimiter::tryAcquireToken()
{
    if (rate == 0)
    {
        return true;
    }
    const std::scoped_lock lock(mutex);

    const auto now = std::chrono::high_resolution_clock::now();
    const auto timeElapsed = std::chrono::duration_cast<std::chrono::milliseconds>(now - lastUpdate).count() / 1000.0;
    tokens += timeElapsed * rate;
    tokens = std::min(tokens, static_cast<double>(rate));
    lastUpdate = now;

    if (tokens >= 1)
    {
        tokens -= 1;
        return true;
    }
    return false;
}

AtomicLimiter::AtomicLimiter(const uint64_t limit) : limit(limit), counter(0)
{
}

bool AtomicLimiter::acquireToken()
{
    if (limit == 0)
    {
        return true;
    }
    while (true)
    {
        if (counter.fetch_add(1, std::memory_order_relaxed) < limit)
        {
            return true;
        }
        /// Revert increment if limit is exceeded and leep for a short duration to prevent busy waiting
        counter.fetch_sub(1, std::memory_order_relaxed);
        std::this_thread::sleep_for(std::chrono::milliseconds(25));
    }
}

bool AtomicLimiter::tryAcquireToken()
{
    if (limit == 0)
    {
        return true;
    }
    if (uint64_t currentValue = counter.load(std::memory_order_relaxed); currentValue < limit)
    {
        // Use compare-exchange to ensure atomic increment
        return counter.compare_exchange_weak(currentValue, currentValue + 1);
    }
    return false;
}

void AtomicLimiter::decrementTokenCounter()
{
    counter.fetch_sub(1, std::memory_order_relaxed);
}

uint64_t AtomicLimiter::getTokenCounter()
{
    return counter;
}

}
