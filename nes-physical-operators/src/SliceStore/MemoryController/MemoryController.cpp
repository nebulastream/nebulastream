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

MemoryController::MemoryController(
    MemoryControllerInfo memoryControllerInfo, const uint64_t numWorkerThreads, const uint64_t minNumFileDescriptorsPerWorker)
    : memoryControllerInfo(std::move(memoryControllerInfo))
    , memoryPool(this->memoryControllerInfo.fileDescriptorBufferSize, this->memoryControllerInfo.numberOfBuffersPerWorker, numWorkerThreads)
{
    /// Memory pool adjusts the buffer size to account for separate key and payload buffers
    this->memoryControllerInfo.fileDescriptorBufferSize = memoryPool.getFileDescriptorBufferSize();

    /// Create file writer map per thread to reduce the time waiting for mutexes
    allFileWriters.resize(numWorkerThreads);
    fileWriters.resize(numWorkerThreads);
    fileWriterMutexes = std::vector<std::mutex>(numWorkerThreads);

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
    std::cout << fmt::format("Number of available file descriptors: {}\n", limiter->getNumAvailableFileWriters());
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
        [this, threadId] { return memoryPool.allocateWriteBuffer(threadId); },
        [this, threadId](char* buf) { memoryPool.deallocateWriteBuffer(buf, threadId); },
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
            filePath,
            [this] { return memoryPool.allocateReadBuffer(); },
            [this](char* buf) { memoryPool.deallocateReadBuffer(buf); },
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
    const std::unordered_map<std::pair<SliceEnd, JoinBuildSideType>, std::shared_ptr<FileWriter>>::iterator it,
    const WorkerThreadId threadId)
{
    /// Files are deleted in FileReader destructor
    const auto filePath = constructFilePath(it->first.first, threadId, it->first.second);
    fileWriters[threadId.getRawValue()].erase(it);
    const FileReader fileReader(filePath, [] { return nullptr; }, [](char*) {}, 0);
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
        freeWriteBuffers.resize(numWorkerThreads);

        for (size_t i = 0; i < writePoolSize; ++i)
        {
            freeWriteBuffers[i % numWorkerThreads].push_back(writeMemoryPool.data() + i * fileDescriptorBufferSize);
        }

        readMemoryPool.reserve(fileDescriptorBufferSize * readPoolSize);
        for (size_t i = 0; i < readPoolSize; ++i)
        {
            freeReadBuffers.push_back(readMemoryPool.data() + i * fileDescriptorBufferSize);
        }
    }
}

char* MemoryPool::allocateWriteBuffer(const WorkerThreadId threadId)
{
    if (fileDescriptorBufferSize == 0)
    {
        return nullptr;
    }

    //std::cout << "Getting write buffer\n";
    std::unique_lock lock(writeMemoryPoolMutex[threadId.getRawValue()]);
    //std::cout << fmt::format("Number of available write buffers: {}\n", freeWriteBuffers.size());
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
