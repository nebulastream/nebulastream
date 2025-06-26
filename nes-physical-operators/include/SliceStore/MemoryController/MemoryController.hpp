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

#pragma once

#include <map>
#include <set>
#include <Configurations/Worker/QueryOptimizerConfiguration.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Join/StreamJoinUtil.hpp>
#include <SliceStore/FileDescriptor/FileDescriptors.hpp>
#include <SliceStore/Slice.hpp>
#include <sys/resource.h>

namespace NES
{

struct MemoryControllerInfo
{
    uint64_t maxNumFileDescriptors;
    uint64_t fileDescriptorBufferSize;
    uint64_t numberOfBuffersPerWorker;
    std::filesystem::path workingDir;
    QueryId queryId;
    OriginId outputOriginId;
};

class MemoryPool
{
public:
    MemoryPool(uint64_t bufferSize, uint64_t numBuffersPerWorker, uint64_t numWorkerThreads);

    char* allocateWriteBuffer();
    void deallocateWriteBuffer(char* buffer);
    char* allocateReadBuffer();
    void deallocateReadBuffer(char* buffer);

    uint64_t getFileDescriptorBufferSize() const;

private:
    /// We need a multiple of 2 buffers as we might need to separate keys and payload depending on the used FileLayout
    static constexpr auto POOL_SIZE_MULTIPLIER = 2UL;

    std::vector<char> writeMemoryPool;
    std::vector<char*> freeWriteBuffers;
    std::condition_variable writeMemoryPoolCondition;
    std::mutex writeMemoryPoolMutex;

    std::vector<char> readMemoryPool;
    std::vector<char*> freeReadBuffers;
    std::condition_variable readMemoryPoolCondition;
    std::mutex readMemoryPoolMutex;

    uint64_t fileDescriptorBufferSize;
};

class AbstractLimiter
{
public:
    virtual ~AbstractLimiter() = default;
    virtual bool acquireToken() = 0;
    virtual bool tryAcquireToken() = 0;
    virtual void decrementTokenCounter() = 0;
    virtual uint64_t getTokenCounter() = 0;
};

class RateLimiter final : AbstractLimiter
{
public:
    explicit RateLimiter(uint64_t rate);

    bool acquireToken() override;
    bool tryAcquireToken() override;
    void decrementTokenCounter() override { }
    uint64_t getTokenCounter() override { return 0; }

private:
    const uint64_t rate;
    double tokens;
    std::chrono::system_clock::time_point lastUpdate;
    std::mutex mutex;
};

class AtomicLimiter final : AbstractLimiter
{
public:
    explicit AtomicLimiter(uint64_t limit);

    bool acquireToken() override;
    bool tryAcquireToken() override;
    void decrementTokenCounter() override;
    uint64_t getTokenCounter() override;

private:
    const uint64_t limit;
    std::atomic_uint64_t counter;
};

class MemoryController
{
public:
    MemoryController(MemoryControllerInfo memoryControllerInfo, uint64_t numWorkerThreads);
    ~MemoryController();

    std::shared_ptr<FileWriter> getFileWriter(
        boost::asio::io_context& ioCtx,
        SliceEnd sliceEnd,
        WorkerThreadId threadId,
        JoinBuildSideType joinBuildSide,
        bool forceWrite,
        bool checkExistence);
    std::shared_ptr<FileReader> getFileReader(SliceEnd sliceEnd, WorkerThreadId threadId, JoinBuildSideType joinBuildSide, bool forceRead);

    void deleteSliceFiles(SliceEnd sliceEnd);

private:
    std::string constructFilePath(SliceEnd sliceEnd, WorkerThreadId threadId, JoinBuildSideType joinBuildSide) const;

    void
    removeFileSystem(std::map<std::pair<SliceEnd, JoinBuildSideType>, std::shared_ptr<FileWriter>>::iterator it, WorkerThreadId threadId);

    /// FileWriters are grouped by thread id thus removing the necessity of locks altogether
    std::vector<std::set<std::string>> allFileWriters;
    std::vector<std::map<std::pair<SliceEnd, JoinBuildSideType>, std::shared_ptr<FileWriter>>> fileWriters;
    std::vector<std::mutex> fileWriterMutexes;

    MemoryControllerInfo memoryControllerInfo;
    MemoryPool memoryPool;
    std::unique_ptr<AtomicLimiter> limiter;
};

inline uint64_t setFileDescriptorLimit(uint64_t limit)
{
    rlimit rlp;
    if (getrlimit(RLIMIT_NOFILE, &rlp) == -1)
    {
        std::cerr << "Failed to get the file descriptor limit.\n";
        return NES::Configurations::QueryOptimizerConfiguration().maxNumFileDescriptors.getDefaultValue();
    }
    limit = std::min(rlp.rlim_max, limit);

    rlp.rlim_cur = limit;
    if (setrlimit(RLIMIT_NOFILE, &rlp) == -1)
    {
        std::cerr << "Failed to set the file descriptor limit.\n";
        return NES::Configurations::QueryOptimizerConfiguration().maxNumFileDescriptors.getDefaultValue();
    }

    std::cout << fmt::format("Maximum number of file descriptors: {}\n", limit);
    return limit;
}

}
