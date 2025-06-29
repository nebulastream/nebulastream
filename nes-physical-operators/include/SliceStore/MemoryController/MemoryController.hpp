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

struct ThreadLocalWriters
{
    using WriterKey = std::pair<SliceEnd, JoinBuildSideType>;
    std::map<WriterKey, std::pair<std::shared_ptr<FileWriter>, std::list<WriterKey>::iterator>> writers;
    std::list<WriterKey> lruQueue;
    std::mutex mutex;
};

class MemoryPool
{
public:
    MemoryPool(uint64_t bufferSize, uint64_t numBuffersPerWorker, uint64_t numWorkerThreads);

    char* allocateWriteBuffer(std::vector<ThreadLocalWriters>& threadWriters, const FileWriter* writer, WorkerThreadId threadId);
    void deallocateWriteBuffer(char* buffer, WorkerThreadId threadId);
    char* allocateReadBuffer();
    void deallocateReadBuffer(char* buffer);

    uint64_t getFileDescriptorBufferSize() const;

private:
    /// We need a multiple of 2 buffers as we might need to separate keys and payload depending on the used FileLayout
    static constexpr auto POOL_SIZE_MULTIPLIER = 2UL;

    std::vector<char> writeMemoryPool;
    std::vector<std::vector<char*>> freeWriteBuffers;
    //std::vector<std::condition_variable> writeMemoryPoolCondition;
    std::vector<std::mutex> writeMemoryPoolMutex;

    std::vector<char> readMemoryPool;
    std::vector<char*> freeReadBuffers;
    std::condition_variable readMemoryPoolCondition;
    std::mutex readMemoryPoolMutex;

    uint64_t fileDescriptorBufferSize;
};

// TODO rename FileDescriptorManager
class MemoryController
{
public:
    MemoryController(MemoryControllerInfo memoryControllerInfo, uint64_t numWorkerThreads, uint64_t minNumFileDescriptorsPerWorker);
    ~MemoryController();

    std::shared_ptr<FileWriter>
    getFileWriter(boost::asio::io_context& ioCtx, SliceEnd sliceEnd, WorkerThreadId threadId, JoinBuildSideType joinBuildSide);
    void unlockFileWriter(WorkerThreadId threadId);
    std::shared_ptr<FileReader> getFileReader(SliceEnd sliceEnd, WorkerThreadId threadId, JoinBuildSideType joinBuildSide);

    void deleteSliceFiles(SliceEnd sliceEnd);

private:
    [[nodiscard]] std::string constructFilePath(SliceEnd sliceEnd, WorkerThreadId threadId, JoinBuildSideType joinBuildSide) const;

    static std::optional<std::shared_ptr<FileWriter>> deleteFileWriter(ThreadLocalWriters& local, const ThreadLocalWriters::WriterKey& key);

    /// Writers are grouped by thread thus reducing resource contention
    std::vector<ThreadLocalWriters> threadWriters;

    MemoryControllerInfo memoryControllerInfo;
    MemoryPool memoryPool;
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
