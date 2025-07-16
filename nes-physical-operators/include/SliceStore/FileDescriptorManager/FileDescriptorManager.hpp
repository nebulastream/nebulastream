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

#include <list>
#include <map>
#include <Identifiers/Identifiers.hpp>
#include <Join/StreamJoinUtil.hpp>
#include <SliceStore/FileDescriptor/FileDescriptors.hpp>
#include <SliceStore/Slice.hpp>

namespace NES
{

struct FileDescriptorManagerInfo
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
    MemoryPool(uint64_t bufferSize, uint64_t numBuffersPerWorker, uint64_t numWorkerThreads, uint64_t poolSizeMultiplier);

    char* allocateWriteBuffer(WorkerThreadId threadId, ThreadLocalWriters& threadWriters, const FileWriter* writer);
    void deallocateWriteBuffer(WorkerThreadId threadId, char* buffer);
    char* getReadBufferForThread(WorkerThreadId threadId, bool keyBuffer);

    uint64_t getFileDescriptorBufferSize() const;

private:
    std::vector<char> writeMemoryPool;
    std::vector<std::vector<char*>> freeWriteBuffers;
    std::vector<std::mutex> writeMemoryPoolMutexes;

    std::vector<char> readMemoryPool;

    uint64_t fileDescriptorBufferSize;
    uint64_t poolSizeMultiplier;
};

class FileDescriptorManager
{
public:
    FileDescriptorManager(
        FileDescriptorManagerInfo memoryControllerInfo,
        uint64_t numWorkerThreads,
        uint64_t minNumFileDescriptorsPerWorker,
        uint64_t memoryPoolSizeMultiplier);
    ~FileDescriptorManager();

    std::shared_ptr<FileWriter>
    getFileWriter(boost::asio::io_context& ioCtx, SliceEnd sliceEnd, WorkerThreadId threadId, JoinBuildSideType joinBuildSide);
    std::optional<std::shared_ptr<FileReader>> getFileReader(
        SliceEnd sliceEnd, WorkerThreadId threadToRead, WorkerThreadId workerThread, JoinBuildSideType joinBuildSide, bool cleanup);

    void deleteSliceFiles(SliceEnd sliceEnd);

private:
    [[nodiscard]] std::string constructFilePath(SliceEnd sliceEnd, WorkerThreadId threadId, JoinBuildSideType joinBuildSide) const;

    static std::optional<std::shared_ptr<FileWriter>> deleteFileWriter(ThreadLocalWriters& local, const ThreadLocalWriters::WriterKey& key);

    static uint64_t setAndGetFileDescriptorLimit(uint64_t limit);

    /// Writers are grouped by thread thus reducing resource contention
    std::vector<ThreadLocalWriters> threadWriters;

    FileDescriptorManagerInfo fileDescriptorManagerInfo;
    MemoryPool memoryPool;
};

}
