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
#include <Identifiers/Identifiers.hpp>
#include <Join/StreamJoinUtil.hpp>
#include <SliceStore/FileDescriptor/FileDescriptors.hpp>
#include <SliceStore/Slice.hpp>

namespace NES
{

struct MemoryControllerInfo
{
    uint64_t fileDescriptorBufferSize;
    uint64_t fileDescriptorGenerationRate;
    std::filesystem::path workingDir;
    QueryId queryId;
    OriginId outputOriginId;
};

class MemoryController
{
public:
    MemoryController(uint64_t numWorkerThreads, MemoryControllerInfo  memoryControllerInfo);
    ~MemoryController();

    std::shared_ptr<FileWriter> getFileWriter(
        boost::asio::io_context& ioCtx,
        SliceEnd sliceEnd,
        WorkerThreadId threadId,
        JoinBuildSideType joinBuildSide,
        bool forceWrite,
        bool checkExistence);
    std::shared_ptr<FileReader> getFileReader(SliceEnd sliceEnd, WorkerThreadId threadId, JoinBuildSideType joinBuildSide);

    void deleteSliceFiles(SliceEnd sliceEnd);

private:
    /// We need a multiple of 2 buffers as we might need to separate keys and payload depending on the used FileLayout
    static constexpr auto POOL_SIZE_MULTIPLIER = 2UL;

    std::string constructFilePath(SliceEnd sliceEnd, WorkerThreadId threadId, JoinBuildSideType joinBuildSide) const;

    void
    removeFileSystem(std::map<std::pair<SliceEnd, JoinBuildSideType>, std::shared_ptr<FileWriter>>::iterator it, WorkerThreadId threadId);

    char* allocateWriteBuffer();
    void deallocateWriteBuffer(char* buffer);
    char* allocateReadBuffer();
    void deallocateReadBuffer(char* buffer);

    std::vector<char> writeMemoryPool;
    std::vector<char*> freeWriteBuffers;
    std::condition_variable writeMemoryPoolCondition;
    std::mutex writeMemoryPoolMutex;

    std::vector<char> readMemoryPool;
    std::vector<char*> freeReadBuffers;
    std::condition_variable readMemoryPoolCondition;
    std::mutex readMemoryPoolMutex;

    /// FileWriters are grouped by thread id thus removing the necessity of locks altogether
    std::vector<std::set<std::string>> allFileWriters;
    std::vector<std::map<std::pair<SliceEnd, JoinBuildSideType>, std::shared_ptr<FileWriter>>> fileWriters;
    std::vector<std::mutex> fileWriterMutexes;

    ///
    long fileDescriptorLimit;
    std::atomic_uint64_t fileWriterCount;

    MemoryControllerInfo memoryControllerInfo;
};

}
