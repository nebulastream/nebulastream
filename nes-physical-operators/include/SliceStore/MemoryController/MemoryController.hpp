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
#include <Identifiers/Identifiers.hpp>
#include <Join/StreamJoinUtil.hpp>
#include <SliceStore/FileDescriptor/FileDescriptors.hpp>
#include <SliceStore/Slice.hpp>

namespace NES
{

class MemoryController
{
public:
    MemoryController(size_t bufferSize, uint64_t numWorkerThreads, std::filesystem::path workingDir, QueryId queryId, OriginId originId);
    ~MemoryController();

    std::shared_ptr<FileWriter>
    getFileWriter(SliceEnd sliceEnd, WorkerThreadId threadId, JoinBuildSideType joinBuildSide, boost::asio::io_context& ioCtx);
    std::shared_ptr<FileReader> getFileReader(SliceEnd sliceEnd, WorkerThreadId threadId, JoinBuildSideType joinBuildSide);

    void setFileLayout(SliceEnd sliceEnd, WorkerThreadId threadId, JoinBuildSideType joinBuildSide, FileLayout fileLayout);
    std::optional<FileLayout> getFileLayout(SliceEnd sliceEnd, WorkerThreadId threadId, JoinBuildSideType joinBuildSide);
    void deleteFileLayout(SliceEnd sliceEnd, WorkerThreadId threadId, JoinBuildSideType joinBuildSide);

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

    size_t bufferSize;

    std::vector<char> writeMemoryPool;
    std::vector<char*> freeWriteBuffers;
    std::condition_variable writeMemoryPoolCondition;
    std::mutex writeMemoryPoolMutex;

    std::vector<char> readMemoryPool;
    std::vector<char*> freeReadBuffers;
    std::condition_variable readMemoryPoolCondition;
    std::mutex readMemoryPoolMutex;

    /// FileWriters are grouped by thread id thus removing the necessity of locks altogether
    std::vector<std::map<std::pair<SliceEnd, JoinBuildSideType>, std::shared_ptr<FileWriter>>> fileWriters;
    std::vector<std::mutex> fileWriterMutexes;

    /// FileLayouts are grouped by thread id thus reducing the wait time to acquire locks
    std::vector<std::map<std::pair<SliceEnd, JoinBuildSideType>, FileLayout>> fileLayouts;
    std::vector<std::mutex> fileLayoutMutexes;

    std::filesystem::path workingDir;
    QueryId queryId;
    OriginId originId;
};

}
