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

#include <Execution/Operators/SliceStore/FileDescriptors/FileDescriptors.hpp>
#include <Execution/Operators/SliceStore/Slice.hpp>
#include <Util/Execution.hpp>

namespace NES::Runtime::Execution
{

using FileWriterId = std::tuple<SliceEnd, WorkerThreadId, QueryCompilation::JoinBuildSideType>;

class MemoryController
{
public:
    MemoryController(size_t bufferSize, size_t poolSize, const std::filesystem::path& workingDir, QueryId queryId, OriginId originId);
    MemoryController(MemoryController& other);
    MemoryController(MemoryController&& other) noexcept;
    MemoryController& operator=(MemoryController& other);
    MemoryController& operator=(MemoryController&& other) noexcept;
    ~MemoryController();

    std::shared_ptr<FileWriter>
    getFileWriter(SliceEnd sliceEnd, WorkerThreadId threadId, QueryCompilation::JoinBuildSideType joinBuildSide);
    std::shared_ptr<FileReader>
    getFileReader(SliceEnd sliceEnd, WorkerThreadId threadId, QueryCompilation::JoinBuildSideType joinBuildSide);

    void
    setFileLayout(SliceEnd sliceEnd, WorkerThreadId threadId, QueryCompilation::JoinBuildSideType joinBuildSide, FileLayout fileLayout);
    std::optional<FileLayout> getFileLayout(SliceEnd sliceEnd, WorkerThreadId threadId, QueryCompilation::JoinBuildSideType joinBuildSide);
    void deleteFileLayout(SliceEnd sliceEnd, WorkerThreadId threadId, QueryCompilation::JoinBuildSideType joinBuildSide);

    void deleteSliceFiles(SliceEnd sliceEnd);

private:
    // TODO add a real read pool as reading is not necessarily done single threaded with predictive read
    static constexpr auto NUM_READ_BUFFERS = 2;

    std::string constructFilePath(SliceEnd sliceEnd, QueryCompilation::JoinBuildSideType joinBuildSide) const;
    std::string constructFilePath(SliceEnd sliceEnd, WorkerThreadId threadId, QueryCompilation::JoinBuildSideType joinBuildSide) const;

    void removeFileSystem(std::map<std::string, std::shared_ptr<FileWriter>>::iterator it);

    char* allocateReadBuffer();
    char* allocateBuffer();
    void deallocateBuffer(char* buffer);

    std::vector<char> readBuffer;
    std::atomic<bool> readBufFlag;

    std::vector<char> memoryPool;
    std::vector<char*> freeBuffers;
    std::condition_variable memoryPoolCondition;
    std::mutex memoryPoolMutex;
    size_t bufferSize;
    size_t poolSize;

    // TODO build vector around maps to structure by WorkerThreadId (use less locks)
    std::map<std::string, std::shared_ptr<FileWriter>> fileWriters;
    std::mutex fileWritersMutex;

    std::map<FileWriterId, FileLayout> fileLayouts;
    std::mutex fileLayoutsMutex;

    std::filesystem::path workingDir;
    QueryId queryId;
    OriginId originId;
    // IO-Auslastung
    // StatisticsEngine
    // CompressionController (Tuples evtl. komprimieren)
    // MemoryProvider (für RAM) und FileComponent (für SSD)
    // MemoryLayout (evtl. Anpassen?)
};

}
