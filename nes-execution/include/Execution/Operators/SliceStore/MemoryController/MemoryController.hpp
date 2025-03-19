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

class MemoryController
{
public:
    static constexpr auto USE_PIPELINE_ID = false;
    static constexpr auto BUFFER_SIZE = 0; // 4 KB buffer size
    static constexpr auto POOL_SIZE = 1024 * 10; // 10 K pool size

    MemoryController(const std::filesystem::path& workingDir, QueryId queryId, OriginId originId);
    MemoryController(const MemoryController& other);
    MemoryController(MemoryController&& other) noexcept;
    MemoryController& operator=(const MemoryController& other);
    MemoryController& operator=(MemoryController&& other) noexcept;
    ~MemoryController();

    std::shared_ptr<FileWriter>
    getFileWriter(SliceEnd sliceEnd, WorkerThreadId threadId, PipelineId pipelineId, QueryCompilation::JoinBuildSideType joinBuildSide);
    std::shared_ptr<FileReader>
    getFileReader(SliceEnd sliceEnd, WorkerThreadId threadId, QueryCompilation::JoinBuildSideType joinBuildSide);

    void deleteSliceFiles(SliceEnd sliceEnd);

private:
    static constexpr auto NUM_READ_BUFFERS = 2;

    std::filesystem::path
    constructFilePath(SliceEnd sliceEnd, WorkerThreadId threadId, QueryCompilation::JoinBuildSideType joinBuildSide) const;

    std::shared_ptr<FileWriter> getFileWriterFromMap(const std::string& filePath);
    std::shared_ptr<FileReader> getFileReaderAndEraseWriter(const std::string& filePath);

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

    std::map<std::string, std::shared_ptr<FileWriter>> fileWriters;
    std::mutex fileWritersMutex;

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
