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

    MemoryController() = default;
    MemoryController(const MemoryController& other);
    MemoryController(MemoryController&& other) noexcept;
    MemoryController& operator=(const MemoryController& other);
    MemoryController& operator=(MemoryController&& other) noexcept;

    std::shared_ptr<FileWriter>
    getFileWriter(SliceEnd sliceEnd, PipelineId pipelineId, WorkerThreadId threadId, QueryCompilation::JoinBuildSideType joinBuildSide);
    std::shared_ptr<FileReader> getFileReader(SliceEnd sliceEnd, PipelineId pipelineId, QueryCompilation::JoinBuildSideType joinBuildSide);

private:
    std::shared_ptr<FileWriter> getFileWriterFromMap(const std::string& filePath);
    std::shared_ptr<FileReader> getFileReaderAndEraseWriter(const std::string& filePath);

    std::map<std::string, std::shared_ptr<FileWriter>> fileWriters;
    std::mutex mutex_;

    // IO-Auslastung
    // StatisticsEngine
    // CompressionController (Tuples evtl. komprimieren)
    // MemoryProvider (für RAM) und FileComponent (für SSD)
    // MemoryLayout (evtl. Anpassen?)
};
}
