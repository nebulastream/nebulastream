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

#include <Execution/Operators/SliceStore/FileStorage/FileStorage.hpp>
#include <Execution/Operators/SliceStore/Slice.hpp>

namespace NES::Runtime::Execution
{

class MemoryController
{
public:
    MemoryController() = default;

    static FileWriter getLeftFileWriter(SliceEnd sliceEnd, WorkerThreadId threadId);
    static FileWriter getRightFileWriter(SliceEnd sliceEnd, WorkerThreadId threadId);

    static FileReader getLeftFileReader(SliceEnd sliceEnd, WorkerThreadId threadId);
    static FileReader getRightFileReader(SliceEnd sliceEnd, WorkerThreadId threadId);

private:
    //std::map<SliceEnd, std::string> filePathForSlice;

    // IO-Auslastung
    // StatisticsEngine
    // CompressionController (Tuples evtl. komprimieren)
    // MemoryProvider (für RAM) und FileComponent (für SSD)
    // MemoryLayout (evtl. Anpassen?)
};
}
