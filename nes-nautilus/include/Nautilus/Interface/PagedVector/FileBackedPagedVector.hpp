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
#include <Nautilus/Interface/PagedVector/PagedVector.hpp>
#include <Util/Execution.hpp>

namespace NES::Nautilus::Interface
{

class FileBackedPagedVector : PagedVector
{
public:
    FileBackedPagedVector() = default;

    /// Writes the projected fields of all tuples to fileStorage.
    static void writeToFile(
        PagedVector* pagedVector,
        PagedVector* pagedVectorKeys,
        Memory::AbstractBufferProvider* bufferProvider,
        const Memory::MemoryLayouts::MemoryLayout* memoryLayout,
        Runtime::Execution::FileWriter& fileWriter,
        Runtime::Execution::FileLayout fileLayout);

    /// Reads the projected fields of all tuples from fileStorage.
    static void readFromFile(
        PagedVector* pagedVector,
        PagedVector* pagedVectorKeys,
        Memory::AbstractBufferProvider* bufferProvider,
        const Memory::MemoryLayouts::MemoryLayout* memoryLayout,
        Runtime::Execution::FileReader& fileReader,
        Runtime::Execution::FileLayout fileLayout);

    /// Deletes the projected fields of all tuples.
    static void truncate(PagedVector* pagedVector, PagedVector* pagedVectorKeys, Runtime::Execution::FileLayout fileLayout);

private:
    static void writePayloadOnlyToFile(
        PagedVector* pagedVector,
        const Memory::MemoryLayouts::MemoryLayout* memoryLayout,
        Memory::AbstractBufferProvider* bufferProvider,
        PagedVector* pagedVectorKeys,
        Runtime::Execution::FileWriter& fileWriter);

    static void writePayloadAndKeysToSeparateFiles(
        PagedVector* pagedVector,
        const Memory::MemoryLayouts::MemoryLayout* memoryLayout,
        PagedVector* pagedVectorKeys,
        Runtime::Execution::FileWriter& fileWriter);

    static void readSeparatelyFromFiles(
        PagedVector* pagedVector,
        const Memory::MemoryLayouts::MemoryLayout* memoryLayout,
        Memory::AbstractBufferProvider* bufferProvider,
        PagedVector* pagedVectorKeys,
        Runtime::Execution::FileReader& fileReader);

    std::vector<Memory::TupleBuffer> keyPages;
};

}
