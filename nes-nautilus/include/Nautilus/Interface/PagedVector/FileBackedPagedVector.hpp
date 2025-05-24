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

#include <Nautilus/Interface/PagedVector/PagedVector.hpp>
#include "../../nes-physical-operators/include/SliceStore/FileDescriptor/FileDescriptors.hpp"

namespace NES::Nautilus::Interface
{

class FileBackedPagedVector final : public PagedVector
{
public:
    /// Appends the pages of the given FileBackedPagedVector with the pages of this FileBackedPagedVector.
    void appendAllPages(PagedVector& other) override;

    /// Copies all pages from other FileBackedPagedVector to this FileBackedPagedVector.
    void copyFrom(const PagedVector& other) override;

    /// Writes the projected fields of all tuples to fileStorage.
    boost::asio::awaitable<void> writeToFile(
        Memory::AbstractBufferProvider* bufferProvider,
        const Memory::MemoryLayouts::MemoryLayout* memoryLayout,
        std::shared_ptr<FileWriter> fileWriter,
        FileLayout fileLayout);

    /// Reads the projected fields of all tuples from fileStorage.
    void readFromFile(
        Memory::AbstractBufferProvider* bufferProvider,
        const Memory::MemoryLayouts::MemoryLayout* memoryLayout,
        FileReader& fileReader,
        FileLayout fileLayout);

    /// Deletes the projected fields of all tuples.
    void truncate(FileLayout fileLayout);

    [[nodiscard]] uint64_t getTotalNumberOfEntries() const override;
    [[nodiscard]] uint64_t getNumberOfPages() const override;

    [[nodiscard]] uint64_t getNumberOfTuplesOnDisk() const;

private:
    /// Appends a new page to the keyPages vector if the last page is full.
    void appendKeyPageIfFull(Memory::AbstractBufferProvider* bufferProvider, const Memory::MemoryLayouts::MemoryLayout* memoryLayout);

    boost::asio::awaitable<void>
    writePayloadAndKeysToSeparateFiles(const Memory::MemoryLayouts::MemoryLayout* memoryLayout, std::shared_ptr<FileWriter> fileWriter);

    boost::asio::awaitable<void> writePayloadOnlyToFile(
        const Memory::MemoryLayouts::MemoryLayout* memoryLayout,
        Memory::AbstractBufferProvider* bufferProvider,
        std::shared_ptr<FileWriter> fileWriter);

    void readSeparatelyFromFiles(
        const Memory::MemoryLayouts::MemoryLayout* memoryLayout, Memory::AbstractBufferProvider* bufferProvider, FileReader& fileReader);

    /// As we allow tuples to be partially written to disk, i.e. only key field data is kept in memory, the remaining data subsequently has
    /// a different schema/memory layout and must therefore be stored separately from any new incoming tuples.
    std::vector<Memory::TupleBuffer> keyPages;

    uint64_t numTuplesOnDisk = 0;
};

}
