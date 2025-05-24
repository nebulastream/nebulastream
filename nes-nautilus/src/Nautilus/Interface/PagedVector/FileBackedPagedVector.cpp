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

#include <Nautilus/Interface/PagedVector/FileBackedPagedVector.hpp>
#include <Util/Common.hpp>
#include <ErrorHandling.hpp>

namespace NES::Nautilus::Interface
{

void FileBackedPagedVector::appendAllPages(PagedVector& other)
{
    PagedVector::appendAllPages(other);
    if (Util::instanceOfConst<FileBackedPagedVector>(other))
    {
        Util::asConst<FileBackedPagedVector>(other).keyPages.clear();
    }
}

void FileBackedPagedVector::copyFrom(const PagedVector& other)
{
    PagedVector::copyFrom(other);
    if (Util::instanceOfConst<FileBackedPagedVector>(other))
    {
        const auto fbOther = Util::asConst<FileBackedPagedVector>(other);
        keyPages.insert(keyPages.end(), fbOther.keyPages.begin(), fbOther.keyPages.end());
        numTuplesOnDisk += fbOther.numTuplesOnDisk;
    }
}

boost::asio::awaitable<void> FileBackedPagedVector::writeToFile(
    Memory::AbstractBufferProvider* bufferProvider,
    const Memory::MemoryLayouts::MemoryLayout* memoryLayout,
    const std::shared_ptr<FileWriter> fileWriter,
    FileLayout fileLayout)
{
    // TODO remove once fileLayout is chosen adaptively
    fileLayout = !memoryLayout->getKeyFieldNames().empty() ? fileLayout : FileLayout::NO_SEPARATION;

    PRECONDITION(!memoryLayout->getSchema().containsVarSizedDataField(), "NLJSlice does not currently support variable sized data");
    PRECONDITION(
        !memoryLayout->getKeyFieldNames().empty() || fileLayout == FileLayout::NO_SEPARATION,
        "Cannot separate key field data and payload as there are no key fields");
    // TODO fix this method and remove preconditon
    /*PRECONDITION(
        memoryLayout->getSchema()->getLayoutType() != Schema::MemoryLayoutType::ROW_LAYOUT,
        "NLJSlice does not currently support any memory layout other than row layout");*/

    switch (fileLayout)
    {
        /// Write all tuples consecutivley to file
        case FileLayout::NO_SEPARATION_KEEP_KEYS:
        case FileLayout::NO_SEPARATION: {
            for (const auto& [_, page] : pages)
            {
                const auto numTuplesOnPage = page.getNumberOfTuples();
                co_await fileWriter->write(page.getBuffer(), numTuplesOnPage * memoryLayout->getTupleSize());
                numTuplesOnDisk += numTuplesOnPage;
            }
            break;
        }
        /// Write only payload to file and append key field data to designated pagedVectorKeys
        case FileLayout::SEPARATE_PAYLOAD: {
            co_await writePayloadOnlyToFile(memoryLayout, bufferProvider, fileWriter);
            break;
        }
        /// Write designated pagedVectorKeys to key file first and then remaining payload and key field data to separate files
        case FileLayout::SEPARATE_KEYS: {
            co_await writePayloadAndKeysToSeparateFiles(memoryLayout, fileWriter);
            break;
        }
    }
    co_return;
}

void FileBackedPagedVector::readFromFile(
    Memory::AbstractBufferProvider* bufferProvider,
    const Memory::MemoryLayouts::MemoryLayout* memoryLayout,
    FileReader& fileReader,
    FileLayout fileLayout)
{
    // TODO remove once fileLayout is chosen adaptively
    fileLayout = !memoryLayout->getKeyFieldNames().empty() ? fileLayout : FileLayout::NO_SEPARATION;

    PRECONDITION(!memoryLayout->getSchema().containsVarSizedDataField(), "NLJSlice does not currently support variable sized data");
    PRECONDITION(
        !memoryLayout->getKeyFieldNames().empty() || fileLayout == FileLayout::NO_SEPARATION,
        "Cannot separate key field data and payload as there are no key fields");
    // TODO fix this method and remove preconditon
    /*PRECONDITION(
        memoryLayout->getSchema()->getLayoutType() != Schema::MemoryLayoutType::ROW_LAYOUT,
        "NLJSlice does not currently support any memory layout other than row layout");*/

    switch (fileLayout)
    {
        /// Read all tuples consecutivley from file
        case FileLayout::NO_SEPARATION_KEEP_KEYS: {
            keyPages.clear();
        }
        case FileLayout::NO_SEPARATION: {
            // TODO just append new page disregarding the number of tuples on the last page?
            const auto tupleSize = memoryLayout->getTupleSize();
            appendPageIfFull(bufferProvider, memoryLayout);
            auto lastPage = pages.back().buffer;
            auto* lastPagePtr = lastPage.getBuffer() + lastPage.getNumberOfTuples() * tupleSize;
            auto tuplesToRead = memoryLayout->getCapacity() - lastPage.getNumberOfTuples();

            while (const auto bytesRead = fileReader.read(lastPagePtr, tuplesToRead * tupleSize))
            {
                const auto tuplesRead = bytesRead / tupleSize;
                lastPage.setNumberOfTuples(lastPage.getNumberOfTuples() + tuplesRead);
                appendPageIfFull(bufferProvider, memoryLayout);
                lastPage = pages.back().buffer;
                lastPagePtr = lastPage.getBuffer();
                tuplesToRead = memoryLayout->getCapacity();
                numTuplesOnDisk -= tuplesRead;
            }
            break;
        }
        /// Read payload and key field data from separate files first and then remaining designated pagedVectorKeys and payload from file
        case FileLayout::SEPARATE_PAYLOAD:
        case FileLayout::SEPARATE_KEYS: {
            readSeparatelyFromFiles(memoryLayout, bufferProvider, fileReader);
            break;
        }
    }
}

void FileBackedPagedVector::truncate(const FileLayout fileLayout)
{
    /// Clear the pages without appending a new one afterwards as this will be done in when writing a new record
    // TODO append key field data to designated pagedVectorKeys to be able to write all tuples to file but keep keys in memory, alternatively create mew FileLayout and do this in writeToFile()->writePayloadOnlyToFile()
    switch (fileLayout)
    {
        /// Append key field data to designated pagedVectorKeys
        case FileLayout::NO_SEPARATION_KEEP_KEYS: {
            // TODO
            break;
        }
        /// Do nothing as key field data has just been written to designated pagedVectorKeys
        case FileLayout::SEPARATE_PAYLOAD: {
            break;
        }
        /// Remove all key field data as a different FileLayout might have been used previously
        case FileLayout::NO_SEPARATION:
        case FileLayout::SEPARATE_KEYS: {
            keyPages.clear();
            break;
        }
    }
    pages.clear();
}

uint64_t FileBackedPagedVector::getTotalNumberOfEntries() const
{
    auto totalNumEntries = 0UL;
    for (const auto& page : keyPages)
    {
        totalNumEntries += page.getNumberOfTuples();
    }
    return totalNumEntries + PagedVector::getTotalNumberOfEntries();
}

uint64_t FileBackedPagedVector::getNumberOfPages() const
{
    return pages.size() + keyPages.size();
}

uint64_t FileBackedPagedVector::getNumberOfTuplesOnDisk() const
{
    return numTuplesOnDisk;
}

void FileBackedPagedVector::appendKeyPageIfFull(
    Memory::AbstractBufferProvider* bufferProvider, const Memory::MemoryLayouts::MemoryLayout* memoryLayout)
{
    PRECONDITION(bufferProvider != nullptr, "EntrySize for a pagedVector has to be larger than 0!");
    PRECONDITION(memoryLayout != nullptr, "EntrySize for a pagedVector has to be larger than 0!");
    PRECONDITION(memoryLayout->getTupleSize() > 0, "EntrySize for a pagedVector has to be larger than 0!");
    PRECONDITION(memoryLayout->getCapacity() > 0, "At least one tuple has to fit on a page!");

    if (keyPages.empty() || keyPages.back().getNumberOfTuples() >= memoryLayout->getCapacity())
    {
        if (auto page = bufferProvider->getUnpooledBuffer(memoryLayout->getBufferSize()); page.has_value())
        {
            keyPages.emplace_back(page.value());
        }
        else
        {
            throw BufferAllocationFailure("No unpooled TupleBuffer available!");
        }
    }
}

boost::asio::awaitable<void> FileBackedPagedVector::writePayloadAndKeysToSeparateFiles(
    const Memory::MemoryLayouts::MemoryLayout* memoryLayout, const std::shared_ptr<FileWriter> fileWriter)
{
    const auto keyFieldsOnlySchema = memoryLayout->createKeyFieldsOnlySchema();
    const auto keyFieldsOnlyMemoryLayout
        = Memory::MemoryLayouts::MemoryLayout::createMemoryLayout(keyFieldsOnlySchema, memoryLayout->getBufferSize());
    const auto groupedFieldTypeSizes = memoryLayout->getGroupedFieldTypeSizes();

    for (const auto& keyPage : keyPages)
    {
        co_await fileWriter->writeKey(keyPage.getBuffer(), keyPage.getNumberOfTuples() * keyFieldsOnlyMemoryLayout->getTupleSize());
    }

    for (const auto& [_, page] : pages)
    {
        const auto* pagePtr = page.getBuffer();
        const auto numTuplesOnPage = page.getNumberOfTuples();
        for (auto tupleIdx = 0UL; tupleIdx < numTuplesOnPage; ++tupleIdx)
        {
            for (const auto& [fieldType, fieldSize] : groupedFieldTypeSizes)
            {
                if (fieldType == Memory::MemoryLayouts::MemoryLayout::FieldType::KEY)
                {
                    co_await fileWriter->writeKey(pagePtr, fieldSize);
                }
                else if (fieldType == Memory::MemoryLayouts::MemoryLayout::FieldType::PAYLOAD)
                {
                    co_await fileWriter->write(pagePtr, fieldSize);
                }
                pagePtr += fieldSize;
            }
        }
        numTuplesOnDisk += numTuplesOnPage;
    }
    co_return;
}

boost::asio::awaitable<void> FileBackedPagedVector::writePayloadOnlyToFile(
    const Memory::MemoryLayouts::MemoryLayout* memoryLayout,
    Memory::AbstractBufferProvider* bufferProvider,
    const std::shared_ptr<FileWriter> fileWriter)
{
    const auto keyFieldsOnlySchema = memoryLayout->createKeyFieldsOnlySchema();
    const auto keyFieldsOnlyMemoryLayout
        = Memory::MemoryLayouts::MemoryLayout::createMemoryLayout(keyFieldsOnlySchema, memoryLayout->getBufferSize());
    const auto groupedFieldTypeSizes = memoryLayout->getGroupedFieldTypeSizes();

    for (const auto& [_, page] : pages)
    {
        const auto* pagePtr = page.getBuffer();
        const auto numTuplesOnPage = page.getNumberOfTuples();
        for (auto tupleIdx = 0UL; tupleIdx < numTuplesOnPage; ++tupleIdx)
        {
            // TODO appendPageIfFull only when page is full not for each tuple
            appendKeyPageIfFull(bufferProvider, keyFieldsOnlyMemoryLayout.get());
            auto& lastKeyPage = keyPages.back();
            const auto numTuplesLastKeyPage = lastKeyPage.getNumberOfTuples();
            auto* lastKeyPagePtr = lastKeyPage.getBuffer() + numTuplesLastKeyPage * keyFieldsOnlyMemoryLayout->getTupleSize();

            for (const auto& [fieldType, fieldSize] : groupedFieldTypeSizes)
            {
                if (fieldType == Memory::MemoryLayouts::MemoryLayout::FieldType::KEY)
                {
                    std::memcpy(lastKeyPagePtr, pagePtr, fieldSize);
                    lastKeyPagePtr += fieldSize;
                }
                else if (fieldType == Memory::MemoryLayouts::MemoryLayout::FieldType::PAYLOAD)
                {
                    co_await fileWriter->write(pagePtr, fieldSize);
                }
                pagePtr += fieldSize;
            }
            lastKeyPage.setNumberOfTuples(numTuplesLastKeyPage + 1);
        }
        numTuplesOnDisk += numTuplesOnPage;
    }
    co_return;
}

void FileBackedPagedVector::readSeparatelyFromFiles(
    const Memory::MemoryLayouts::MemoryLayout* memoryLayout, Memory::AbstractBufferProvider* bufferProvider, FileReader& fileReader)
{
    const auto keyFieldsOnlySchema = memoryLayout->createKeyFieldsOnlySchema();
    const auto keyFieldsOnlyMemoryLayout
        = Memory::MemoryLayouts::MemoryLayout::createMemoryLayout(keyFieldsOnlySchema, memoryLayout->getBufferSize());
    const auto groupedFieldTypeSizes = memoryLayout->getGroupedFieldTypeSizes();

    const auto* keyPagePtr = !keyPages.empty() ? keyPages.front().getBuffer() : nullptr;
    while (true)
    {
        // TODO appendPageIfFull only when page is full not for each tuple
        appendPageIfFull(bufferProvider, memoryLayout);
        auto& lastPage = pages.back().buffer;
        const auto numTuplesLastPage = lastPage.getNumberOfTuples();
        auto* lastPagePtr = lastPage.getBuffer() + numTuplesLastPage * memoryLayout->getTupleSize();

        // TODO get new key page only when all tuples were read and do not check for each tuple
        if (keyPagePtr != nullptr)
        {
            if (const auto& currKeyPage = keyPages.front();
                keyPagePtr >= currKeyPage.getBuffer() + currKeyPage.getNumberOfTuples() * keyFieldsOnlyMemoryLayout->getTupleSize())
            {
                keyPages.erase(keyPages.begin());
                keyPagePtr = !keyPages.empty() ? keyPages.front().getBuffer() : nullptr;
                // TODO if fileReader.peek() != EOF then return
            }
        }

        for (const auto& [fieldType, fieldSize] : groupedFieldTypeSizes)
        {
            if (fieldType == Memory::MemoryLayouts::MemoryLayout::FieldType::KEY)
            {
                if (fileReader.readKey(lastPagePtr, fieldSize) == 0)
                {
                    if (keyPagePtr != nullptr)
                    {
                        std::memcpy(lastPagePtr, keyPagePtr, fieldSize);
                        keyPagePtr += fieldSize;
                    }
                }
            }
            else if (fieldType == Memory::MemoryLayouts::MemoryLayout::FieldType::PAYLOAD)
            {
                if (fileReader.read(lastPagePtr, fieldSize) == 0)
                {
                    return;
                }
            }
            lastPagePtr += fieldSize;
        }
        lastPage.setNumberOfTuples(numTuplesLastPage + 1);
        --numTuplesOnDisk;
    }
}

}
