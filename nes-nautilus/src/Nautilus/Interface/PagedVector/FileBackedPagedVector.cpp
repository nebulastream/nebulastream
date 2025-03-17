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
#include <Util/Core.hpp>

namespace NES::Nautilus::Interface
{

void FileBackedPagedVector::writeToFile(
    PagedVector* pagedVector,
    PagedVector* pagedVectorKeys,
    Memory::AbstractBufferProvider* bufferProvider,
    const Memory::MemoryLayouts::MemoryLayout* memoryLayout,
    Runtime::Execution::FileWriter& fileWriter,
    const Runtime::Execution::FileLayout fileLayout)
{
    switch (fileLayout)
    {
        /// Write all tuples consecutivley to file
        case Runtime::Execution::NO_SEPARATION_KEEP_KEYS:
        case Runtime::Execution::NO_SEPARATION: {
            for (const auto& page : pagedVector->getPages())
            {
                fileWriter.write(page.getBuffer(), page.getNumberOfTuples() * memoryLayout->getTupleSize());
            }
            break;
        }
        /// Write only payload to file and append key field data to designated pagedVectorKeys
        case Runtime::Execution::SEPARATE_PAYLOAD: {
            writePayloadOnlyToFile(pagedVector, memoryLayout, bufferProvider, pagedVectorKeys, fileWriter);
            break;
        }
        /// Write designated pagedVectorKeys to key file first and then remaining payload and key field data to separate files
        case Runtime::Execution::SEPARATE_KEYS: {
            writePayloadAndKeysToSeparateFiles(pagedVector, memoryLayout, pagedVectorKeys, fileWriter);
            break;
        }
    }
}

void FileBackedPagedVector::readFromFile(
    PagedVector* pagedVector,
    PagedVector* pagedVectorKeys,
    Memory::AbstractBufferProvider* bufferProvider,
    const Memory::MemoryLayouts::MemoryLayout* memoryLayout,
    Runtime::Execution::FileReader& fileReader,
    const Runtime::Execution::FileLayout fileLayout)
{
    switch (fileLayout)
    {
        /// Read all tuples consecutivley from file
        case Runtime::Execution::NO_SEPARATION_KEEP_KEYS:
        case Runtime::Execution::NO_SEPARATION: {
            pagedVector->appendPageIfFull(bufferProvider, memoryLayout);
            auto lastPage = pagedVector->getLastPage();
            auto tuplesToRead = memoryLayout->getCapacity() - lastPage.getNumberOfTuples();
            const auto tupleSize = memoryLayout->getTupleSize();

            while (const auto bytesRead = fileReader.read(lastPage.getBuffer(), tuplesToRead * tupleSize))
            {
                lastPage.setNumberOfTuples(bytesRead / tupleSize);
                pagedVector->appendPageIfFull(bufferProvider, memoryLayout);
                lastPage = pagedVector->getLastPage();
                tuplesToRead = memoryLayout->getCapacity();
            }
            break;
        }
        /// Read payload and key field data from separate files first and then remaining designated pagedVectorKeys and payload from file
        case Runtime::Execution::SEPARATE_PAYLOAD:
        case Runtime::Execution::SEPARATE_KEYS: {
            readSeparatelyFromFiles(pagedVector, memoryLayout, bufferProvider, pagedVectorKeys, fileReader);
            break;
        }
    }
}

void FileBackedPagedVector::truncate(
    PagedVector* pagedVector, PagedVector* pagedVectorKeys, const Runtime::Execution::FileLayout fileLayout)
{
    // TODO append key field data to designated pagedVectorKeys to be able to write all tuples to file but keep keys in memory, alternatively create mew FileLayout and do this in writeToFile()->writePayloadOnlyToFile()
    switch (fileLayout)
    {
        /// Append key field data to designated pagedVectorKeys
        case Runtime::Execution::NO_SEPARATION_KEEP_KEYS: {
            // TODO
            break;
        }
        /// Do nothing as key field data has just been written to designated pagedVectorKeys
        case Runtime::Execution::SEPARATE_PAYLOAD: {
            break;
        }
        /// Remove all key field data as a different FileLayout might have been used previously
        case Runtime::Execution::NO_SEPARATION:
        case Runtime::Execution::SEPARATE_KEYS: {
            pagedVectorKeys->getPages().clear();
            break;
        }
    }
    pagedVector->getPages().clear();
}

void FileBackedPagedVector::writePayloadOnlyToFile(
    PagedVector* pagedVector,
    const Memory::MemoryLayouts::MemoryLayout* memoryLayout,
    Memory::AbstractBufferProvider* bufferProvider,
    PagedVector* pagedVectorKeys,
    Runtime::Execution::FileWriter& fileWriter)
{
    const auto keyFieldsOnlySchema = memoryLayout->createKeyFieldsOnlySchema();
    const auto keyFieldsOnlyMemoryLayout = NES::Util::createMemoryLayout(keyFieldsOnlySchema, memoryLayout->getBufferSize());
    const auto groupedFieldTypeSizes = memoryLayout->getGroupedFieldTypeSizes();

    for (const auto& page : pagedVector->getPages())
    {
        const auto* pagePtr = page.getBuffer();
        for (auto tupleIdx = 0UL; tupleIdx < page.getNumberOfTuples(); ++tupleIdx)
        {
            // TODO appendPageIfFull only when page is full not for each tuple
            pagedVectorKeys->appendPageIfFull(bufferProvider, keyFieldsOnlyMemoryLayout.get());
            auto lastKeyPage = pagedVectorKeys->getLastPage();
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
                    fileWriter.write(pagePtr, fieldSize);
                }
                pagePtr += fieldSize;
            }
            lastKeyPage.setNumberOfTuples(numTuplesLastKeyPage + 1);
        }
    }
}

void FileBackedPagedVector::writePayloadAndKeysToSeparateFiles(
    PagedVector* pagedVector,
    const Memory::MemoryLayouts::MemoryLayout* memoryLayout,
    PagedVector* pagedVectorKeys,
    Runtime::Execution::FileWriter& fileWriter)
{
    const auto keyFieldsOnlySchema = memoryLayout->createKeyFieldsOnlySchema();
    const auto keyFieldsOnlyMemoryLayout = NES::Util::createMemoryLayout(keyFieldsOnlySchema, memoryLayout->getBufferSize());
    const auto groupedFieldTypeSizes = memoryLayout->getGroupedFieldTypeSizes();

    for (const auto& keyPage : pagedVectorKeys->getPages())
    {
        fileWriter.writeKey(keyPage.getBuffer(), keyPage.getNumberOfTuples() * keyFieldsOnlyMemoryLayout->getTupleSize());
    }

    for (const auto& page : pagedVector->getPages())
    {
        const auto* pagePtr = page.getBuffer();
        for (auto tupleIdx = 0UL; tupleIdx < page.getNumberOfTuples(); ++tupleIdx)
        {
            for (const auto& [fieldType, fieldSize] : groupedFieldTypeSizes)
            {
                if (fieldType == Memory::MemoryLayouts::MemoryLayout::FieldType::KEY)
                {
                    fileWriter.writeKey(pagePtr, fieldSize);
                }
                else if (fieldType == Memory::MemoryLayouts::MemoryLayout::FieldType::PAYLOAD)
                {
                    fileWriter.write(pagePtr, fieldSize);
                }
                pagePtr += fieldSize;
            }
        }
    }
}

void FileBackedPagedVector::readSeparatelyFromFiles(
    PagedVector* pagedVector,
    const Memory::MemoryLayouts::MemoryLayout* memoryLayout,
    Memory::AbstractBufferProvider* bufferProvider,
    PagedVector* pagedVectorKeys,
    Runtime::Execution::FileReader& fileReader)
{
    const auto keyFieldsOnlySchema = memoryLayout->createKeyFieldsOnlySchema();
    const auto keyFieldsOnlyMemoryLayout = NES::Util::createMemoryLayout(keyFieldsOnlySchema, memoryLayout->getBufferSize());
    const auto groupedFieldTypeSizes = memoryLayout->getGroupedFieldTypeSizes();

    auto& keyPages = pagedVectorKeys->getPages();
    const auto* keyPagePtr = !keyPages.empty() ? keyPages.front().getBuffer() : nullptr;

    while (true)
    {
        // TODO appendPageIfFull only when page is full not for each tuple
        pagedVector->appendPageIfFull(bufferProvider, memoryLayout);
        auto lastPage = pagedVector->getLastPage();
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
                if (fileReader.readKey(lastPagePtr, fieldSize) == 0 && keyPagePtr != nullptr)
                {
                    std::memcpy(lastPagePtr, keyPagePtr, fieldSize);
                    keyPagePtr += fieldSize;
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
    }
}

}
