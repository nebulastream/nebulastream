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

#include <Nautilus/Interface/PagedVector/PagedVector.hpp>
#include <Util/Core.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Timer.hpp>
#include <BaseUnitTest.hpp>

namespace NES
{

using FieldType = enum : uint8_t { KEY, PAYLOAD };
using SeparateKeys = enum : uint8_t { NO_SEPARATION, SAME_FILE_KEYS, SAME_FILE_PAYLOAD, SEPARATE_FILES };

class MicroBenchmarksTest : public Testing::BaseUnitTest
{
public:
    // TODO NOTE: The test needs approximately
    static constexpr uint64_t DATA_SIZE = 6442450944; /// 6GB //12884901888; /// 12GB //17179869184; /// 16GB
    std::vector<double> execTimes;

    static void SetUpTestSuite()
    {
        Logger::setupLogging("MicroBenchmarksTest.log", LogLevel::LOG_DEBUG);
        NES_INFO("Setup MicroBenchmarksTest class.");
    }

    static void TearDownTestSuite() { NES_INFO("Tear down MicroBenchmarksTest class."); }

    static void addSchemaFields(const SchemaPtr& schema)
    {
        schema->addField("f0", BasicType::UINT64)
            ->addField("f1", BasicType::UINT64)
            ->addField("f2", BasicType::UINT64)
            ->addField("f3", BasicType::UINT64)
            ->addField("f4", BasicType::UINT64)
            ->addField("f5", BasicType::UINT64)
            ->addField("f6", BasicType::UINT64)
            ->addField("f7", BasicType::UINT64)
            ->addField("f8", BasicType::UINT64)
            ->addField("f9", BasicType::UINT64);
    }

    static std::vector<std::tuple<uint8_t, uint64_t>> getFieldTypeSizes(const Memory::MemoryLayouts::MemoryLayoutPtr& memoryLayout)
    {
        std::vector<std::tuple<uint8_t, uint64_t>> fieldTypeSizes;
        const auto schema = memoryLayout->getSchema();
        const auto keyFieldNames = memoryLayout->getKeyFieldNames();

        for (auto fieldIdx = 0UL; fieldIdx < schema->getFieldCount(); ++fieldIdx)
        {
            uint8_t fieldType = PAYLOAD;
            if (std::ranges::find(keyFieldNames, schema->getFieldNames()[fieldIdx]) != keyFieldNames.end())
            {
                fieldType = KEY;
            }

            uint64_t fieldSize = memoryLayout->getFieldSizes()[fieldIdx];
            if (const auto lastFieldTypeSize = fieldTypeSizes.back();
                !fieldTypeSizes.empty() && std::get<uint8_t>(lastFieldTypeSize) == fieldType)
            {
                fieldSize += std::get<uint64_t>(lastFieldTypeSize);
                fieldTypeSizes.pop_back();
            }

            fieldTypeSizes.emplace_back(std::make_tuple(fieldType, fieldSize));
        }

        return fieldTypeSizes;
    }

    static std::tuple<uint64_t, uint64_t> getKeyAndPayloadSize(const Memory::MemoryLayouts::MemoryLayoutPtr& memoryLayout)
    {
        auto keySize = 0UL;
        auto payloadSize = 0UL;
        const auto schema = memoryLayout->getSchema();
        const auto keyFieldNames = memoryLayout->getKeyFieldNames();
        const auto fieldSizes = memoryLayout->getFieldSizes();

        for (auto fieldIdx = 0UL; fieldIdx < schema->getFieldCount(); ++fieldIdx)
        {
            if (std::ranges::find(keyFieldNames, schema->getFieldNames()[fieldIdx]) != keyFieldNames.end())
            {
                keySize += fieldSizes[fieldIdx];
            }
            else
            {
                payloadSize += fieldSizes[fieldIdx];
            }
        }

        NES_ASSERT2_FMT(keySize + payloadSize == memoryLayout->getTupleSize(), "Error occurred in getKeyAndPayloadSizes");
        return std::make_tuple(keySize, payloadSize);
    }

    static std::shared_ptr<Nautilus::Interface::PagedVector> copyPagedVector(Nautilus::Interface::PagedVector pagedVector)
    {
        return std::make_shared<Nautilus::Interface::PagedVector>(pagedVector);
    }

    static std::shared_ptr<Nautilus::Interface::PagedVector>
    createKeysOnlyPagedVector(const std::vector<std::string>& keyFieldNames, const Memory::BufferManagerPtr& bufferManager)
    {
        const auto keysOnlySchema = Schema::create();
        for (const auto& keyFieldName : keyFieldNames)
        {
            keysOnlySchema->addField(keyFieldName, BasicType::UINT64);
        }

        return std::make_shared<Nautilus::Interface::PagedVector>(
            bufferManager, Util::createMemoryLayout(keysOnlySchema, bufferManager->getBufferSize()));
    }

    void clearPagedVector(
        std::shared_ptr<Nautilus::Interface::PagedVector> pagedVector,
        const Memory::BufferManagerPtr& bufferManager,
        const SeparateKeys separateKeys)
    {
        NES_INFO("PagedVector clearing...");
        Timer<> timer("clearTimer");
        timer.start();

        if (separateKeys == SAME_FILE_PAYLOAD)
        {
            const auto oldMemoryLayout = pagedVector->getMemoryLayout();
            const auto fieldTypeSizes = getFieldTypeSizes(oldMemoryLayout);
            const auto oldCapacity = oldMemoryLayout->getCapacity();
            const auto oldNumTuples = oldCapacity * pagedVector->getNumberOfPages();

            const auto newPagedVector = createKeysOnlyPagedVector(oldMemoryLayout->getKeyFieldNames(), bufferManager);
            const auto newMemoryLayout = newPagedVector->getMemoryLayout();
            const auto newCapacity = newMemoryLayout->getCapacity();
            const uint64_t expectedNumPages = std::ceil(oldNumTuples / newCapacity);

            // TODO does this enhance performance?
            newPagedVector->appendPageIfFull();
            newPagedVector->getPages().reserve(expectedNumPages);

            auto& newPages = newPagedVector->getPages();
            auto* newFieldAddr = newPages.front().getBuffer();

            auto tupleCnt = 0UL;
            for (auto& page : pagedVector->getPages())
            {
                const auto* oldFieldAddr = page.getBuffer();

                for (auto oldTupleIdx = 0UL; oldTupleIdx < oldCapacity; ++oldTupleIdx, ++tupleCnt)
                {
                    if (tupleCnt >= newCapacity)
                    {
                        newPages.back().setNumberOfTuples(newCapacity);
                        newPagedVector->appendPageIfFull();
                        newFieldAddr = newPages.back().getBuffer();
                        tupleCnt = 0;
                    }

                    for (const auto [fieldType, fieldSize] : fieldTypeSizes)
                    {
                        if (fieldType == KEY)
                        {
                            std::memcpy(newFieldAddr, oldFieldAddr, fieldSize);
                            newFieldAddr += fieldSize;
                        }

                        oldFieldAddr += fieldSize;
                    }
                }
            }

            const auto numTuplesLastPage = oldNumTuples % newCapacity == 0 ? newCapacity : oldNumTuples % newCapacity;
            ASSERT_EQ(tupleCnt, numTuplesLastPage);
            newPages.back().setNumberOfTuples(numTuplesLastPage);
            pagedVector = newPagedVector;
            ASSERT_EQ(pagedVector->getNumberOfPages(), expectedNumPages);
        }
        else
        {
            pagedVector->getPages().clear();
            ASSERT_EQ(pagedVector->getNumberOfPages(), 0);
        }

        timer.snapshot("done clearing");
        timer.pause();
        auto timeInMs = timer.getPrintTime();
        execTimes.emplace_back(timeInMs);
        NES_INFO("PagedVector cleared in {} ms", timeInMs);
    }

    void generateData(const std::shared_ptr<Nautilus::Interface::PagedVector>& pagedVector, const uint64_t numBuffers)
    {
        NES_INFO("Data generating...");
        Timer<> timer("generateDataTimer");
        timer.start();

        // TODO does this enhance performance?
        pagedVector->appendPageIfFull();
        pagedVector->getPages().reserve(numBuffers + pagedVector->getNumberOfPages() - 1);

        const auto memoryLayout = pagedVector->getMemoryLayout();
        const auto numFields = memoryLayout->getCapacity() * memoryLayout->getSchema()->getFieldCount();

        auto valueCnt = 0UL;
        const auto tenPercentOfNumBuffers = std::max(1UL, (numBuffers * 10) / 100);

        for (auto bufferIdx = 0UL; bufferIdx < numBuffers; ++bufferIdx)
        {
            pagedVector->appendPageIfFull();
            auto& tupleBuffer = pagedVector->getPages().back();
            tupleBuffer.setNumberOfTuples(memoryLayout->getCapacity());
            auto* const tupleBufferAddr = tupleBuffer.getBuffer<uint64_t>();

            for (auto fieldIdx = 0UL; fieldIdx < numFields; ++fieldIdx)
            {
                tupleBufferAddr[fieldIdx] = valueCnt++;
            }

            if (bufferIdx % tenPercentOfNumBuffers == 0)
            {
                NES_INFO("Data {}% generated", std::round(100 * static_cast<double>(bufferIdx) / numBuffers));
            }
        }

        timer.snapshot("done generating");
        timer.pause();
        auto timeInMs = timer.getPrintTime();
        execTimes.emplace_back(timeInMs);
        NES_INFO("Data generated in {} ms", timeInMs);
    }

    void writePagedVectorToFile(
        const std::shared_ptr<Nautilus::Interface::PagedVector>& pagedVector,
        const std::vector<std::string>& fileNames,
        const uint64_t fileBufferSize,
        const SeparateKeys separateKeys)
    {
        NES_INFO("File writing...");
        Timer<> timer("writeFileTimer");
        timer.start();

        std::ofstream payloadFile;
        std::ofstream keyFile;

        if (separateKeys == SAME_FILE_KEYS)
        {
            payloadFile = std::ofstream(fileNames.front(), std::ios::out | std::ios::binary);
        }
        else
        {
            payloadFile = std::ofstream(fileNames.front(), std::ios::out | std::ios::trunc | std::ios::binary);
        }
        ASSERT_TRUE(payloadFile);
        if (fileNames.size() > 1)
        {
            ASSERT_EQ(separateKeys, SEPARATE_FILES);
            keyFile = std::ofstream(fileNames.at(1), std::ios::out | std::ios::trunc | std::ios::binary);
            ASSERT_TRUE(keyFile);
        }
        ASSERT_EQ(payloadFile.tellp(), 0);
        ASSERT_EQ(keyFile.tellp(), 0);

        // TODO remove?
        const auto& pages = pagedVector->getPages();
        const auto numPages = pagedVector->getNumberOfPages();
        const auto memoryLayout = pagedVector->getMemoryLayout();
        const auto tupleSize = memoryLayout->getTupleSize();
        const auto bufferSize = memoryLayout->getBufferSize();
        const auto capacity = memoryLayout->getCapacity();
        ASSERT_EQ(fileBufferSize % bufferSize, 0);

        const auto fieldTypeSizes = getFieldTypeSizes(memoryLayout);
        const auto [keySize, payloadSize] = getKeyAndPayloadSize(memoryLayout);

        const uint64_t numBuffers = std::ceil(bufferSize * numPages / fileBufferSize);
        const auto numPagesPerBuffer = fileBufferSize / bufferSize;
        const auto keyFileWriteSize
            = separateKeys == SEPARATE_FILES || separateKeys == SAME_FILE_KEYS ? numPagesPerBuffer * capacity * keySize : 0;
        const auto payloadFileWriteSize = separateKeys == SEPARATE_FILES || separateKeys == SAME_FILE_PAYLOAD
            ? numPagesPerBuffer * capacity * payloadSize
            : fileBufferSize;
        const auto tenPercentOfNumBuffers = std::max(1UL, (numBuffers * 10) / 100);
        const auto pagePadding = bufferSize - capacity * tupleSize;

        auto pageIdx = 0UL;
        std::vector<char> keyBuffer(keyFileWriteSize);
        std::vector<char> payloadBuffer(payloadFileWriteSize);

        for (auto bufferIdx = 0UL; bufferIdx < numBuffers; ++bufferIdx)
        {
            auto tuplesWritten = 0UL;
            auto* fieldAddrKeyBuffer = keyBuffer.data();
            auto* fieldAddrPayloadBuffer = payloadBuffer.data();

            for (auto pageNum = 0UL; pageNum < numPagesPerBuffer && pageIdx < numPages; ++pageNum, ++pageIdx)
            {
                const auto& page = pages[pageIdx];
                const auto* fieldAddrPagedVector = page.getBuffer();

                if (separateKeys == NO_SEPARATION)
                {
                    std::memcpy(fieldAddrPayloadBuffer, fieldAddrPagedVector, bufferSize);
                    fieldAddrPayloadBuffer += bufferSize;
                }
                else
                {
                    for (auto tupleIdx = 0UL; tupleIdx < capacity; ++tupleIdx, ++tuplesWritten)
                    {
                        for (const auto [fieldType, fieldSize] : fieldTypeSizes)
                        {
                            if (fieldType == KEY && (separateKeys == SEPARATE_FILES || separateKeys == SAME_FILE_KEYS))
                            {
                                std::memcpy(fieldAddrKeyBuffer, fieldAddrPagedVector, fieldSize);
                                fieldAddrKeyBuffer += fieldSize;
                            }
                            else if (fieldType == PAYLOAD && (separateKeys == SEPARATE_FILES || separateKeys == SAME_FILE_PAYLOAD))
                            {
                                std::memcpy(fieldAddrPayloadBuffer, fieldAddrPagedVector, fieldSize);
                                fieldAddrPayloadBuffer += fieldSize;
                            }

                            fieldAddrPagedVector += fieldSize;
                        }
                    }
                }
            }

            if (separateKeys == NO_SEPARATION)
            {
                payloadFile.write(payloadBuffer.data(), fileBufferSize);
            }
            else if (separateKeys == SEPARATE_FILES)
            {
                keyFile.write(keyBuffer.data(), tuplesWritten * keySize);
                payloadFile.write(payloadBuffer.data(), tuplesWritten * payloadSize);
            }
            else
            {
                fieldAddrKeyBuffer = keyBuffer.data();
                fieldAddrPayloadBuffer = payloadBuffer.data();
                for (auto tupleIdx = 0UL; tupleIdx < tuplesWritten; ++tupleIdx)
                {
                    for (const auto [fieldType, fieldSize] : fieldTypeSizes)
                    {
                        if (separateKeys == SAME_FILE_KEYS && fieldType == KEY)
                        {
                            payloadFile.write(fieldAddrKeyBuffer, fieldSize);
                            fieldAddrKeyBuffer += fieldSize;
                        }
                        else if (separateKeys == SAME_FILE_PAYLOAD && fieldType == PAYLOAD)
                        {
                            payloadFile.write(fieldAddrPayloadBuffer, fieldSize);
                            fieldAddrPayloadBuffer += fieldSize;
                        }
                        else
                        {
                            payloadFile.seekp(fieldSize, std::ios::cur);
                        }
                    }

                    if (tupleIdx % capacity == 0)
                    {
                        payloadFile.seekp(pagePadding, std::ios::cur);
                    }
                }
            }

            if (bufferIdx % tenPercentOfNumBuffers == 0)
            {
                NES_INFO("File {}% written", std::round(100 * static_cast<double>(bufferIdx) / numBuffers));
            }
        }

        payloadFile.close();
        if (fileNames.size() > 1)
        {
            keyFile.close();
        }

        timer.snapshot("done writing");
        timer.pause();
        auto timeInMs = timer.getPrintTime();
        execTimes.emplace_back(timeInMs);
        NES_INFO("File written in {} ms", timeInMs);
    }

    void writeFileToPagedVector(
        std::shared_ptr<Nautilus::Interface::PagedVector> pagedVector,
        const Memory::MemoryLayouts::MemoryLayoutPtr& memoryLayout,
        const Memory::BufferManagerPtr& bufferManager,
        const std::vector<std::string>& fileNames,
        const uint64_t expectedNumPages,
        const uint64_t fileBufferSize,
        const SeparateKeys separateKeys)
    {
        NES_INFO("PagedVector writing...");
        Timer<> timer("writePagedVectorTimer");
        timer.start();

        std::ifstream payloadFile;
        std::ifstream keyFile;

        payloadFile = std::ifstream(fileNames.front(), std::ios::in | std::ios::binary);
        ASSERT_TRUE(payloadFile);
        if (fileNames.size() > 1)
        {
            ASSERT_EQ(separateKeys, SEPARATE_FILES);
            keyFile = std::ifstream(fileNames.at(1), std::ios::in | std::ios::binary);
            ASSERT_TRUE(keyFile);
        }
        ASSERT_EQ(payloadFile.tellg(), 0);
        ASSERT_EQ(keyFile.tellg(), 0);

        // TODO does this enhance performance?
        auto newPagedVector = std::make_shared<Nautilus::Interface::PagedVector>(bufferManager, memoryLayout);
        newPagedVector->getPages().reserve(expectedNumPages);

        // TODO remove?
        auto& oldPages = pagedVector->getPages();
        const auto oldCapacity = pagedVector->getMemoryLayout()->getCapacity();
        auto& pages = newPagedVector->getPages();
        const auto tupleSize = memoryLayout->getTupleSize();
        const auto bufferSize = memoryLayout->getBufferSize();
        const auto capacity = memoryLayout->getCapacity();
        ASSERT_EQ(fileBufferSize % bufferSize, 0);

        const auto fieldTypeSizes = getFieldTypeSizes(memoryLayout);
        const auto [keySize, payloadSize] = getKeyAndPayloadSize(memoryLayout);

        const auto numPagesPerBuffer = fileBufferSize / bufferSize;
        const auto keyFileReadSize = separateKeys == SEPARATE_FILES ? numPagesPerBuffer * capacity * keySize : 0;
        const auto payloadFileReadSize = separateKeys == SEPARATE_FILES ? numPagesPerBuffer * capacity * payloadSize : fileBufferSize;
        const auto tenPercentOfNumPages = std::max(1UL, (expectedNumPages * 10) / 100);
        const auto pagePadding = bufferSize - capacity * tupleSize;

        auto pageCnt = 0UL;
        std::vector<char> keyBuffer(keyFileReadSize);
        std::vector<char> payloadBuffer(payloadFileReadSize);

        auto oldPageCnt = 0UL;
        auto oldTupleCnt = 0UL;
        auto* fieldAddrKeyPagedVector = !oldPages.empty() ? oldPages.front().getBuffer() : nullptr;

        while (payloadFile.read(payloadBuffer.data(), payloadFileReadSize) || payloadFile.gcount() > 0)
        {
            auto* fieldAddrKeyBuffer = keyBuffer.data();
            auto* fieldAddrPayloadBuffer = payloadBuffer.data();

            if (separateKeys == SEPARATE_FILES)
            {
                keyFile.read(keyBuffer.data(), keyFileReadSize);
                ASSERT_GT(keyFile.gcount(), 0);
            }

            for (auto pageIdx = 0UL; pageIdx < numPagesPerBuffer; ++pageIdx)
            {
                newPagedVector->appendPageIfFull();
                auto& page = pages.back();
                page.setNumberOfTuples(capacity);

                auto* fieldAddrPagedVector = page.getBuffer();

                if (separateKeys == NO_SEPARATION || separateKeys == SAME_FILE_KEYS)
                {
                    std::memcpy(fieldAddrPagedVector, fieldAddrPayloadBuffer, bufferSize);
                    fieldAddrPayloadBuffer += bufferSize;
                }
                else
                {
                    for (auto tupleIdx = 0UL; tupleIdx < capacity; ++tupleIdx)
                    {
                        if (separateKeys == SAME_FILE_PAYLOAD && oldTupleCnt++ >= oldCapacity)
                        {
                            fieldAddrKeyPagedVector = oldPages[++oldPageCnt].getBuffer();
                            oldTupleCnt = 0;
                        }

                        for (const auto [fieldType, fieldSize] : fieldTypeSizes)
                        {
                            if (fieldType == KEY)
                            {
                                if (separateKeys == SEPARATE_FILES)
                                {
                                    std::memcpy(fieldAddrPagedVector, fieldAddrKeyBuffer, fieldSize);
                                    fieldAddrKeyBuffer += fieldSize;
                                }
                                else
                                {
                                    std::memcpy(fieldAddrPagedVector, fieldAddrKeyPagedVector, fieldSize);
                                    fieldAddrKeyPagedVector += fieldSize;
                                    fieldAddrPayloadBuffer += fieldSize;
                                }
                            }
                            else
                            {
                                std::memcpy(fieldAddrPagedVector, fieldAddrPayloadBuffer, fieldSize);
                                fieldAddrPayloadBuffer += fieldSize;
                            }

                            fieldAddrPagedVector += fieldSize;
                        }
                    }
                }

                if (separateKeys == SAME_FILE_PAYLOAD)
                {
                    fieldAddrPayloadBuffer += pagePadding;
                }

                if (pageCnt++ % tenPercentOfNumPages == 0)
                {
                    NES_INFO("PagedVector {}% written", std::round(100 * static_cast<double>(pageCnt - 1) / expectedNumPages));
                }
            }
        }

        payloadFile.close();
        if (fileNames.size() > 1)
        {
            keyFile.close();
        }

        pagedVector = newPagedVector;

        timer.snapshot("done writing");
        timer.pause();
        auto timeInMs = timer.getPrintTime();
        execTimes.emplace_back(timeInMs);
        NES_INFO("PagedVector written in {} ms", timeInMs);
    }

    void comparePagedVectors(
        const std::shared_ptr<Nautilus::Interface::PagedVector>& expectedPagedVector,
        const std::shared_ptr<Nautilus::Interface::PagedVector>& actualPagedVector)
    {
        NES_INFO("PagedVectors comparing...");
        Timer<> timer("compareTimer");
        timer.start();

        const auto expectedNumPages = expectedPagedVector->getNumberOfPages();
        const auto actualNumPages = actualPagedVector->getNumberOfPages();

        ASSERT_EQ(expectedNumPages, actualNumPages);

        const auto& expectedPages = expectedPagedVector->getPages();
        const auto& actualPages = actualPagedVector->getPages();

        const auto tenPercentOfNumPages = std::max(1UL, (expectedNumPages * 10) / 100);
        const auto bufferSize = expectedPagedVector->getMemoryLayout()->getBufferSize();

        for (auto pageIdx = 0UL; pageIdx < expectedNumPages; ++pageIdx)
        {
            const auto* const expectedBufferAddr = expectedPages[pageIdx].getBuffer();
            const auto* const actualBufferAddr = actualPages[pageIdx].getBuffer();

            ASSERT_EQ(std::memcmp(expectedBufferAddr, actualBufferAddr, bufferSize), 0);

            if (pageIdx % tenPercentOfNumPages == 0)
            {
                NES_INFO("PagedVectors {}% compared", std::round(100 * static_cast<double>(pageIdx) / expectedNumPages));
            }
        }

        timer.snapshot("done comparing");
        timer.pause();
        auto timeInMs = timer.getPrintTime();
        execTimes.emplace_back(timeInMs);
        NES_INFO("PagedVectors compared in {} ms", timeInMs);
    }
};

TEST_F(MicroBenchmarksTest, benchmark1)
{
    execTimes.clear();
    const auto pageSize = 4 * 1024;
    const auto numBuffers = DATA_SIZE / pageSize;
    const auto seperateKeys = NO_SEPARATION;

    const std::vector<std::string> fileNames = {"../../../benchmark1_payload.dat"};
    const auto testSchema = Schema::create();
    addSchemaFields(testSchema);

    const auto bufferManager = Memory::BufferManager::create(pageSize, numBuffers);
    const auto memoryLayout = Util::createMemoryLayout(testSchema, pageSize);
    memoryLayout->setKeyFieldNames({"f0", "f1"});

    const auto pagedVector = std::make_shared<Nautilus::Interface::PagedVector>(bufferManager, memoryLayout);
    generateData(pagedVector, numBuffers);
    NES_INFO("PagedVector copying...");
    auto expectedPagedVector = copyPagedVector(*pagedVector);
    NES_INFO("PagedVector copied");

    writePagedVectorToFile(pagedVector, fileNames, pageSize, seperateKeys);
    clearPagedVector(pagedVector, bufferManager, seperateKeys);

    writeFileToPagedVector(pagedVector, memoryLayout, bufferManager, fileNames, numBuffers, pageSize, seperateKeys);
    comparePagedVectors(expectedPagedVector, pagedVector);

    //createMeasurementsCSV("fileName.csv");
}

}
