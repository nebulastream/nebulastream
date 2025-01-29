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

class MicroBenchmarksTest : public Testing::BaseUnitTest,
                            public testing::WithParamInterface<std::tuple<uint64_t, uint64_t, std::vector<std::string>>>
{
public:
    /// NOTE: The test needs approximately up to five times DATA_SIZE of memory
    static constexpr uint64_t DATA_SIZE = 10737418240; /// 10GB

    static void SetUpTestSuite()
    {
        Logger::setupLogging("MicroBenchmarksTest.log", LogLevel::LOG_DEBUG);
        NES_INFO("Setup MicroBenchmarksTest class.");
    }

    static void TearDownTestSuite() { NES_INFO("Tear down MicroBenchmarksTest class."); }

    static SchemaPtr createSchema()
    {
        return Schema::create()
            ->addField("f0", BasicType::UINT64)
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
            if (!fieldTypeSizes.empty())
            {
                if (const auto [lastFieldType, lastFieldSize] = fieldTypeSizes.back(); lastFieldType == fieldType)
                {
                    fieldSize += lastFieldSize;
                    fieldTypeSizes.pop_back();
                }
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

        const auto memoryLayout = Util::createMemoryLayout(keysOnlySchema, bufferManager->getBufferSize());
        memoryLayout->setKeyFieldNames(keyFieldNames);
        return std::make_shared<Nautilus::Interface::PagedVector>(bufferManager, memoryLayout);
    }

    static void clearPagedVector(
        std::shared_ptr<Nautilus::Interface::PagedVector>& pagedVector,
        const Memory::BufferManagerPtr& bufferManager,
        const SeparateKeys separateKeys,
        std::vector<double>& execTimes)
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
            const uint64_t expectedNumPages = std::ceil(static_cast<double>(oldNumTuples) / newCapacity);

            // TODO does this enhance performance?
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
            ASSERT_EQ(newPagedVector->getNumberOfPages(), expectedNumPages);
            pagedVector = newPagedVector;
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

    static std::shared_ptr<Nautilus::Interface::PagedVector> createPagedVector(
        const Memory::MemoryLayouts::MemoryLayoutPtr& memoryLayout,
        const Memory::BufferManagerPtr& bufferManager,
        const uint64_t numBuffers,
        std::vector<double>& execTimes)
    {
        NES_INFO("Data generating...");
        Timer<> timer("generateDataTimer");
        timer.start();

        // TODO does this enhance performance?
        const auto pagedVector = std::make_shared<Nautilus::Interface::PagedVector>(bufferManager, memoryLayout);
        pagedVector->getPages().reserve(numBuffers);

        auto valueCnt = 0UL;
        const auto numFields = memoryLayout->getCapacity() * memoryLayout->getSchema()->getFieldCount();
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

        return pagedVector;
    }

    static void writePagedVectorToFile(
        const std::shared_ptr<Nautilus::Interface::PagedVector>& pagedVector,
        const Memory::MemoryLayouts::MemoryLayoutPtr& oldMemoryLayout,
        const std::vector<std::string>& fileNames,
        const uint64_t fileBufferSize,
        const SeparateKeys separateKeys,
        std::vector<double>& execTimes)
    {
        NES_INFO("File writing...");
        Timer<> timer("writeFileTimer");
        timer.start();

        std::ofstream payloadFile;
        std::ofstream keyFile;

        if (separateKeys == SAME_FILE_KEYS)
        {
            payloadFile = std::ofstream(fileNames.front(), std::ios::out | std::ios::in | std::ios::binary);
        }
        else
        {
            payloadFile = std::ofstream(fileNames.front(), std::ios::out | std::ios::trunc | std::ios::binary);
        }
        ASSERT_TRUE(payloadFile);
        ASSERT_EQ(payloadFile.tellp(), 0);
        if (fileNames.size() > 1)
        {
            ASSERT_EQ(separateKeys, SEPARATE_FILES);
            keyFile = std::ofstream(fileNames.at(1), std::ios::out | std::ios::trunc | std::ios::binary);
            ASSERT_TRUE(keyFile);
            ASSERT_EQ(keyFile.tellp(), 0);
        }

        // TODO remove?
        const auto& pages = pagedVector->getPages();
        const auto numPages = pagedVector->getNumberOfPages();
        const auto memoryLayout = pagedVector->getMemoryLayout();
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

        auto pageIdx = 0UL;
        std::vector<char> keyBuffer(keyFileWriteSize);
        std::vector<char> payloadBuffer(payloadFileWriteSize);

        auto numTuplesLastPage = 0UL;
        const auto oldCapacity = oldMemoryLayout->getCapacity();
        const auto oldFieldTypeSizes = getFieldTypeSizes(oldMemoryLayout);
        const auto oldPagePadding = bufferSize - oldCapacity * oldMemoryLayout->getTupleSize();

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
                    for (auto tupleIdx = 0UL; tupleIdx < page.getNumberOfTuples(); ++tupleIdx, ++tuplesWritten)
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
                for (auto tupleIdx = numTuplesLastPage; tupleIdx < tuplesWritten + numTuplesLastPage; ++tupleIdx)
                {
                    if (tupleIdx > 0 && tupleIdx % oldCapacity == 0)
                    {
                        payloadFile.seekp(oldPagePadding, std::ios::cur);
                    }

                    for (const auto [fieldType, fieldSize] : oldFieldTypeSizes)
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
                }

                numTuplesLastPage = (numTuplesLastPage + tuplesWritten % oldCapacity) % oldCapacity;
                if (numTuplesLastPage == 0)
                {
                    payloadFile.seekp(oldPagePadding, std::ios::cur);
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

    static void writeFileToPagedVector(
        std::shared_ptr<Nautilus::Interface::PagedVector>& pagedVector,
        const Memory::MemoryLayouts::MemoryLayoutPtr& memoryLayout,
        const Memory::BufferManagerPtr& bufferManager,
        const std::vector<std::string>& fileNames,
        const uint64_t expectedNumPages,
        const uint64_t fileBufferSize,
        const SeparateKeys separateKeys,
        std::vector<double>& execTimes)
    {
        NES_INFO("PagedVector writing...");
        Timer<> timer("writePagedVectorTimer");
        timer.start();

        std::ifstream payloadFile;
        std::ifstream keyFile;

        payloadFile = std::ifstream(fileNames.front(), std::ios::in | std::ios::binary);
        ASSERT_TRUE(payloadFile);
        ASSERT_EQ(payloadFile.tellg(), 0);
        if (fileNames.size() > 1)
        {
            ASSERT_EQ(separateKeys, SEPARATE_FILES);
            keyFile = std::ifstream(fileNames.at(1), std::ios::in | std::ios::binary);
            ASSERT_TRUE(keyFile);
            ASSERT_EQ(keyFile.tellg(), 0);
        }

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

        auto pageCnt = 0UL;
        std::vector<char> keyBuffer(keyFileReadSize);
        std::vector<char> payloadBuffer(payloadFileReadSize);

        auto oldPageCnt = 0UL;
        auto oldTupleCnt = 0UL;
        const auto pagePadding = bufferSize - capacity * tupleSize;
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
                    for (auto tupleIdx = 0UL; tupleIdx < capacity; ++tupleIdx, ++oldTupleCnt)
                    {
                        if (separateKeys == SAME_FILE_PAYLOAD && oldTupleCnt >= oldCapacity)
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

    static void comparePagedVectors(
        const std::shared_ptr<Nautilus::Interface::PagedVector>& expectedPagedVector,
        const std::shared_ptr<Nautilus::Interface::PagedVector>& actualPagedVector,
        std::vector<double>& execTimes)
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
        const auto memoryLayout = expectedPagedVector->getMemoryLayout();
        const auto tuplesOnPageSize = memoryLayout->getCapacity() * memoryLayout->getTupleSize();

        ASSERT_EQ(*memoryLayout, *actualPagedVector->getMemoryLayout());

        for (auto pageIdx = 0UL; pageIdx < expectedNumPages; ++pageIdx)
        {
            const auto* const expectedBufferAddr = expectedPages[pageIdx].getBuffer();
            const auto* const actualBufferAddr = actualPages[pageIdx].getBuffer();

            ASSERT_EQ(std::memcmp(expectedBufferAddr, actualBufferAddr, tuplesOnPageSize), 0);

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

    static void runTests(const SeparateKeys separateKeys)
    {
        std::vector<double> execTimes;

        const auto [bufferSize, fileBufferSizePercent, keyFieldNames] = GetParam();
        const auto numBuffers = DATA_SIZE / bufferSize;
        const auto fileBufferSize = std::max(1UL, fileBufferSizePercent * numBuffers / 100) * bufferSize;

        std::vector<std::string> fileNames = {"../../../micro_benchmarks_payload.dat"};
        if (separateKeys == SEPARATE_FILES)
        {
            fileNames.emplace_back("../../../micro_benchmarks_keys.dat");
        }

        const auto testSchema = createSchema();
        const auto bufferManager = Memory::BufferManager::create(bufferSize, 3 * numBuffers);
        const auto memoryLayout = Util::createMemoryLayout(testSchema, bufferSize);
        memoryLayout->setKeyFieldNames(keyFieldNames);

        auto pagedVector = createPagedVector(memoryLayout, bufferManager, numBuffers, execTimes);
        NES_INFO("PagedVector copying...");
        const auto expectedPagedVector = copyPagedVector(*pagedVector);
        NES_INFO("PagedVector copied");

        if (separateKeys == SAME_FILE_KEYS)
        {
            writePagedVectorToFile(pagedVector, memoryLayout, fileNames, fileBufferSize, SAME_FILE_PAYLOAD, execTimes);
            execTimes.pop_back();
            clearPagedVector(pagedVector, bufferManager, SAME_FILE_PAYLOAD, execTimes);
            execTimes.pop_back();
        }

        writePagedVectorToFile(pagedVector, memoryLayout, fileNames, fileBufferSize, separateKeys, execTimes);
        clearPagedVector(pagedVector, bufferManager, separateKeys, execTimes);

        writeFileToPagedVector(pagedVector, memoryLayout, bufferManager, fileNames, numBuffers, fileBufferSize, separateKeys, execTimes);
        comparePagedVectors(expectedPagedVector, pagedVector, execTimes);

        ASSERT_EQ(execTimes.size(), 5);
        //createMeasurementsCSV("fileName.csv");
    }
};

TEST_P(MicroBenchmarksTest, noSeparation)
{
    runTests(NO_SEPARATION);
}

TEST_P(MicroBenchmarksTest, DISABLED_separateFiles)
{
    runTests(SEPARATE_FILES);
}

TEST_P(MicroBenchmarksTest, DISABLED_sameFileOnlyPayload)
{
    runTests(SAME_FILE_PAYLOAD);
}

TEST_P(MicroBenchmarksTest, DISABLED_sameFilePayloadAndKey)
{
    runTests(SAME_FILE_KEYS);
}

INSTANTIATE_TEST_CASE_P(
    //Benchmarks, MicroBenchmarksTest, ::testing::Combine(::testing::Values(1024, 4096, 134217728, 268435456, 1073741824), ::testing::Values(0, 100)));
    Benchmarks,
    MicroBenchmarksTest,
    ::testing::Combine(
        ::testing::Values(4096),
        ::testing::Values(0, 100),
        ::testing::Values(
            std::vector<std::string>{"f0", "f1"},
            std::vector<std::string>{"f0", "f5"},
            std::vector<std::string>{"f0", "f1", "f2", "f3", "f4"},
            std::vector<std::string>{"f0", "f2", "f4", "f6", "f8"})));

}
