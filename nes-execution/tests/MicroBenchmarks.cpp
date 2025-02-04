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

/// NOTE: Comment out define if measurements are taken
//#define LOGGING

#define MEASUREMENT_DIR "05-02"

namespace NES
{

using FieldType = enum : uint8_t { KEY, PAYLOAD };
using SeparateKeys = enum : uint8_t { NO_SEPARATION, SAME_FILE_KEYS, SAME_FILE_PAYLOAD, SEPARATE_FILES_KEYS, SEPARATE_FILES_PAYLOAD };

class MicroBenchmarksTest : public Testing::BaseUnitTest,
                            public testing::WithParamInterface<std::tuple<uint64_t, uint64_t, std::vector<std::string>>>
{
public:
    /// NOTE: The test needs approximately up to five times DATA_SIZE of memory
    static constexpr uint64_t DATA_SIZE = 4294967296; /// 4GB

    /// INFO: Each experiment will run NUM_MEASUREMENTS many times
    static constexpr uint64_t NUM_MEASUREMENTS = 5;

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

    static std::vector<std::tuple<FieldType, uint64_t>> getFieldTypeSizes(const Memory::MemoryLayouts::MemoryLayoutPtr& memoryLayout)
    {
        std::vector<std::tuple<FieldType, uint64_t>> fieldTypeSizes;
        const auto schema = memoryLayout->getSchema();
        const auto& keyFieldNames = memoryLayout->getKeyFieldNames();

        for (auto fieldIdx = 0UL; fieldIdx < schema->getFieldCount(); ++fieldIdx)
        {
            FieldType fieldType = PAYLOAD;
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
        const auto fieldSizes = memoryLayout->getFieldSizes();
        const auto& keyFieldNames = memoryLayout->getKeyFieldNames();

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

    static std::shared_ptr<Nautilus::Interface::PagedVector> createPagedVector(
        const Memory::MemoryLayouts::MemoryLayoutPtr& memoryLayout,
        const Memory::BufferManagerPtr& bufferManager,
        const uint64_t numBuffers,
        std::vector<double>& execTimesInMs)
    {
        NES_INFO("Data generating...");
        Timer<> timer("generateDataTimer");
        timer.start();

        const auto pagedVector = std::make_shared<Nautilus::Interface::PagedVector>(bufferManager, memoryLayout);
        pagedVector->getPages().reserve(numBuffers);

        auto valueCnt = 0UL;
        const auto numFields = memoryLayout->getCapacity() * memoryLayout->getSchema()->getFieldCount();

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
        }

        timer.pause();
        auto timeInMs = timer.getPrintTime();
        execTimesInMs.emplace_back(timeInMs);
        NES_INFO("Data generated in {} ms", timeInMs);

        return pagedVector;
    }

    template <SeparateKeys SeparateKeys>
    static void clearPagedVector(
        std::shared_ptr<Nautilus::Interface::PagedVector>& pagedVector,
        const Memory::BufferManagerPtr& bufferManager,
        std::vector<double>& execTimesInMs)
    {
        NES_INFO("PagedVector clearing...");
        Timer<> timer("clearTimer");
        timer.start();

        if constexpr (SeparateKeys == SAME_FILE_PAYLOAD || SeparateKeys == SEPARATE_FILES_PAYLOAD)
        {
            const auto oldMemoryLayout = pagedVector->getMemoryLayout();
            if (const auto& keyFieldNames = oldMemoryLayout->getKeyFieldNames(); keyFieldNames.empty())
            {
                pagedVector->getPages().clear();
                ASSERT_EQ(pagedVector->getNumberOfPages(), 0);
            }
            else
            {
                const auto fieldTypeSizes = getFieldTypeSizes(oldMemoryLayout);
                const auto oldCapacity = oldMemoryLayout->getCapacity();
                const auto oldNumTuples = oldCapacity * pagedVector->getNumberOfPages();

                const auto newPagedVector = createKeysOnlyPagedVector(keyFieldNames, bufferManager);
                const auto newMemoryLayout = newPagedVector->getMemoryLayout();
                const auto newCapacity = newMemoryLayout->getCapacity();

                const uint64_t expectedNumPages = std::ceil(static_cast<double>(oldNumTuples) / newCapacity);
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

                newPages.back().setNumberOfTuples(tupleCnt);
                pagedVector = newPagedVector;
                ASSERT_EQ(pagedVector->getNumberOfPages(), expectedNumPages);
            }
        }
        else
        {
            pagedVector->getPages().clear();
            ASSERT_EQ(pagedVector->getNumberOfPages(), 0);
        }

        timer.pause();
        auto timeInMs = timer.getPrintTime();
        execTimesInMs.emplace_back(timeInMs);
        NES_INFO("PagedVector cleared in {} ms", timeInMs);
    }

    template <SeparateKeys SeparateKeys>
    static void writePagedVectorToFile(
        const std::shared_ptr<Nautilus::Interface::PagedVector>& pagedVector,
        const Memory::MemoryLayouts::MemoryLayoutPtr& oldMemoryLayout,
        const std::vector<std::string>& fileNames,
        const uint64_t fileBufferSize,
        std::vector<double>& execTimesInMs)
    {
        NES_INFO("File writing...");
        Timer<> timer("writeFileTimer");
        timer.start();

        const auto& pages = pagedVector->getPages();
        const auto numPages = pagedVector->getNumberOfPages();
        const auto memoryLayout = pagedVector->getMemoryLayout();
        const auto bufferSize = memoryLayout->getBufferSize();
        const auto capacity = memoryLayout->getCapacity();
        ASSERT_EQ(fileBufferSize % bufferSize, 0);

        const auto fieldTypeSizes = getFieldTypeSizes(memoryLayout);
        const auto [keySize, payloadSize] = getKeyAndPayloadSize(memoryLayout);
        const uint64_t numBuffers = std::ceil(static_cast<double>(bufferSize * numPages) / fileBufferSize);
        const auto tenPercentOfNumPages = std::max(1UL, (numPages * 10) / 100);
        const auto numPagesPerBuffer = fileBufferSize / bufferSize;

        auto keyFileWriteSize = 0UL;
        auto payloadFileWriteSize = 0UL;
        if constexpr (SeparateKeys == SEPARATE_FILES_KEYS || SeparateKeys == SEPARATE_FILES_PAYLOAD)
        {
            if constexpr (SeparateKeys == SEPARATE_FILES_KEYS)
            {
                keyFileWriteSize = numPagesPerBuffer * capacity * keySize;
            }
            payloadFileWriteSize = numPagesPerBuffer * capacity * payloadSize;
        }
        else if constexpr (SeparateKeys == NO_SEPARATION)
        {
            payloadFileWriteSize = fileBufferSize;
        }

        auto pageIdx = 0UL;
        auto numTuplesLastPage = 0UL;
        const auto oldCapacity = oldMemoryLayout->getCapacity();
        const auto oldFieldTypeSizes = getFieldTypeSizes(oldMemoryLayout);
        const auto oldPagePadding = bufferSize - oldCapacity * oldMemoryLayout->getTupleSize();

        timer.snapshot("defined variables");

        std::vector<char> keyBuffer(keyFileWriteSize);
        std::vector<char> payloadBuffer(payloadFileWriteSize);

        timer.snapshot("allocated memory");

        std::ofstream payloadFile;
        std::ofstream keyFile;

        if constexpr (SeparateKeys == SAME_FILE_KEYS)
        {
            payloadFile = std::ofstream(fileNames.front(), std::ios::out | std::ios::in | std::ios::binary);
        }
        else
        {
            payloadFile = std::ofstream(fileNames.front(), std::ios::out | std::ios::trunc | std::ios::binary);
        }
        ASSERT_TRUE(payloadFile.is_open());
        ASSERT_EQ(payloadFile.tellp(), 0);
        if constexpr (SeparateKeys == SEPARATE_FILES_KEYS)
        {
            ASSERT_GT(fileNames.size(), 1);
            keyFile = std::ofstream(fileNames.at(1), std::ios::out | std::ios::trunc | std::ios::binary);
            ASSERT_TRUE(keyFile.is_open());
            ASSERT_EQ(keyFile.tellp(), 0);
        }

        timer.snapshot("opened file");

        for (auto bufferIdx = 0UL; bufferIdx < numBuffers; ++bufferIdx)
        {
            auto* fieldAddrKeyBuffer = keyBuffer.data();
            auto* fieldAddrPayloadBuffer = payloadBuffer.data();

            for (auto pageNum = 0UL; pageNum < numPagesPerBuffer && pageIdx < numPages; ++pageNum, ++pageIdx)
            {
                const auto& page = pages[pageIdx];
                const auto* fieldAddrPagedVector = page.getBuffer();

                if constexpr (SeparateKeys == NO_SEPARATION)
                {
                    std::memcpy(fieldAddrPayloadBuffer, fieldAddrPagedVector, bufferSize);
                    fieldAddrPayloadBuffer += bufferSize;
                }
                else if constexpr (SeparateKeys == SEPARATE_FILES_KEYS || SeparateKeys == SEPARATE_FILES_PAYLOAD)
                {
                    for (auto tupleIdx = 0UL; tupleIdx < capacity; ++tupleIdx)
                    {
                        for (const auto [fieldType, fieldSize] : fieldTypeSizes)
                        {
                            if (fieldType == KEY)
                            {
                                if constexpr (SeparateKeys == SEPARATE_FILES_KEYS)
                                {
                                    std::memcpy(fieldAddrKeyBuffer, fieldAddrPagedVector, fieldSize);
                                    fieldAddrKeyBuffer += fieldSize;
                                }
                            }
                            else
                            {
                                std::memcpy(fieldAddrPayloadBuffer, fieldAddrPagedVector, fieldSize);
                                fieldAddrPayloadBuffer += fieldSize;
                            }

                            fieldAddrPagedVector += fieldSize;
                        }
                    }
                }
                else
                {
                    for (auto tupleIdx = numTuplesLastPage; tupleIdx < page.getNumberOfTuples() + numTuplesLastPage; ++tupleIdx)
                    {
                        if (tupleIdx > 0 && tupleIdx % oldCapacity == 0)
                        {
                            payloadFile.seekp(oldPagePadding, std::ios::cur);
                        }

                        for (const auto [fieldType, fieldSize] : oldFieldTypeSizes)
                        {
                            if ((fieldType == KEY && SeparateKeys == SAME_FILE_KEYS)
                                || (fieldType == PAYLOAD && SeparateKeys == SAME_FILE_PAYLOAD))
                            {
                                payloadFile.write(reinterpret_cast<const char*>(fieldAddrPagedVector), fieldSize);
                                fieldAddrPagedVector += fieldSize;
                            }
                            else
                            {
                                payloadFile.seekp(fieldSize, std::ios::cur);
                                if constexpr (SeparateKeys == SAME_FILE_PAYLOAD)
                                {
                                    fieldAddrPagedVector += fieldSize;
                                }
                            }
                        }
                    }

                    if constexpr (SeparateKeys == SAME_FILE_KEYS)
                    {
                        numTuplesLastPage = (numTuplesLastPage + page.getNumberOfTuples() % oldCapacity) % oldCapacity;
                    }
                    if (numTuplesLastPage == 0)
                    {
                        payloadFile.seekp(oldPagePadding, std::ios::cur);
                    }
                }

#ifdef LOGGING
                if (pageIdx % tenPercentOfNumPages == 0)
                {
                    NES_INFO("File {}% written", std::round(100 * static_cast<double>(pageIdx) / numPages));
                }
#endif
            }

            if (bufferIdx == 0)
            {
                timer.snapshot("wrote to buffer");
            }

            if constexpr (SeparateKeys == NO_SEPARATION)
            {
                payloadFile.write(payloadBuffer.data(), fieldAddrPayloadBuffer - payloadBuffer.data());
            }
            else if constexpr (SeparateKeys == SEPARATE_FILES_KEYS || SeparateKeys == SEPARATE_FILES_PAYLOAD)
            {
                if constexpr (SeparateKeys == SEPARATE_FILES_KEYS)
                {
                    keyFile.write(keyBuffer.data(), fieldAddrKeyBuffer - keyBuffer.data());
                }
                payloadFile.write(payloadBuffer.data(), fieldAddrPayloadBuffer - payloadBuffer.data());
            }

            if (bufferIdx == 0)
            {
                timer.snapshot("wrote to file");
            }
        }

        payloadFile.close();
        if (fileNames.size() > 1)
        {
            keyFile.close();
        }

        timer.snapshot("closed file");

        timer.pause();
        auto timeInMs = timer.getPrintTime();
        execTimesInMs.emplace_back(timeInMs);
        for (auto snapshot : timer.getSnapshots())
        {
            execTimesInMs.emplace_back(snapshot.getPrintTime());
        }
        NES_INFO("File written in {} ms", timeInMs);
    }

    template <SeparateKeys SeparateKeys>
    static void writeFileToPagedVector(
        std::shared_ptr<Nautilus::Interface::PagedVector>& pagedVector,
        const Memory::MemoryLayouts::MemoryLayoutPtr& memoryLayout,
        const Memory::BufferManagerPtr& bufferManager,
        const std::vector<std::string>& fileNames,
        const uint64_t expectedNumPages,
        const uint64_t fileBufferSize,
        std::vector<double>& execTimesInMs)
    {
        NES_INFO("PagedVector writing...");
        Timer<> timer("writePagedVectorTimer");
        timer.start();

        auto newPagedVector = std::make_shared<Nautilus::Interface::PagedVector>(bufferManager, memoryLayout);
        newPagedVector->getPages().reserve(expectedNumPages);
        auto& pages = newPagedVector->getPages();
        const auto tupleSize = memoryLayout->getTupleSize();
        const auto bufferSize = memoryLayout->getBufferSize();
        const auto capacity = memoryLayout->getCapacity();
        ASSERT_EQ(fileBufferSize % bufferSize, 0);

        const auto fieldTypeSizes = getFieldTypeSizes(memoryLayout);
        const auto [keySize, payloadSize] = getKeyAndPayloadSize(memoryLayout);
        const auto tenPercentOfNumPages = std::max(1UL, (expectedNumPages * 10) / 100);
        const auto numPagesPerBuffer = fileBufferSize / bufferSize;

        auto keyFileReadSize = 0UL;
        auto payloadFileReadSize = fileBufferSize;
        if constexpr (SeparateKeys == SEPARATE_FILES_KEYS || SeparateKeys == SEPARATE_FILES_PAYLOAD)
        {
            if constexpr (SeparateKeys == SEPARATE_FILES_KEYS)
            {
                keyFileReadSize = numPagesPerBuffer * capacity * keySize;
            }
            payloadFileReadSize = numPagesPerBuffer * capacity * payloadSize;
        }

        auto pageIdx = 0UL;
        auto oldPageCnt = 0UL;
        auto oldTupleCnt = 0UL;
        auto& oldPages = pagedVector->getPages();
        const auto oldPagePadding = bufferSize - capacity * tupleSize;
        const auto oldCapacity = pagedVector->getMemoryLayout()->getCapacity();
        auto* fieldAddrKeyPagedVector = !oldPages.empty() ? oldPages.front().getBuffer() : nullptr;

        timer.snapshot("defined variables");

        std::vector<char> keyBuffer(keyFileReadSize);
        std::vector<char> payloadBuffer(payloadFileReadSize);

        timer.snapshot("allocated memory");

        std::ifstream payloadFile;
        std::ifstream keyFile;

        payloadFile = std::ifstream(fileNames.front(), std::ios::in | std::ios::binary);
        ASSERT_TRUE(payloadFile.is_open());
        ASSERT_EQ(payloadFile.tellg(), 0);
        if constexpr (SeparateKeys == SEPARATE_FILES_KEYS)
        {
            ASSERT_GT(fileNames.size(), 1);
            keyFile = std::ifstream(fileNames.at(1), std::ios::in | std::ios::binary);
            ASSERT_TRUE(keyFile.is_open());
            ASSERT_EQ(keyFile.tellg(), 0);
        }

        timer.snapshot("opened file");

        while (payloadFile.read(payloadBuffer.data(), payloadFileReadSize) || payloadFile.gcount() > 0)
        {
            if (pageIdx == 0)
            {
                timer.snapshot("wrote to buffer");
            }

            auto* fieldAddrKeyBuffer = keyBuffer.data();
            auto* fieldAddrPayloadBuffer = payloadBuffer.data();

            if constexpr (SeparateKeys == SEPARATE_FILES_KEYS)
            {
                keyFile.read(keyBuffer.data(), keyFileReadSize);
            }

            for (auto pageNum = 0UL; pageNum < numPagesPerBuffer && pageIdx < expectedNumPages; ++pageNum, ++pageIdx)
            {
                newPagedVector->appendPageIfFull();
                auto& page = pages.back();
                page.setNumberOfTuples(capacity);

                auto* fieldAddrPagedVector = page.getBuffer();

                if constexpr (SeparateKeys == NO_SEPARATION || SeparateKeys == SAME_FILE_KEYS)
                {
                    std::memcpy(fieldAddrPagedVector, fieldAddrPayloadBuffer, bufferSize);
                    fieldAddrPayloadBuffer += bufferSize;
                }
                else
                {
                    for (auto tupleIdx = 0UL; tupleIdx < capacity; ++tupleIdx)
                    {
                        if constexpr (SeparateKeys == SAME_FILE_PAYLOAD || SeparateKeys == SEPARATE_FILES_PAYLOAD)
                        {
                            if (oldTupleCnt++ >= oldCapacity)
                            {
                                fieldAddrKeyPagedVector = oldPages[++oldPageCnt].getBuffer();
                                oldTupleCnt = 1;
                            }
                        }

                        for (const auto [fieldType, fieldSize] : fieldTypeSizes)
                        {
                            if (fieldType == KEY)
                            {
                                if constexpr (SeparateKeys == SEPARATE_FILES_KEYS)
                                {
                                    std::memcpy(fieldAddrPagedVector, fieldAddrKeyBuffer, fieldSize);
                                    fieldAddrKeyBuffer += fieldSize;
                                }
                                else
                                {
                                    std::memcpy(fieldAddrPagedVector, fieldAddrKeyPagedVector, fieldSize);
                                    fieldAddrKeyPagedVector += fieldSize;

                                    if constexpr (SeparateKeys == SAME_FILE_PAYLOAD)
                                    {
                                        fieldAddrPayloadBuffer += fieldSize;
                                    }
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

                    if constexpr (SeparateKeys == SAME_FILE_PAYLOAD)
                    {
                        fieldAddrPayloadBuffer += oldPagePadding;
                    }
                }

#ifdef LOGGING
                if (pageIdx % tenPercentOfNumPages == 0)
                {
                    NES_INFO("PagedVector {}% written", std::round(100 * static_cast<double>(pageIdx) / expectedNumPages));
                }
#endif
            }

            if (pageIdx == numPagesPerBuffer)
            {
                timer.snapshot("wrote to memory");
            }
        }

        payloadFile.close();
        if (fileNames.size() > 1)
        {
            keyFile.close();
        }

        timer.snapshot("closed file");

        if constexpr (SeparateKeys != SAME_FILE_PAYLOAD && SeparateKeys != SEPARATE_FILES_PAYLOAD)
        {
            pagedVector = newPagedVector;
        }
        else
        {
            if (payloadSize != 0)
            {
                pagedVector = newPagedVector;
            }
        }

        timer.pause();
        auto timeInMs = timer.getPrintTime();
        execTimesInMs.emplace_back(timeInMs);
        for (auto snapshot : timer.getSnapshots())
        {
            execTimesInMs.emplace_back(snapshot.getPrintTime());
        }
        NES_INFO("PagedVector written in {} ms", timeInMs);
    }

    static void comparePagedVectors(
        const std::shared_ptr<Nautilus::Interface::PagedVector>& expectedPagedVector,
        const std::shared_ptr<Nautilus::Interface::PagedVector>& actualPagedVector,
        std::vector<double>& execTimesInMs)
    {
        NES_INFO("PagedVectors comparing...");
        Timer<> timer("compareTimer");
        timer.start();

        const auto expectedNumPages = expectedPagedVector->getNumberOfPages();
        const auto actualNumPages = actualPagedVector->getNumberOfPages();

        ASSERT_EQ(expectedNumPages, actualNumPages);

        const auto& expectedPages = expectedPagedVector->getPages();
        const auto& actualPages = actualPagedVector->getPages();

        const auto memoryLayout = expectedPagedVector->getMemoryLayout();
        const auto tuplesOnPageSize = memoryLayout->getCapacity() * memoryLayout->getTupleSize();

        ASSERT_EQ(*memoryLayout, *actualPagedVector->getMemoryLayout());

        for (auto pageIdx = 0UL; pageIdx < expectedNumPages; ++pageIdx)
        {
            const auto& expectedBuffer = expectedPages[pageIdx];
            const auto& actualBuffer = actualPages[pageIdx];

            ASSERT_EQ(expectedBuffer.getNumberOfTuples(), actualBuffer.getNumberOfTuples());
            ASSERT_EQ(std::memcmp(expectedBuffer.getBuffer(), actualBuffer.getBuffer(), tuplesOnPageSize), 0);
        }

        timer.pause();
        auto timeInMs = timer.getPrintTime();
        execTimesInMs.emplace_back(timeInMs);
        NES_INFO("PagedVectors compared in {} ms", timeInMs);
    }

    template <SeparateKeys SeparateKeys>
    static std::string createMeasurementsFileName(
        const std::string& testName,
        const uint64_t bufferSize,
        const uint64_t fileBufferSizePercent,
        const std::vector<std::string>& keyFieldNames)
    {
        std::string separateKeys;
        std::stringstream filename;

        if constexpr (SeparateKeys == NO_SEPARATION)
        {
            separateKeys = "NO_SEPARATION";
        }
        else if constexpr (SeparateKeys == SAME_FILE_KEYS)
        {
            separateKeys = "SAME_FILE_KEYS";
        }
        else if constexpr (SeparateKeys == SAME_FILE_PAYLOAD)
        {
            separateKeys = "SAME_FILE_PAYLOAD";
        }
        else if constexpr (SeparateKeys == SEPARATE_FILES_KEYS)
        {
            separateKeys = "SEPARATE_FILES_KEYS";
        }
        else if constexpr (SeparateKeys == SEPARATE_FILES_PAYLOAD)
        {
            separateKeys = "SEPARATE_FILES_PAYLOAD";
        }

        filename << "../../../measurements/data/" << MEASUREMENT_DIR << "/micro_benchmarks_" << testName << "_" << separateKeys;
        filename << "_bufferSize_" << bufferSize;
        filename << "_fileBufferSizePercent_" << fileBufferSizePercent;
        if (!keyFieldNames.empty())
        {
            filename << "_keys";
        }
        for (const auto& key : keyFieldNames)
        {
            filename << "_" << key;
        }
        filename << ".csv";

        return filename.str();
    }

    static void createMeasurementsCSV(const std::vector<double>& execTimesInMs, const uint64_t numCols, const std::string& fileName)
    {
        std::ofstream file(fileName, std::ios::out | std::ios::trunc);
        ASSERT_TRUE(file.is_open());

        ASSERT_EQ(execTimesInMs.size(), numCols * NUM_MEASUREMENTS);
        for (auto rowIdx = 0UL; rowIdx < execTimesInMs.size() / numCols; ++rowIdx)
        {
            for (auto colIdx = 0UL; colIdx < numCols; ++colIdx)
            {
                file << execTimesInMs[rowIdx * numCols + colIdx];
                if (colIdx < numCols - 1)
                {
                    file << ",";
                }
            }
            file << "\n";
        }

        file.close();
    }

    template <SeparateKeys SeparateKeys>
    static void runSeparationTests()
    {
        std::vector<double> execTimesInMs;

        const auto [bufferSize, fileBufferSizePercent, keyFieldNames] = GetParam();
        const auto numBuffers = DATA_SIZE / bufferSize;
        const auto fileBufferSize
            = std::max(1UL, static_cast<uint64_t>(std::round(fileBufferSizePercent / 100.0 * numBuffers))) * bufferSize;

        std::vector<std::string> fileNames = {"../../../measurements/data/micro_benchmarks_payload.dat"};
        if constexpr (SeparateKeys == SEPARATE_FILES_KEYS)
        {
            fileNames.emplace_back("../../../measurements/data/micro_benchmarks_keys.dat");
        }

        const auto testSchema = createSchema();
        const auto bufferManager = Memory::BufferManager::create(bufferSize, 3 * numBuffers);
        const auto memoryLayout = Util::createMemoryLayout(testSchema, bufferSize);
        memoryLayout->setKeyFieldNames(keyFieldNames);

        for (auto i = 0UL; i < NUM_MEASUREMENTS; ++i)
        {
            auto pagedVector = createPagedVector(memoryLayout, bufferManager, numBuffers, execTimesInMs);
            NES_INFO("PagedVector copying...");
            const auto expectedPagedVector = copyPagedVector(*pagedVector);
            NES_INFO("PagedVector copied");

            if constexpr (SeparateKeys == SAME_FILE_KEYS)
            {
                const auto execTimesSize = execTimesInMs.size();
                writePagedVectorToFile<SAME_FILE_PAYLOAD>(pagedVector, memoryLayout, fileNames, fileBufferSize, execTimesInMs);
                clearPagedVector<SAME_FILE_PAYLOAD>(pagedVector, bufferManager, execTimesInMs);
                execTimesInMs.resize(execTimesSize);
            }

            writePagedVectorToFile<SeparateKeys>(pagedVector, memoryLayout, fileNames, fileBufferSize, execTimesInMs);
            clearPagedVector<SeparateKeys>(pagedVector, bufferManager, execTimesInMs);

            writeFileToPagedVector<SeparateKeys>(
                pagedVector, memoryLayout, bufferManager, fileNames, numBuffers, fileBufferSize, execTimesInMs);
            comparePagedVectors(expectedPagedVector, pagedVector, execTimesInMs);
        }

        createMeasurementsCSV(
            execTimesInMs,
            17,
            createMeasurementsFileName<SeparateKeys>("SeparationTest", bufferSize, fileBufferSizePercent, keyFieldNames));
    }
};

TEST_P(MicroBenchmarksTest, separationTestNoSeparation)
{
    runSeparationTests<NO_SEPARATION>();
}

TEST_P(MicroBenchmarksTest, separationTestSeparateFilesPayloadOnly)
{
    runSeparationTests<SEPARATE_FILES_PAYLOAD>();
}

TEST_P(MicroBenchmarksTest, separationTestSeparateFilesPayloadAndKey)
{
    runSeparationTests<SEPARATE_FILES_KEYS>();
}

TEST_P(MicroBenchmarksTest, DISABLED_separationTestSameFilePayloadOnly)
{
    runSeparationTests<SAME_FILE_PAYLOAD>();
}

TEST_P(MicroBenchmarksTest, DISABLED_separationTestSameFilePayloadAndKey)
{
    runSeparationTests<SAME_FILE_KEYS>();
}

TEST_F(MicroBenchmarksTest, bulkWriteTest)
{
    const std::vector<uint64_t> dataSizes
        = {8 * 1024,
           32 * 1024,
           128 * 1024,
           512 * 1024,
           1 * 1024 * 1024,
           8 * 1024 * 1024,
           32 * 1024 * 1024,
           128 * 1024 * 1024,
           512 * 1024 * 1024,
           1 * 1024 * 1024 * 1024,
           2147483648,
           3221225472,
           4294967296};
    const std::vector<uint64_t> bufferSizes = {1 * 1024, 4 * 1024, 8 * 1024, 16 * 1024, 32 * 1024, 64 * 1024, 128 * 1024};

    for (const auto dataSize : dataSizes)
    {
        for (const auto bufferSize : bufferSizes)
        {
            if (bufferSize > dataSize)
            {
                continue;
            }

            std::vector<double> execTimesInMs;
            const auto numBuffers = dataSize / bufferSize;
            std::vector<std::string> fileNames = {"../../../measurements/data/micro_benchmarks_bulk.dat"};

            const auto testSchema = createSchema()->addField("f0", BasicType::UINT64);
            const auto bufferManager = Memory::BufferManager::create(bufferSize, numBuffers);
            const auto memoryLayout = Util::createMemoryLayout(testSchema, bufferSize);

            for (auto i = 0UL; i < NUM_MEASUREMENTS; ++i)
            {
                auto pagedVector = createPagedVector(memoryLayout, bufferManager, numBuffers, execTimesInMs);
                writePagedVectorToFile<NO_SEPARATION>(pagedVector, memoryLayout, fileNames, bufferSize, execTimesInMs);
            }

            createMeasurementsCSV(execTimesInMs, 8, createMeasurementsFileName<NO_SEPARATION>("BulkWriteTest", bufferSize, dataSize, {}));
        }
    }
}

INSTANTIATE_TEST_CASE_P(
    Benchmarks,
    MicroBenchmarksTest,
    ::testing::Combine(
        ::testing::Values(1024, 4096, 8192, 16384, 32768, 65536, 131072),
        ::testing::Values(0, 20, 40, 60, 80, 100),
        ::testing::Values(
            std::vector<std::string>{},
            std::vector<std::string>{"f0"},
            std::vector<std::string>{"f5"},
            std::vector<std::string>{"f0", "f1"},
            std::vector<std::string>{"f0", "f5"},
            std::vector<std::string>{"f0", "f1", "f2"},
            std::vector<std::string>{"f0", "f1", "f5"},
            std::vector<std::string>{"f0", "f4", "f8"},
            std::vector<std::string>{"f0", "f1", "f2", "f3"},
            std::vector<std::string>{"f0", "f1", "f5", "f6"},
            std::vector<std::string>{"f0", "f3", "f6", "f9"},
            std::vector<std::string>{"f0", "f1", "f2", "f3", "f4"},
            std::vector<std::string>{"f0", "f1", "f3", "f6", "f7"},
            std::vector<std::string>{"f0", "f2", "f4", "f6", "f8"},
            std::vector<std::string>{"f0", "f1", "f2", "f3", "f4", "f5"},
            std::vector<std::string>{"f0", "f1", "f3", "f4", "f7", "f8"},
            std::vector<std::string>{"f0", "f1", "f3", "f5", "f7", "f9"},
            std::vector<std::string>{"f0", "f1", "f2", "f3", "f4", "f5", "f6"},
            std::vector<std::string>{"f0", "f1", "f3", "f4", "f6", "f7", "f9"},
            std::vector<std::string>{"f0", "f1", "f2", "f3", "f4", "f5", "f6", "f7"},
            std::vector<std::string>{"f0", "f1", "f2", "f4", "f5", "f7", "f8", "f9"},
            std::vector<std::string>{"f0", "f1", "f2", "f3", "f4", "f5", "f6", "f7", "f8"})));

}
