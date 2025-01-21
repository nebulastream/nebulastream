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
    static constexpr uint64_t MEM_CONSUMPTION = 6442450944; /// 6GB //12884901888; /// 12GB //17179869184; /// 16GB
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

    static Nautilus::Interface::PagedVector copyPagedVector(Nautilus::Interface::PagedVector pagedVector) { return pagedVector; }

    void clearPagedVector(Nautilus::Interface::PagedVector& pagedVector, const SeparateKeys separateKeys)
    {
        NES_INFO("PagedVector clearing...");
        Timer<> timer("clearTimer");
        timer.start();

        if (separateKeys != NO_SEPARATION)
        {
        }
        else
        {
            pagedVector.getPages().clear();
            ASSERT_EQ(pagedVector.getNumberOfPages(), 0);
        }

        timer.snapshot("done clearing");
        timer.pause();
        auto timeInMs = timer.getPrintTime();
        execTimes.emplace_back(timeInMs);
        NES_INFO("PagedVector cleared in {} ms", timeInMs);
    }

    void generateData(Nautilus::Interface::PagedVector& pagedVector, const uint64_t numBuffers)
    {
        NES_INFO("Data generating...");
        Timer<> timer("generateDataTimer");
        timer.start();

        // TODO does this enhance performance?
        pagedVector.appendPageIfFull();
        pagedVector.getPages().reserve(numBuffers + pagedVector.getNumberOfPages() - 1);

        const auto memoryLayout = pagedVector.getMemoryLayout();
        const auto numFields = memoryLayout->getCapacity() * memoryLayout->getSchema()->getFieldCount();

        auto valueCnt = 0UL;
        const auto tenPercentOfNumBuffers = std::max(1UL, (numBuffers * 10) / 100);

        for (auto bufferIdx = 0UL; bufferIdx < numBuffers; ++bufferIdx)
        {
            pagedVector.appendPageIfFull();
            auto& tupleBuffer = pagedVector.getPages().back();
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
        Nautilus::Interface::PagedVector& pagedVector,
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
            ASSERT_EQ(payloadFile.tellp(), 0);
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

        // TODO remove?
        const auto& pages = pagedVector.getPages();
        const auto numPages = pagedVector.getNumberOfPages();
        const auto memoryLayout = pagedVector.getMemoryLayout();
        const auto tupleSize = memoryLayout->getTupleSize();
        const auto bufferSize = memoryLayout->getBufferSize();
        const auto capacity = memoryLayout->getCapacity();
        ASSERT_EQ(fileBufferSize % bufferSize, 0);

        auto pageIdx = 0UL;
        std::vector<char> keyBuffer(fileBufferSize);
        std::vector<char> payloadBuffer(fileBufferSize);
        const auto fieldTypeSizes = getFieldTypeSizes(memoryLayout);
        const auto [keySize, payloadSize] = getKeyAndPayloadSize(memoryLayout);

        const auto tenPercentOfNumPages = std::max(1UL, (numPages * 10) / 100);
        for (auto bufferIdx = 0UL; bufferIdx < std::ceil(bufferSize * numPages / fileBufferSize); ++bufferIdx)
        {
            auto tuplesWritten = 0UL;
            for (auto pageNum = 0UL; pageNum < fileBufferSize / bufferSize && pageIdx < numPages; ++pageNum, ++pageIdx)
            {
                const auto& page = pages[pageIdx];

                if (separateKeys != NO_SEPARATION)
                {
                    for (auto tupleIdx = 0UL; tupleIdx < capacity; ++tupleIdx)
                    {
                        ++tuplesWritten;
                        auto fieldAddrPage = tupleIdx * tupleSize;
                        auto* fieldAddrBuffer = payloadBuffer.data() + (pageNum * bufferSize + tupleIdx) * tupleSize;
                        for (const auto [fieldType, fieldSize] : fieldTypeSizes)
                        {
                            const auto* fieldAddrPagedVector = fieldAddrPage + page.getBuffer();
                            if (separateKeys == SEPARATE_FILES)
                            {
                                if (fieldType == KEY)
                                {
                                    fieldAddrBuffer = keyBuffer.data() + (pageNum * capacity + tupleIdx) * keySize;
                                }
                                else
                                {
                                    fieldAddrBuffer = payloadBuffer.data() + (pageNum * capacity + tupleIdx) * payloadSize;
                                }
                            }

                            // TODO change SAME_FILE_ logic
                            if (separateKeys == SEPARATE_FILES || (separateKeys == SAME_FILE_KEYS && fieldType == KEY)
                                || (separateKeys == SAME_FILE_PAYLOAD && fieldType == PAYLOAD))
                            {
                                std::memcpy(fieldAddrBuffer, fieldAddrPagedVector, fieldSize);
                            }

                            fieldAddrPage += fieldSize;
                            fieldAddrBuffer += fieldSize;
                        }
                    }
                }
                else
                {
                    std::memcpy(payloadBuffer.data() + pageNum * bufferSize, page.getBuffer(), bufferSize);
                }

                if (pageIdx % tenPercentOfNumPages == 0)
                {
                    NES_INFO("File {}% written", std::round(100 * static_cast<double>(pageIdx) / numPages));
                }
            }

            // TODO SAME_FILE_KEYS
            if (separateKeys == SEPARATE_FILES)
            {
                keyFile.write(keyBuffer.data(), tuplesWritten * keySize);
                payloadFile.write(payloadBuffer.data(), tuplesWritten * payloadSize);
            }
            else
            {
                payloadFile.write(payloadBuffer.data(), fileBufferSize);
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
        Nautilus::Interface::PagedVector& pagedVector,
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
            keyFile = std::ifstream(fileNames.at(1), std::ios::in | std::ios::binary);
            ASSERT_TRUE(keyFile);
        }
        else
        {
            ASSERT_NE(separateKeys, SEPARATE_FILES);
        }

        // TODO does this enhance performance?
        pagedVector.appendPageIfFull();
        pagedVector.getPages().reserve(expectedNumPages);

        // TODO remove?
        auto& pages = pagedVector.getPages();
        const auto memoryLayout = pagedVector.getMemoryLayout();
        const auto pageSize = memoryLayout->getBufferSize();
        const auto capacity = memoryLayout->getCapacity();
        ASSERT_EQ(fileBufferSize % pageSize, 0);

        auto pageCnt = 0UL;
        std::vector<char> fileBuffer(fileBufferSize);
        const auto fieldTypeSizes = getFieldTypeSizes(memoryLayout);
        const auto tenPercentOfNumPages = std::max(1UL, (expectedNumPages * 10) / 100);
        const auto [keySize, payloadSize] = getKeyAndPayloadSize(memoryLayout);

        while (payloadFile.read(fileBuffer.data(), fileBufferSize) || payloadFile.gcount() > 0)
        {
            for (auto pageIdx = 0UL; pageIdx < pageSize / fileBufferSize; ++pageIdx)
            {
                pagedVector.appendPageIfFull();
                auto& tupleBuffer = pages.back();
                tupleBuffer.setNumberOfTuples(capacity);

                if (separateKeys != NO_SEPARATION)
                {
                    if (separateKeys == SAME_FILE)
                    {
                        // TODO remove case
                    }
                    else if (separateKeys == SEPARATE_FILES)
                    {
                        for (auto i = 0UL; i < capacity; ++i)
                        {
                            for (auto fieldTypeIdx = 0UL; fieldTypeIdx < fieldTypeSizes.size(); ++fieldTypeIdx)
                            {
                            }
                        }
                    }
                }
                else
                {
                    std::memcpy(tupleBuffer.getBuffer(), fileBuffer.data() + pageIdx * pageSize, pageSize);
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

        timer.snapshot("done writing");
        timer.pause();
        auto timeInMs = timer.getPrintTime();
        execTimes.emplace_back(timeInMs);
        NES_INFO("PagedVector written in {} ms", timeInMs);
    }

    void comparePagedVectors(Nautilus::Interface::PagedVector& expectedPagedVector, Nautilus::Interface::PagedVector& actualPagedVector)
    {
        NES_INFO("PagedVectors comparing...");
        Timer<> timer("compareTimer");
        timer.start();

        const auto expectedNumPages = expectedPagedVector.getNumberOfPages();
        const auto actualNumPages = actualPagedVector.getNumberOfPages();

        ASSERT_EQ(expectedNumPages, actualNumPages);

        const auto& expectedPages = expectedPagedVector.getPages();
        const auto& actualPages = actualPagedVector.getPages();

        const auto tenPercentOfNumPages = std::max(1UL, (expectedNumPages * 10) / 100);
        const auto bufferSize = expectedPagedVector.getMemoryLayout()->getBufferSize();

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
    const auto numBuffers = MEM_CONSUMPTION / pageSize;

    const std::vector<std::string> fileNames = {"../../../benchmark1_payload.dat"};
    const auto testSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT);
    addSchemaFields(testSchema);

    const auto bufferManager = Memory::BufferManager::create(pageSize, numBuffers);
    const auto memoryLayout = Util::createMemoryLayout(testSchema, pageSize);
    memoryLayout->setKeyFieldNames({"f0", "f1"});

    Nautilus::Interface::PagedVector pagedVector(bufferManager, memoryLayout);
    generateData(pagedVector, numBuffers);
    NES_INFO("PagedVector copying...");
    Nautilus::Interface::PagedVector expectedPagedVector = copyPagedVector(pagedVector);
    NES_INFO("PagedVector copied");

    writePagedVectorToFile(pagedVector, fileNames, pageSize, NO_SEPARATION);
    clearPagedVector(pagedVector, NO_SEPARATION);

    writeFileToPagedVector(pagedVector, fileNames, numBuffers, pageSize, NO_SEPARATION);
    comparePagedVectors(expectedPagedVector, pagedVector);

    //createMeasurementsCSV("fileName.csv");
}

}
