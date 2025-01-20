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

using SeparateKeys = enum { NO_SEPARATION, SAME_FILE, SEPARATE_FILES };

class MicroBenchmarksTest : public Testing::BaseUnitTest
{
public:
    static constexpr uint64_t MEM_CONSUMPTION = 12884901888; /// 12GB //17179869184; /// 16GB
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

        payloadFile = std::ofstream(fileNames.front(), std::ios::out | std::ios::trunc | std::ios::binary);
        ASSERT_TRUE(payloadFile);
        if (fileNames.size() > 1)
        {
            keyFile = std::ofstream(fileNames.at(2), std::ios::out | std::ios::trunc | std::ios::binary);
            ASSERT_TRUE(keyFile);
        }

        // TODO remove?
        const auto& pages = pagedVector.getPages();
        const auto numPages = pagedVector.getNumberOfPages();
        const auto memoryLayout = pagedVector.getMemoryLayout();
        const auto bufferSize = memoryLayout->getBufferSize();

        auto pageCnt = 0UL;
        std::vector<char> fileBuffer(fileBufferSize);
        const auto tenPercentOfNumPages = std::max(1UL, (numPages * 10) / 100);

        for (const auto& page : pages)
        {
            const auto& tupleBuffer = pages[pageIndex];

            if (separateKeys != NO_SEPARATION)
            {
                if (separateKeys == SAME_FILE)
                {
                }
                else if (separateKeys == SEPARATE_FILES)
                {
                }
            }
            else
            {
                payloadFile.write(tupleBuffer.getBuffer<char>(), bufferSize);
            }

            if (pageIndex % tenPercentOfNumPages == 0)
            {
                NES_INFO("File {}% written", std::round(100 * static_cast<double>(pageIndex) / numPages));
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
            keyFile = std::ifstream(fileNames.at(2), std::ios::in | std::ios::binary);
            ASSERT_TRUE(keyFile);
        }

        // TODO does this enhance performance?
        pagedVector.appendPageIfFull();
        pagedVector.getPages().reserve(expectedNumPages);

        // TODO remove?
        auto& pages = pagedVector.getPages();
        const auto memoryLayout = pagedVector.getMemoryLayout();
        const auto pageSize = memoryLayout->getBufferSize();
        ASSERT_EQ(fileBufferSize % pageSize, 0);

        auto pageCnt = 0UL;
        std::vector<char> fileBuffer(fileBufferSize);
        const auto tenPercentOfNumPages = std::max(1UL, (expectedNumPages * 10) / 100);

        while (payloadFile.read(fileBuffer.data(), fileBufferSize) || payloadFile.gcount() > 0)
        {
            for (auto pageIndex = 0UL; pageIndex < pageSize / fileBufferSize; ++pageIndex)
            {
                pagedVector.appendPageIfFull();
                auto& tupleBuffer = pages.back();
                tupleBuffer.setNumberOfTuples(memoryLayout->getCapacity());

                if (separateKeys != NO_SEPARATION)
                {
                    if (separateKeys == SAME_FILE)
                    {
                    }
                    else if (separateKeys == SEPARATE_FILES)
                    {
                    }
                }
                else
                {
                    std::memcpy(tupleBuffer.getBuffer(), fileBuffer.data() + pageIndex * pageSize, pageSize);
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

        for (auto pageIndex = 0UL; pageIndex < expectedNumPages; ++pageIndex)
        {
            const auto* const expectedBufferAddr = expectedPages[pageIndex].getBuffer();
            const auto* const actualBufferAddr = actualPages[pageIndex].getBuffer();

            ASSERT_EQ(std::memcmp(expectedBufferAddr, actualBufferAddr, bufferSize), 0);

            if (pageIndex % tenPercentOfNumPages == 0)
            {
                NES_INFO("PagedVectors {}% compared", std::round(100 * static_cast<double>(pageIndex) / expectedNumPages));
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
