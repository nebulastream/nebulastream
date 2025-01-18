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
#include <BaseUnitTest.hpp>

namespace NES
{

class MicroBenchmarksTest : public Testing::BaseUnitTest
{
public:
    static constexpr uint64_t MEM_CONSUMPTION = 268435456; /// 256MB //2147483648; /// 2GB //17179869184; /// 16GB

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

    static void generateData(Nautilus::Interface::PagedVector& pagedVector, const uint64_t numBuffers)
    {
        NES_INFO("Data generating...");

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

        NES_INFO("Data generated");
    }

    static void clearPagedVector(Nautilus::Interface::PagedVector& pagedVector)
    {
        NES_INFO("PagedVector clearing...");

        pagedVector.getPages().clear();
        ASSERT_EQ(pagedVector.getNumberOfPages(), 0);

        NES_INFO("PagedVector cleared");
    }

    static void writePagedVectorToFile(Nautilus::Interface::PagedVector& pagedVector, const std::string& fileName)
    {
        NES_INFO("File writing...");

        std::ofstream outFile(fileName, std::ios::out | std::ios::trunc | std::ios::binary);
        ASSERT_TRUE(outFile);

        const auto& pages = pagedVector.getPages();
        const auto numPages = pagedVector.getNumberOfPages();
        const auto bufferSize = pagedVector.getMemoryLayout()->getBufferSize();

        const auto tenPercentOfNumPages = std::max(1UL, (numPages * 10) / 100);

        for (auto pageIndex = 0UL; pageIndex < numPages; ++pageIndex)
        {
            const auto& tupleBuffer = pages[pageIndex];
            outFile.write(tupleBuffer.getBuffer<char>(), bufferSize);

            if (pageIndex % tenPercentOfNumPages == 0)
            {
                NES_INFO("File {}% written", std::round(100 * static_cast<double>(pageIndex) / numPages));
            }
        }

        outFile.close();

        NES_INFO("File written");
    }

    static void
    writeFileToPagedVector(Nautilus::Interface::PagedVector& pagedVector, const std::string& fileName, const uint64_t expectedNumPages)
    {
        NES_INFO("PagedVector writing...");

        std::ifstream inFile(fileName, std::ios::in | std::ios::binary);
        ASSERT_TRUE(inFile);

        /// TODO does this enhance performance?
        pagedVector.getPages().reserve(expectedNumPages);

        const auto memoryLayout = pagedVector.getMemoryLayout();
        const auto bufferSize = memoryLayout->getBufferSize();

        auto pageCnt = 0UL;
        std::vector<char> fileBuffer(bufferSize);
        const auto tenPercentOfNumPages = std::max(1UL, (expectedNumPages * 10) / 100);

        while (inFile.read(fileBuffer.data(), bufferSize) || inFile.gcount() > 0)
        {
            pagedVector.appendPageIfFull();
            auto& tupleBuffer = pagedVector.getPages().back();
            tupleBuffer.setNumberOfTuples(memoryLayout->getCapacity());
            std::memcpy(tupleBuffer.getBuffer(), fileBuffer.data(), inFile.gcount());

            if (pageCnt++ % tenPercentOfNumPages == 0)
            {
                NES_INFO("PagedVector {}% written", std::round(100 * static_cast<double>(pageCnt - 1) / expectedNumPages));
            }
        }

        inFile.close();

        NES_INFO("PagedVector written");
    }

    static void
    comparePagedVectors(Nautilus::Interface::PagedVector& expectedPagedVector, Nautilus::Interface::PagedVector& actualPagedVector)
    {
        NES_INFO("PagedVectors comparing...");

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

        NES_INFO("PagedVectors compared");
    }

    /*
    static void compareFileToExpectedPagedVector(Nautilus::Interface::PagedVector& expectedPagedVector, const std::string& fileName)
    {
        NES_INFO("File and PagedVector comparing...");

        std::ifstream inFile(fileName, std::ios::in | std::ios::binary);
        ASSERT_TRUE(inFile);

        const auto pages = expectedPagedVector.getPages();
        const auto numPages = expectedPagedVector.getNumberOfPages();
        const auto tenPercentOfNumPages = std::max(1UL, (numPages * 10) / 100);

        const auto memoryLayout = expectedPagedVector.getMemoryLayout();
        const auto bufferSize = memoryLayout->getBufferSize();
        const auto numFields = memoryLayout->getCapacity() * memoryLayout->getSchema()->getFieldCount();

        auto valueCnt = 0UL;
        std::vector<char> fileBuffer(bufferSize);

        for (auto pageIndex = 0UL; pageIndex < numPages; ++pageIndex)
        {
            const auto& tupleBuffer = pages[pageIndex];
            inFile.read(fileBuffer.data(), bufferSize);

            ASSERT_EQ(inFile.gcount(), static_cast<long>(bufferSize));
            ASSERT_EQ(std::memcmp(tupleBuffer.getBuffer(), fileBuffer.data(), bufferSize), 0);

            for (auto fieldIdx = 0UL; fieldIdx < numFields; ++fieldIdx)
            {
                ASSERT_EQ(reinterpret_cast<uint64_t*>(fileBuffer.data())[fieldIdx], valueCnt++);
            }

            if (pageIndex % tenPercentOfNumPages == 0)
            {
                NES_INFO("File and PagedVector {}% compared", std::round(100 * static_cast<double>(pageIndex) / numPages));
            }
        }

        inFile.close();

        NES_INFO("File and PagedVector compared");
    }
    */
};

TEST_F(MicroBenchmarksTest, benchmark1)
{
    const auto fileName = "../../../benchmark1_output.dat";
    const auto testSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT);
    addSchemaFields(testSchema);

    const auto pageSize = 4 * 1024;
    const auto numBuffers = MEM_CONSUMPTION / pageSize;

    const auto bufferManager = Memory::BufferManager::create(pageSize, numBuffers);
    const auto memoryLayout = Util::createMemoryLayout(testSchema, pageSize);

    Nautilus::Interface::PagedVector pagedVector(bufferManager, memoryLayout);
    generateData(pagedVector, numBuffers);
    NES_INFO("PagedVector copying...");
    Nautilus::Interface::PagedVector expectedPagedVector = copyPagedVector(pagedVector);
    NES_INFO("PagedVector copied");

    writePagedVectorToFile(pagedVector, fileName);
    clearPagedVector(pagedVector);

    writeFileToPagedVector(pagedVector, fileName, expectedPagedVector.getNumberOfPages());
    comparePagedVectors(expectedPagedVector, pagedVector);
}

TEST_F(MicroBenchmarksTest, DISABLED_benchmark2)
{
    const auto fileName = "../../../benchmark2_output.dat";
    const auto testSchema = Schema::create(Schema::MemoryLayoutType::COLUMNAR_LAYOUT);
    addSchemaFields(testSchema);

    const auto pageSize = 4 * 1024;
    const auto numBuffers = MEM_CONSUMPTION / pageSize;

    const auto bufferManager = Memory::BufferManager::create(pageSize, numBuffers);
    const auto memoryLayout = Util::createMemoryLayout(testSchema, pageSize);
    Nautilus::Interface::PagedVector pagedVector(bufferManager, memoryLayout);

    generateData(pagedVector, numBuffers);
    writePagedVectorToFile(pagedVector, fileName);
    //compareFileToExpectedPagedVector(pagedVector, fileName);
}

}
