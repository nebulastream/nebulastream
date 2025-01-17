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

    static void fillPagedVector(Nautilus::Interface::PagedVector& pagedVector, const uint64_t numBuffers)
    {
        NES_INFO("PagedVector now filling");

        const auto tenPercentOfNumBuffers = std::max(1UL, (numBuffers * 10) / 100);
        const auto bufCapacity = pagedVector.getMemoryLayout()->getCapacity();

        auto tupleCnt = 0UL;
        for (auto bufferIdx = 0UL; bufferIdx < numBuffers; ++bufferIdx)
        {
            pagedVector.appendPageIfFull();
            auto tupleBuffer = pagedVector.getLastPage();
            tupleBuffer.setNumberOfTuples(bufCapacity);
            auto* const tupleBufferAddr = tupleBuffer.getBuffer<uint64_t>();

            for (auto tupleIdx = 0UL; tupleIdx < bufCapacity; ++tupleIdx)
            {
                tupleBufferAddr[tupleIdx] = tupleCnt++;
            }

            if (bufferIdx % tenPercentOfNumBuffers == 0)
            {
                NES_INFO("PagedVector {}% filled", std::round(100 * static_cast<double>(bufferIdx) / numBuffers));
            }
        }

        NES_INFO("PagedVector done filling");
    }

    static void writePagedVectorToFile(const Nautilus::Interface::PagedVector& pagedVector, const std::string& fileName)
    {
        NES_INFO("File now filling");

        std::ofstream outFile(fileName, std::ios::out | std::ios::trunc | std::ios::binary);
        if (!outFile)
        {
            throw std::runtime_error("Failed to open file for writing");
        }

        const auto numPages = pagedVector.getNumberOfPages();
        const auto tenPercentOfNumPages = std::max(1UL, (numPages * 10) / 100);

        for (size_t pageIndex = 0; pageIndex < numPages; ++pageIndex)
        {
            const auto tupleBuffer = pagedVector.getPages()[pageIndex];
            outFile.write(reinterpret_cast<const char*>(tupleBuffer.getBuffer()), tupleBuffer.getBufferSize());

            if (pageIndex % tenPercentOfNumPages == 0)
            {
                NES_INFO("File {}% filled", std::round(100 * static_cast<double>(pageIndex) / numPages));
            }
        }

        outFile.close();

        NES_INFO("File done filling");
    }
};

TEST_F(MicroBenchmarksTest, benchmark1)
{
    const auto testSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT);
    addSchemaFields(testSchema);

    const auto pageSize = 4 * 1024;
    const auto numBuffers = MEM_CONSUMPTION / pageSize;

    const auto bufferManager = Memory::BufferManager::create(pageSize, numBuffers);
    const auto memoryLayout = Util::createMemoryLayout(testSchema, pageSize);
    Nautilus::Interface::PagedVector pagedVector(bufferManager, memoryLayout);

    fillPagedVector(pagedVector, numBuffers);
    writePagedVectorToFile(pagedVector, "../../../benchmark1_output.dat");
}

TEST_F(MicroBenchmarksTest, benchmark2)
{
    const auto testSchema = Schema::create(Schema::MemoryLayoutType::COLUMNAR_LAYOUT);
    addSchemaFields(testSchema);

    const auto pageSize = 4 * 1024;
    const auto numBuffers = MEM_CONSUMPTION / pageSize;

    const auto bufferManager = Memory::BufferManager::create(pageSize, numBuffers);
    const auto memoryLayout = Util::createMemoryLayout(testSchema, pageSize);
    Nautilus::Interface::PagedVector pagedVector(bufferManager, memoryLayout);

    fillPagedVector(pagedVector, numBuffers);
    writePagedVectorToFile(pagedVector, "../../../benchmark2_output.dat");
}

}
