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

#include <cmath>
#include <cstdint>
#include <cstring>
#include <memory>
#include <sstream>
#include <vector>
#include <API/Schema.hpp>
#include <MemoryLayout/ColumnLayout.hpp>
#include <MemoryLayout/RowLayout.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/Interface/PagedVector/PagedVector.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/Common.hpp>
#include <Util/Core.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <Util/TestTupleBuffer.hpp>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>
#include <Common/DataTypes/BasicTypes.hpp>

namespace NES
{

class MicroBenchmarksTest : public Testing::BaseUnitTest
{
public:
    static constexpr uint64_t MEM_CONSUMPTION = 1048576; /// 256KB //8388608; /// 2MB //268435456; /// 256MB //2147483648; /// 2GB //17179869184; /// 16GB

    static void SetUpTestSuite()
    {
        Logger::setupLogging("MicroBenchmarksTest.log", LogLevel::LOG_DEBUG);
        NES_INFO("Setup MicroBenchmarksTest class.");
    }

    static void TearDownTestSuite() { NES_INFO("Tear down MicroBenchmarksTest class."); }

    static void fillPagedVector(Nautilus::Interface::PagedVector pagedVector, const uint64_t numTuples)
    {
        const auto memoryLayout = pagedVector.getMemoryLayout();
        for (auto tupleIdx = 0UL; tupleIdx < numTuples; ++tupleIdx)
        {
            pagedVector.appendPageIfFull();
            auto tupleBuffer = pagedVector.getLastPage();
            auto numTuplesOnPage = tupleBuffer.getNumberOfTuples();

            auto fieldCnt = 0UL;
            for (size_t fieldIdx = 0; fieldIdx < memoryLayout->getSchema()->getFieldNames().size(); ++fieldIdx)
            {
                const auto entryVal = (memoryLayout->getSchema()->getSchemaSizeInBytes() * tupleIdx) + fieldCnt++;
                int8_t* fieldAddr;

                if (Util::instanceOf<Memory::MemoryLayouts::RowLayout>(memoryLayout))
                {
                    auto rowMemoryLayoutPtr = Util::as<Memory::MemoryLayouts::RowLayout>(memoryLayout);

                    const auto rowOffset = tupleBuffer.getBuffer() + (rowMemoryLayoutPtr->getTupleSize() * numTuplesOnPage);
                    const auto fieldOffset = rowMemoryLayoutPtr->getFieldOffSets()[fieldIdx];

                    fieldAddr = rowOffset + fieldOffset;
                }
                else if (Util::instanceOf<Memory::MemoryLayouts::ColumnLayout>(memoryLayout))
                {
                    auto columnMemoryLayoutPtr = Util::as<Memory::MemoryLayouts::ColumnLayout>(memoryLayout);

                    const auto columnOffset = columnMemoryLayoutPtr->getColumnOffsets()[fieldIdx];
                    const auto fieldOffset = tupleBuffer.getBuffer() + (columnMemoryLayoutPtr->getFieldSizes()[fieldIdx] * numTuplesOnPage);

                    fieldAddr = columnOffset + fieldOffset;
                }
                else
                {
                    NES_THROW_RUNTIME_ERROR("Unknown memory layout");
                }

                std::memcpy(fieldAddr, std::addressof(entryVal), sizeof(uint64_t));
                if (auto percentageFilled = static_cast<uint64_t>(100 * static_cast<double>(tupleIdx) / numTuples);
                    percentageFilled % 10 == 0)
                {
                    NES_INFO("PagedVector {}% filled", percentageFilled);
                }
            }

            tupleBuffer.setNumberOfTuples(numTuplesOnPage + 1);
        }
    }
    /*
    static void runStoreTest(
        PagedVector& pagedVector,
        const std::shared_ptr<MemoryProvider::TupleBufferMemoryProvider>& memoryProvider,
        const std::vector<Record>& allRecords)
    {
        const uint64_t expectedNumberOfEntries = allRecords.size();
        const uint64_t capacityPerPage = memoryProvider->getMemoryLayoutPtr()->getCapacity();
        const uint64_t numberOfPages = std::ceil(static_cast<double>(expectedNumberOfEntries) / capacityPerPage);
        auto pagedVectorRef = PagedVectorRef(nautilus::val<PagedVector*>(&pagedVector), memoryProvider);

        for (const auto& record : allRecords)
        {
            pagedVectorRef.writeRecord(record);
        }

        ASSERT_EQ(pagedVector.getTotalNumberOfEntries(), expectedNumberOfEntries);
        ASSERT_EQ(pagedVector.getNumberOfPages(), numberOfPages);

        /// As we do lazy allocation, we do not create a new page if the last tuple fit on the page
        const bool lastTupleFitsOntoLastPage = (expectedNumberOfEntries % capacityPerPage) == 0;
        const uint64_t numTuplesLastPage = lastTupleFitsOntoLastPage ? capacityPerPage : (expectedNumberOfEntries % capacityPerPage);
        ASSERT_EQ(pagedVector.getLastPage().getNumberOfTuples(), numTuplesLastPage);
    }

    static void runRetrieveTest(
        PagedVector& pagedVector,
        const std::shared_ptr<MemoryProvider::TupleBufferMemoryProvider>& memoryProvider,
        const std::vector<Record>& allRecords)
    {
        auto pagedVectorRef = PagedVectorRef(nautilus::val<PagedVector*>(&pagedVector), memoryProvider);
        ASSERT_EQ(pagedVector.getTotalNumberOfEntries(), allRecords.size());

        auto itemPos = 0UL;
        const auto projections = memoryProvider->getMemoryLayoutPtr()->getSchema()->getFieldNames();
        for (auto it = pagedVectorRef.begin(projections); it != pagedVectorRef.end(projections); ++it)
        {
            ASSERT_EQ(*it, allRecords[itemPos++]);
        }
        ASSERT_EQ(itemPos, allRecords.size());
    }*/
};

TEST_F(MicroBenchmarksTest, benchmark1)
{
    const auto testSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
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

    const auto pageSize = 4 * 1024;
    const auto numBuffers = MEM_CONSUMPTION / pageSize;

    const auto bufferManager = Memory::BufferManager::create(pageSize, numBuffers);
    const auto memoryLayout = Util::createMemoryLayout(testSchema, pageSize);
    const Nautilus::Interface::PagedVector pagedVector(bufferManager, memoryLayout);

    const auto numItems = memoryLayout->getTupleSize() * memoryLayout->getCapacity() * numBuffers;
    fillPagedVector(pagedVector, numItems);
}

}
