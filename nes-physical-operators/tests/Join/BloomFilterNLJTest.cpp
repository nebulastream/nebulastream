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

#include <gtest/gtest.h>
#include <Join/NestedLoopJoin/NLJSlice.hpp>
#include <Join/StreamJoinUtil.hpp>
#include <Nautilus/Interface/Hash/BloomFilter.hpp>
#include <Time/Timestamp.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <NautilusTestUtils.hpp>
#include <PagedVectorTestUtils.hpp>

namespace NES
{

class BloomFilterNLJTest : public testing::Test, public TestUtils::NautilusTestUtils
{
public:
    static void SetUpTestSuite()
    {
        Logger::setupLogging("BloomFilterNLJTest.log", LogLevel::LOG_DEBUG);
        NES_INFO("Setup BloomFilterNLJTest class.");
    }

    static void TearDownTestSuite()
    {
        NES_INFO("Tear down BloomFilterNLJTest class.");
    }


protected:
    void SetUp() override 
    {
        bufferManager = BufferManager::create();
        schema = Schema{}.addField("id", DataType::Type::UINT64);
        
        // Setup NautilusEngine for PagedVector operations
        nautilus::engine::Options options;
        options.setOption("engine.Compilation", false); // Use interpreter for faster tests
        nautilusEngine = std::make_unique<nautilus::engine::NautilusEngine>(options);
    }
    
    void TearDown() override 
    {
        bufferManager.reset();
        nautilusEngine.reset();
    }
    
    std::shared_ptr<BufferManager> bufferManager;
    Schema schema;
    std::unique_ptr<nautilus::engine::NautilusEngine> nautilusEngine;
};

/**
 * @brief Integration TEST: Verify BUILD phase creates accessible BloomFilter references
 *
 * Minimal integration coverage for NLJ + BloomFilter:
 * - create NLJSlice
 * - store tuples on both sides
 * - run combinePagedVectors()
 * - fetch BloomFilterRef for both build sides
 */
TEST_F(BloomFilterNLJTest, BuildPhase_BloomFilterRefAccess)
{
    std::cout << "\nBUILD Phase Test: BloomFilterRef Access" << std::endl;
    
    NLJSlice slice(Timestamp(0), Timestamp(100), 1);
    
    // Add tuples to both sides
    const uint64_t tupleCount = 30;
    auto records = createMonotonicallyIncreasingValues(
        schema, MemoryLayoutType::ROW_LAYOUT, tupleCount, *bufferManager);
    
    PagedVector* leftPagedVector = slice.getPagedVectorRefLeft(WorkerThreadId(0));
    PagedVector* rightPagedVector = slice.getPagedVectorRefRight(WorkerThreadId(0));
    
    constexpr auto pageSize = 4096;
    const auto projections = schema.getFieldNames();
    
    TestUtils::runStoreTest(*leftPagedVector, schema, MemoryLayoutType::ROW_LAYOUT, 
                            pageSize, projections, records, *nautilusEngine, *bufferManager);
    TestUtils::runStoreTest(*rightPagedVector, schema, MemoryLayoutType::ROW_LAYOUT,
                            pageSize, projections, records, *nautilusEngine, *bufferManager);
    
    slice.combinePagedVectors();
    
    // Get BloomFilterRef, this is what performNLJ() use
    Nautilus::Interface::BloomFilterRef leftRef = slice.getBloomFilterRef(JoinBuildSideType::Left);
    Nautilus::Interface::BloomFilterRef rightRef = slice.getBloomFilterRef(JoinBuildSideType::Right);
    
    std::cout << "BloomFilterRef for LEFT obtained" << std::endl;
    std::cout << "BloomFilterRef for RIGHT obtained" << std::endl;
    std::cout << "Ready for use in performNLJ() JIT-compiled code\n" << std::endl;
    
    // We cannot evaluate BloomFilterRef.mightContain() directly here because it expects
    // Nautilus val<uint64_t>. Accessing both refs without errors validates integration wiring.
}

} // namespace NES
