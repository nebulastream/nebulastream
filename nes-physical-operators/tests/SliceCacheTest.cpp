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

#include <cstdint>
#include <random>
#include <sstream>
#include <vector>
#include <SliceCache/SliceCache.hpp>
#include <SliceCache/SliceCacheNone.hpp>
#include <SliceCache/SliceCacheSecondChance.hpp>
#include <Util/ExecutionMode.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <gtest/gtest.h>
#include <magic_enum/magic_enum.hpp>
#include <SliceStore/SliceAssigner.hpp>
#include <BaseUnitTest.hpp>
#include <Engine.hpp>
#include <ErrorHandling.hpp>
#include <options.hpp>

namespace NES
{


class SliceCacheTest : public Testing::BaseUnitTest,
                       public testing::WithParamInterface<std::tuple<ExecutionMode, bool, uint64_t, uint64_t>>
{
public:
    struct SliceCacheTestOperation
    {
        uint64_t timestamp;
        SliceStart sliceStart;
        SliceEnd sliceEnd;
        int8_t* expectedResult; /// Points to expectedResultHelper
        std::unique_ptr<uint64_t> expectedResultHelper;
    };

    static constexpr bool mlirEnableMultithreading = false;
    std::unique_ptr<nautilus::engine::NautilusEngine> nautilusEngine;
    ExecutionMode backend = ExecutionMode::INTERPRETER;
    std::unique_ptr<SliceCache> sliceCache;
    bool useSliceCache;
    uint64_t numberOfEntries;
    uint64_t sliceCacheSize;
    std::vector<SliceCacheTestOperation> operations;

    static void SetUpTestSuite()
    {
        Logger::setupLogging("SliceCacheTest.log", LogLevel::LOG_DEBUG);
        NES_INFO("Setup SliceCacheTest class.");
    }

    void SetUp() override
    {
        BaseUnitTest::SetUp();
        backend = std::get<0>(GetParam());
        useSliceCache = std::get<1>(GetParam());
        numberOfEntries = std::get<2>(GetParam());
        sliceCacheSize = std::get<3>(GetParam());

        /// Setting the correct options for the engine, depending on the enum value from the backend
        nautilus::engine::Options options;
        const bool compilation = (backend == ExecutionMode::COMPILER);
        NES_INFO("Backend: {} and compilation: {}", magic_enum::enum_name(backend), compilation);
        options.setOption("engine.Compilation", compilation);
        options.setOption("mlir.enableMultithreading", mlirEnableMultithreading);
        nautilusEngine = std::make_unique<nautilus::engine::NautilusEngine>(options);

        if (useSliceCache)
        {
            sliceCache = std::make_unique<SliceCacheSecondChance>(numberOfEntries, sizeof(SliceCacheEntrySecondChance));
        }
        else
        {
            sliceCache = std::make_unique<SliceCacheNone>();
        }


    }

    void createRandomSliceCacheTestOperation()
    {
        SliceAssigner sliceAssigner(sliceCacheSize, sliceCacheSize);
        // todo create here random SliceCacheTestOperation so that we can use them in each test
    }

    static void TearDownTestSuite() { NES_INFO("Tear down SliceCacheTest class."); }
};

INSTANTIATE_TEST_CASE_P(
    SliceCacheTest,
    SliceCacheTest,
    ::testing::Combine(
        ::testing::Values(ExecutionMode::INTERPRETER, ExecutionMode::COMPILER), /// Nautilus execution backend
        ::testing::Values(false, true), /// Testing if SliceCache and NONE work as expected
        ::testing::Values(1, 5, 10, 15), /// Number of cache entries
        ::testing::Values(1, 10, 100, 1000) /// Size of slice
        ),
    [](const testing::TestParamInfo<SliceCacheTest::ParamType>& info)
    {
        std::stringstream ss;
        // todo add code here
        return ss.str();
    });
}
