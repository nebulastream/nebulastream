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
#include <cstdlib>
#include <cstring>
#include <random>
#include <SliceCache/SliceCache.hpp>
#include <SliceCache/SliceCacheNone.hpp>
#include <SliceCache/SliceCacheSecondChance.hpp>
#include <Util/ExecutionMode.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <gtest/gtest.h>
#include <magic_enum/magic_enum.hpp>

#include <BaseUnitTest.hpp>
#include <Engine.hpp>
#include <ErrorHandling.hpp>
#include <options.hpp>

namespace NES
{

enum class SliceCacheType : uint8_t
{
    NONE,
    SECOND_CHANCE,
};

class SliceCacheTest : public Testing::BaseUnitTest, public testing::WithParamInterface<std::tuple<ExecutionMode, SliceCacheType, uint64_t>>
{
public:
    std::unique_ptr<nautilus::engine::NautilusEngine> nautilusEngine;
    ExecutionMode backend = ExecutionMode::INTERPRETER;
    std::unique_ptr<SliceCache> sliceCache;

    static void SetUpTestSuite()
    {
        Logger::setupLogging("SliceCacheTest.log", LogLevel::LOG_DEBUG);
        NES_INFO("Setup SliceCacheTest class.");
    }

    void SetUp() override
    {
        BaseUnitTest::SetUp();
        backend = std::get<0>(GetParam());
        /// Setting the correct options for the engine, depending on the enum value from the backend
        nautilus::engine::Options options;
        const bool compilation = (backend == ExecutionMode::COMPILER);
        NES_INFO("Backend: {} and compilation: {}", magic_enum::enum_name(backend), compilation);
        options.setOption("engine.Compilation", compilation);
        // options.setOption("mlir.enableMultithreading", mlirEnableMultithreading);
        nautilusEngine = std::make_unique<nautilus::engine::NautilusEngine>(options);

        const SliceCacheType sliceCacheType = std::get<1>(GetParam());
        const uint64_t numberOfEntries = std::get<2>(GetParam());

        switch (sliceCacheType)
        {
            case SliceCacheType::NONE:
                sliceCache = std::make_unique<SliceCacheNone>();
            case SliceCacheType::SECOND_CHANCE:
                sliceCache = std::make_unique<SliceCacheSecondChance>(numberOfEntries, sizeof(SliceCacheEntrySecondChance));
        }
    }

    static void TearDownTestSuite() { NES_INFO("Tear down SliceCacheTest class."); }
};

TEST_P(SliceCacheTest, getDataStructureRef)
{
}

INSTANTIATE_TEST_CASE_P(
    SliceCacheTest,
    SliceCacheTest,
    ::testing::Combine(
        ::testing::Values(ExecutionMode::INTERPRETER, ExecutionMode::COMPILER),
        ::testing::Values(SliceCacheType::NONE, SliceCacheType::SECOND_CHANCE),
        ::testing::Values(1, 5, 10, 15)),
    [](const testing::TestParamInfo<SliceCacheTest::ParamType>& info)
    {
        std::stringstream ss;
        ss << magic_enum::enum_name(std::get<0>(info.param));
        ss << magic_enum::enum_name(std::get<1>(info.param));
        ss << std::get<2>(info.param);
        return ss.str();
    });
}
