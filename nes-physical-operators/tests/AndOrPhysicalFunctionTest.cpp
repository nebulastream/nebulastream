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


#include <initializer_list>
#include <memory>
#include <unordered_map>
#include <Functions/BooleanFunctions/AndPhysicalFunction.hpp>
#include <Functions/BooleanFunctions/OrPhysicalFunction.hpp>
#include <Functions/FieldAccessPhysicalFunction.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <gtest/gtest.h>
#include <Arena.hpp>
#include <BaseUnitTest.hpp>
#include <val_bool.hpp>

namespace NES
{

class AndOrLogicalFunctionTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite()
    {
        Logger::setupLogging("AndOrLogicalFunctionTest.log", LogLevel::LOG_DEBUG);
        NES_DEBUG("Setup AndOrLogicalFunctionTest test class.");
    }

    void SetUp() override
    {
        BaseUnitTest::SetUp();
        bufferManager = BufferManager::create();

        /// Creating record under test with all four possible combinations of (true/false)x(null/not null)
        constexpr auto isNullable = true;
        const nautilus::val<bool> isNull = true;
        const nautilus::val<bool> isNotNull = false;
        record = Record{std::unordered_map<Record::RecordFieldIdentifier, VarVal>{
            {"true", VarVal{nautilus::val<bool>{true}, isNullable, isNotNull}},
            {"false", VarVal{nautilus::val<bool>{false}, isNullable, isNotNull}},
            {"null", VarVal{nautilus::val<bool>{true}, isNullable, isNull}}}};
    }

    Record record;
    FieldAccessPhysicalFunction readTrue{"true"};
    FieldAccessPhysicalFunction readFalse{"false"};
    FieldAccessPhysicalFunction readNull{"null"};
    std::shared_ptr<AbstractBufferProvider> bufferManager;
};

/// We test here if our AndPhysicalFunction performs and with null correctly.
/// Any expression involving null results in NULL, expect for (NULL and False), as (NULL and False) can be determined without evaluation the NULL value
TEST_F(AndOrLogicalFunctionTest, NullTestAnd)
{
    Arena arena{bufferManager};
    ArenaRef arenaRef{&arena};
    const auto trueAndFalse = AndPhysicalFunction{readTrue, readFalse}.execute(record, arenaRef);
    EXPECT_EQ(trueAndFalse.cast<nautilus::val<bool>>(), false);
    EXPECT_EQ(trueAndFalse.isNull(), false);

    const auto trueAndNull = AndPhysicalFunction{readTrue, readNull}.execute(record, arenaRef);
    EXPECT_EQ(trueAndNull.isNull(), true);

    const auto falseAndNull = AndPhysicalFunction{readFalse, readNull}.execute(record, arenaRef);
    EXPECT_EQ(falseAndNull.cast<nautilus::val<bool>>(), false);
    EXPECT_EQ(falseAndNull.isNull(), false);
}

/// We test here if our OrPhysicalFunction performs or with null correctly.
/// Any expression involving null results in NULL, expect for (NULL or True), as (NULL or True) can be determined without evaluation the NULL value
TEST_F(AndOrLogicalFunctionTest, NullTestOr)
{
    Arena arena{bufferManager};
    ArenaRef arenaRef{&arena};
    const auto trueOrFalse = OrPhysicalFunction{readTrue, readFalse}.execute(record, arenaRef);
    EXPECT_EQ(trueOrFalse.cast<nautilus::val<bool>>(), true);
    EXPECT_EQ(trueOrFalse.isNull(), false);

    const auto trueOrNull = OrPhysicalFunction{readTrue, readNull}.execute(record, arenaRef);
    EXPECT_EQ(trueOrNull.isNull(), false);

    const auto falseOrNull = OrPhysicalFunction{readFalse, readNull}.execute(record, arenaRef);
    EXPECT_EQ(falseOrNull.cast<nautilus::val<bool>>(), false);
    EXPECT_EQ(falseOrNull.isNull(), true);
}
}
