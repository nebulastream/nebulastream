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
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>
#include <val_arith.hpp>
#include <val_bool.hpp>

namespace NES
{

class StreamJoinProbePhysicalOperatorTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite()
    {
        Logger::setupLogging("StreamJoinProbePhysicalOperatorTest.log", LogLevel::LOG_DEBUG);
        NES_DEBUG("Setup StreamJoinProbePhysicalOperatorTest class.");
    }
};

/// Verify typed null VarVal construction for all DataType::Type values used in join schemas
TEST_F(StreamJoinProbePhysicalOperatorTest, nullVarValConstructionCoversAllTypes)
{
    const auto nullFlag = nautilus::val<bool>{true};

    const VarVal nullBool(nautilus::val<bool>{false}, true, nullFlag);
    const VarVal nullInt8(nautilus::val<int8_t>{0}, true, nullFlag);
    const VarVal nullInt16(nautilus::val<int16_t>{0}, true, nullFlag);
    const VarVal nullInt32(nautilus::val<int32_t>{0}, true, nullFlag);
    const VarVal nullInt64(nautilus::val<int64_t>{0}, true, nullFlag);
    const VarVal nullUint8(nautilus::val<uint8_t>{0}, true, nullFlag);
    const VarVal nullUint16(nautilus::val<uint16_t>{0}, true, nullFlag);
    const VarVal nullUint32(nautilus::val<uint32_t>{0}, true, nullFlag);
    const VarVal nullUint64(nautilus::val<uint64_t>{0}, true, nullFlag);
    const VarVal nullFloat32(nautilus::val<float>{0.0F}, true, nullFlag);
    const VarVal nullFloat64(nautilus::val<double>{0.0}, true, nullFlag);
    const VarVal nullChar(nautilus::val<char>{0}, true, nullFlag);

    EXPECT_TRUE(nullBool.isNullable());
    EXPECT_TRUE(nullInt64.isNullable());
    EXPECT_TRUE(nullUint64.isNullable());
    EXPECT_TRUE(nullFloat64.isNullable());
    EXPECT_TRUE(nullChar.isNullable());
}

} /// namespace NES
