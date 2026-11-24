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
#include <memory>
#include <utility>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <DataTypes/RuntimeValue.hpp>
#include <Sources/AsyncInputAccessor.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Reflection.hpp>
#include <gtest/gtest.h>
#include <ErrorHandling.hpp>

namespace NES
{
namespace
{
const DataType INTEGER{DataType::Type::INT64, DataType::NULLABLE::NOT_NULLABLE};
const DataType NULLABLE{DataType::Type::INT64, DataType::NULLABLE::IS_NULLABLE};

int64_t readInteger(const AsyncInputAccessor& accessor, const AsyncRowHandle& row)
{
    const auto value = accessor(row);
    return value.view().get<int64_t>();
}
}

TEST(AsyncInputAccessorTest, CallableProviderDoesNotRequireTupleBuffer)
{
    const AsyncInputAccessors inputs{{INTEGER, NULLABLE}};
    const auto row = inputs.createRowHandle(
        [](size_t index) -> RuntimeValue
        {
            if (index == 0)
            {
                return RuntimeValue{int64_t{17}};
            }
            return RuntimeValue::null(NULLABLE);
        });
    EXPECT_EQ(readInteger(inputs[0], row), 17);
    EXPECT_TRUE(inputs[1](row).isNull());
    const auto values = inputs.read(row);
    EXPECT_EQ(values[0].get<int64_t>(), 17);
}

TEST(AsyncInputAccessorTest, CopiesRetainTheContextAndRowsRetainTheirProvider)
{
    const auto [accessor, row] = []
    {
        const AsyncInputAccessors original{{INTEGER}};
        const auto copied = original;
        const auto owner = std::make_shared<int64_t>(23);
        return std::pair{original[0], copied.createRowHandle([owner](size_t) { return RuntimeValue{*owner}; })};
    }();
    EXPECT_EQ(readInteger(accessor, row), 23);
    EXPECT_EQ(accessor.getDataType(), INTEGER);
}

#ifndef NO_ASSERT
TEST(AsyncInputAccessorTest, RejectsRowsFromAnotherInputContext)
{
    Logger::setupLogging("AsyncInputAccessorContextTest.log", LogLevel::LOG_DEBUG, false);
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    const AsyncInputAccessors first{{INTEGER}};
    const AsyncInputAccessors second{{INTEGER}};
    const auto row = second.createRowHandle([](size_t) -> RuntimeValue { return RuntimeValue{int64_t{1}}; });
    EXPECT_DEATH(static_cast<void>(first[0](row)), "Precondition violated");
}
#endif

TEST(AsyncInputAccessorTest, TypedNullAndReflectionPreserveACoherentFreshContext)
{
    const AsyncInputAccessors inputs{{NULLABLE}};
    const auto nullValue = RuntimeValue::null(NULLABLE);
    const auto row = inputs.createRowHandle([nullValue](size_t) { return nullValue; });
    EXPECT_EQ(inputs[0](row).getDataType(), NULLABLE);
    EXPECT_TRUE(inputs[0](row).isNull());
    const auto constantRow = inputs.createRowHandle(SharedRuntimeValues::make(nullValue));
    EXPECT_TRUE(inputs[0](constantRow).isNull());
    EXPECT_EQ(inputs[0](constantRow).getDataType(), NULLABLE);

    const ReflectionContext reflection;
    const auto restored = reflection.unreflect<AsyncInputAccessors>(reflection.reflect(inputs));
    EXPECT_EQ(restored.getTypes(), inputs.getTypes());
    const auto restoredRow = restored.createRowHandle([](size_t) { return RuntimeValue::null(NULLABLE); });
    EXPECT_TRUE(restored[0](restoredRow).isNull());
    EXPECT_EQ(restored[0](restoredRow).getDataType(), NULLABLE);
#ifndef NO_ASSERT
    Logger::setupLogging("AsyncInputAccessorReflectionContextTest.log", LogLevel::LOG_DEBUG, false);
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    EXPECT_DEATH(static_cast<void>(inputs[0](restoredRow)), "Precondition violated");
#endif
}

TEST(AsyncInputAccessorTest, RepeatedRowsResolveTheirOwnValues)
{
    const AsyncInputAccessors inputs{{INTEGER}};
    const auto makeRow = [&inputs](int64_t value) { return inputs.createRowHandle([value](size_t) { return RuntimeValue{value}; }); };
    const auto first = makeRow(31);
    const auto second = makeRow(47);
    EXPECT_EQ(readInteger(inputs[0], first), 31);
    EXPECT_EQ(readInteger(inputs[0], second), 47);
    EXPECT_EQ(readInteger(inputs[0], first), 31);
}
}
