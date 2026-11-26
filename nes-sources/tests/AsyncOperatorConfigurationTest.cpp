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
#include <DataTypes/DataType.hpp>
#include <DataTypes/RuntimeValue.hpp>
#include <Sources/AsyncInputAccessor.hpp>
#include <Util/Reflection.hpp>
#include <gtest/gtest.h>
#include <AsyncTestOperator.hpp>

namespace NES
{
TEST(AsyncOperatorConfigurationTest, RetainsFixtureStateAndConstantAccessorThroughReflection)
{
    const DataType type{DataType::Type::UINT64, DataType::NULLABLE::IS_NULLABLE};
    const auto accessor = AsyncInputAccessor{RuntimeValue{type, uint64_t{17}}};
    auto native = makeAsyncTestOperator("NativeFixture", {type}, 73, {accessor});
    const ReflectionContext context;
    const auto restored = context.unreflect<AsyncTestOperatorConfig>(context.reflect(*native.getAs<AsyncTestOperatorConfig>()));
    EXPECT_EQ(restored.label, "NativeFixture");
    EXPECT_EQ(restored.marker, 73);
    EXPECT_EQ(restored.accessors.size(), 1);
    EXPECT_EQ(restored.accessors.front().getDataType(), type);
    const auto value = restored.accessors.front()(AsyncRowHandle{});
    EXPECT_EQ(value.view().get<uint64_t>(), 17);
}
}
