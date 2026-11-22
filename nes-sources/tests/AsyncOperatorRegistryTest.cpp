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
/// NOLINTBEGIN(readability-magic-numbers) Numeric values are fixture data in these tests.
#include <atomic>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <gtest/gtest.h>
#include <AsyncOperatorRegistry.hpp>
#include <AsyncTestOperator.hpp>
#include <ErrorHandling.hpp>

namespace NES
{
TEST(AsyncOperatorRegistryTest, FactoryReceivesNativeArgumentTypesExactlyOnce)
{
    static std::atomic_size_t calls{0};
    const bool registered = AsyncOperatorRegistry::instance().addEntry(
        "NATIVE_TEST_FACTORY",
        [](AsyncOperatorRegistryArguments arguments)
        {
            ++calls;
            return AsyncTestOperatorConfig{.label = "NATIVE_TEST_FACTORY", .resultTypes = arguments.argumentTypes, .marker = 73};
        });
    ASSERT_TRUE(registered);
    const DataType type{DataType::Type::UINT64, DataType::NULLABLE::IS_NULLABLE};
    auto config = createAsyncOperator("NATIVE_TEST_FACTORY", {type});
    EXPECT_EQ(calls.load(), 1);
    const auto concrete = config.getAs<AsyncTestOperatorConfig>();
    EXPECT_EQ(concrete->resultTypes, std::vector<DataType>{type});
    EXPECT_EQ(concrete->marker, 73);
}

TEST(AsyncOperatorRegistryTest, UnknownFactoryNameIsRejected)
{
    EXPECT_THROW(static_cast<void>(createAsyncOperator("NO_SUCH_NATIVE_OPERATOR", {})), Exception);
}
}
/// NOLINTEND(readability-magic-numbers)
