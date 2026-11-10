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

#include <any>
#include <string>
#include <string_view>
#include <type_traits>
#include <Identifiers/NESStrongTypeFormat.hpp>
#include <Plugins/PluginDescriptor.hpp>
#include <Sources/AsyncFunctionType.hpp>
#include <Util/Reflection.hpp>
#include <Util/RuntimeRegistry.hpp>
#include <fmt/format.h>
#include <gtest/gtest.h>
#include <rfl/Generic.hpp>
#include <rfl/json/read.hpp>
#include <rfl/json/write.hpp>
#include <AsyncFunctionRegistry.hpp>

namespace NES
{
namespace
{
class TypedFunctionRegistry final : public RuntimeRegistry<TypedFunctionRegistry, AsyncFunctionType, int, false>
{
};

class StringFunctionRegistry final : public RuntimeRegistry<StringFunctionRegistry, std::string, int, false>
{
};
}

static_assert(std::is_same_v<AsyncFunctionRegistry::KeyType, AsyncFunctionType>);
static_assert(!std::is_default_constructible_v<AsyncFunctionType>);
static_assert(!std::is_convertible_v<std::string, AsyncFunctionType>);
static_assert(!std::is_convertible_v<AsyncFunctionType, std::string>);
static_assert(!std::is_convertible_v<AsyncFunctionType, std::string_view>);

TEST(AsyncFunctionTypeTest, UsesExistingStrongStringFormattingAndReflection)
{
    const AsyncFunctionType type{"HttpCall"};
    EXPECT_EQ(fmt::format("{}", type), "HttpCall");
    const ReflectionContext context;
    const auto json = rfl::json::write(*context.reflect(type));
    EXPECT_EQ(json, "\"HttpCall\"");
    const auto parsed = rfl::json::read<rfl::Generic>(json);
    ASSERT_TRUE(parsed);
    EXPECT_EQ(context.unreflect<AsyncFunctionType>(*parsed), type);
}

TEST(AsyncFunctionTypeTest, StrongRegistryKeysKeepCaseInsensitiveLookupAndDuplicateDetection)
{
    auto& registry = TypedFunctionRegistry::instance();
    EXPECT_TRUE(registry.addEntry(AsyncFunctionType{"HttpCall"}, 42));
    EXPECT_TRUE(registry.contains(AsyncFunctionType{"httpcall"}));
    EXPECT_EQ(registry.find(AsyncFunctionType{"HTTPCALL"}), 42);
    EXPECT_FALSE(registry.addEntry(AsyncFunctionType{"hTtPcAlL"}, 99));
    EXPECT_EQ(registry.find(AsyncFunctionType{"HttpCall"}), 42);
}

TEST(AsyncFunctionTypeTest, PluginProvisionConstructsStrongKeysAtTheStringTransportBoundary)
{
    const auto provision = provideRegistry<TypedFunctionRegistry>("TypedFunctionRegistry");
    EXPECT_EQ(provision.insertEntry("AsyncDelay", std::any{7}), InsertResult::Success);
    EXPECT_EQ(TypedFunctionRegistry::instance().find(AsyncFunctionType{"asyncdelay"}), 7);
    EXPECT_EQ(provision.insertEntry("ASYNCDELAY", std::any{8}), InsertResult::DuplicateKey);
    EXPECT_EQ(provision.insertEntry("AnotherType", std::any{std::string{"wrong entry type"}}), InsertResult::EntryTypeMismatch);
    EXPECT_FALSE(TypedFunctionRegistry::instance().contains(AsyncFunctionType{"AnotherType"}));
}

TEST(AsyncFunctionTypeTest, StringKeyedRegistryProvisionsRemainUnchanged)
{
    const auto provision = provideRegistry<StringFunctionRegistry>("StringFunctionRegistry");
    EXPECT_EQ(provision.insertEntry("MixedCase", std::any{9}), InsertResult::Success);
    EXPECT_EQ(StringFunctionRegistry::instance().find("mixedcase"), 9);
    EXPECT_EQ(provision.insertEntry("MIXEDCASE", std::any{8}), InsertResult::DuplicateKey);
}

}
