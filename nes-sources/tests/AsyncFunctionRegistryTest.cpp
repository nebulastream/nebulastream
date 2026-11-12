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
#include <type_traits>
#include <Plugins/PluginDescriptor.hpp>
#include <Util/RuntimeRegistry.hpp>
#include <gtest/gtest.h>
#include <AsyncFunctionRegistry.hpp>

namespace NES
{
namespace
{
class StringFunctionRegistry final : public RuntimeRegistry<StringFunctionRegistry, std::string, int, false>
{
};
}

static_assert(std::is_same_v<AsyncFunctionRegistry::KeyType, std::string>);

TEST(AsyncFunctionRegistryTest, StringKeysKeepCaseInsensitiveLookupAndDuplicateDetection)
{
    auto& registry = StringFunctionRegistry::instance();
    EXPECT_TRUE(registry.addEntry("HttpCall", 42));
    EXPECT_TRUE(registry.contains("httpcall"));
    EXPECT_EQ(registry.find("HTTPCALL"), 42);
    EXPECT_FALSE(registry.addEntry("hTtPcAlL", 99));
    EXPECT_EQ(registry.find("HttpCall"), 42);
}

TEST(AsyncFunctionRegistryTest, PluginProvisionUsesStringKeys)
{
    const auto provision = provideRegistry<StringFunctionRegistry>("StringFunctionRegistry");
    EXPECT_EQ(provision.insertEntry("AsyncDelay", std::any{7}), InsertResult::Success);
    EXPECT_EQ(StringFunctionRegistry::instance().find("asyncdelay"), 7);
    EXPECT_EQ(provision.insertEntry("ASYNCDELAY", std::any{8}), InsertResult::DuplicateKey);
    EXPECT_EQ(provision.insertEntry("AnotherType", std::any{std::string{"wrong entry type"}}), InsertResult::EntryTypeMismatch);
    EXPECT_FALSE(StringFunctionRegistry::instance().contains("AnotherType"));
}

}
