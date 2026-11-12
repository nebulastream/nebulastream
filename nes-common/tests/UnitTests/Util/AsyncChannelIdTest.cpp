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
#include <string>
#include <type_traits>
#include <unordered_set>
#include <Identifiers/Identifiers.hpp>
#include <Identifiers/NESStrongTypeFormat.hpp>
#include <Util/Reflection.hpp>
#include <Util/UUID.hpp>
#include <fmt/format.h>
#include <gtest/gtest.h>
#include <rfl/json/write.hpp>
#include <ErrorHandling.hpp>

namespace NES
{
static_assert(!std::is_default_constructible_v<AsyncChannelId>);
static_assert(!std::is_convertible_v<std::string, AsyncChannelId>);
static_assert(!std::is_convertible_v<UUID, AsyncChannelId>);
static_assert(!std::is_convertible_v<AsyncChannelId, std::string>);
static_assert(!std::is_convertible_v<LocalQueryId, AsyncChannelId>);

TEST(AsyncChannelIdTest, BinaryAndTextConstructionPreserveUuidIdentityAndHashing)
{
    const AsyncChannelId original{generateUUID()};
    const AsyncChannelId parsed{original.getRawValue()};
    EXPECT_EQ(parsed, original);
    EXPECT_EQ(parsed.view(), original.view());
    const std::unordered_set<AsyncChannelId> ids{original, parsed};
    EXPECT_EQ(ids.size(), 1);
}

TEST(AsyncChannelIdTest, UuidSpellingIsCanonicalAndReflectionRemainsAString)
{
    const std::string lower{"aabbccdd-1122-3344-5566-778899aabbcc"};
    const AsyncChannelId id{std::string{"AABBCCDD-1122-3344-5566-778899AABBCC"}};
    EXPECT_EQ(id.getRawValue(), lower);
    EXPECT_EQ(fmt::format("{}", id), lower);
    const ReflectionContext context;
    const auto reflected = context.reflect(id);
    EXPECT_EQ(rfl::json::write(*reflected), "\"" + lower + "\"");
    EXPECT_EQ(context.unreflect<AsyncChannelId>(reflected), id);
}

TEST(AsyncChannelIdTest, InvalidTextIsRejectedAtTheBoundary)
{
    EXPECT_THROW((AsyncChannelId{std::string{"not-a-uuid"}}), Exception);
    const ReflectionContext context;
    EXPECT_THROW(static_cast<void>(context.unreflect<AsyncChannelId>(context.reflect(std::string{"not-a-uuid"}))), Exception);
}

}
