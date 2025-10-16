#include "Identifiers/Identifier.hpp"


#include "Util/Logger/Logger.hpp"


#include "BaseUnitTest.hpp"
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
namespace NES
{
///NOLINTBEGIN(bugprone-unchecked-optional-access)
class IdentifierTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestCase()
    {
        Logger::setupLogging("IdentifierTest.log", LogLevel::LOG_DEBUG);
        NES_INFO("IdentifierTest test class SetUpTestCase.");
    }
};

TEST_F(IdentifierTest, AutoUpperCase)
{
    const auto identifier = Identifier::parse("test");
    EXPECT_EQ(fmt::format("{}", identifier), "TEST");
    EXPECT_FALSE(identifier.isCaseSensitive());
    EXPECT_EQ(identifier.getOriginalString(), "test");
}

TEST_F(IdentifierTest, CaseSensitive)
{
    const auto identifier = Identifier::parse("\"Test\"");
    EXPECT_EQ(fmt::format("{}", identifier), "Test");
    EXPECT_TRUE(identifier.isCaseSensitive());
    EXPECT_EQ(identifier.getOriginalString(), "\"Test\"");
}

TEST_F(IdentifierTest, CaseSensitiveCAPS)
{
    const auto identifier = Identifier::parse("\"TEST\"");
    EXPECT_EQ(fmt::format("{}", identifier), "TEST");
    EXPECT_TRUE(identifier.isCaseSensitive());
    EXPECT_EQ(identifier.getOriginalString(), "\"TEST\"");
}

TEST_F(IdentifierTest, Equality)
{
    const auto identifier1 = Identifier::parse("test");
    const auto identifier2 = Identifier::parse("\"TEST\"");
    EXPECT_EQ(identifier1, identifier2);
    EXPECT_EQ(identifier2, identifier1);
}

TEST_F(IdentifierTest, Inequality)
{
    const auto identifier1 = Identifier::parse("test");
    const auto identifier2 = Identifier::parse("\"Test\"");
    EXPECT_NE(identifier1, identifier2);
    EXPECT_NE(identifier2, identifier1);
}

TEST_F(IdentifierTest, NonOwning)
{
    /// You should not rely on this behaviour, this is just to test that the identifier really does not make another copy
    std::string testString = "test";
    const auto identifier = IdentifierBase<false>::parse(testString);
    EXPECT_EQ(fmt::format("{}", identifier), "TEST");
    EXPECT_FALSE(identifier.isCaseSensitive());
    /// It will throw because the non owning identifier uses a string_view that while pointing to the string, has a constant size, so will cut of after the 's'
    /// Again, DO NOT RELY ON THIS BEHAVIOUR
    testString = "\"Test\"";
    EXPECT_THROW([&]{ return identifier.isCaseSensitive();}(), Exception);
    testString = "\"Es\"";
    EXPECT_EQ(fmt::format("{}", identifier), "Es");
    EXPECT_TRUE(identifier.isCaseSensitive());

}
}
