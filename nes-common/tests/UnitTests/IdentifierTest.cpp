#include "Identifiers/Identifier.hpp"
#include <functional>
#include <ranges>
#include <span>
#include <unordered_set>


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
    constexpr auto identifier = Identifier::parse("test");
    EXPECT_EQ(fmt::format("{}", identifier), "TEST");
    EXPECT_FALSE(identifier.isCaseSensitive());
    EXPECT_EQ(identifier.getOriginalString(), "test");
}

TEST_F(IdentifierTest, CaseSensitive)
{
    constexpr auto identifier = Identifier::parse("\"Test\"");
    EXPECT_EQ(fmt::format("{}", identifier), "Test");
    EXPECT_TRUE(identifier.isCaseSensitive());
    EXPECT_EQ(identifier.getOriginalString(), "\"Test\"");
}

TEST_F(IdentifierTest, CaseSensitiveCAPS)
{
    constexpr auto identifier = Identifier::parse("\"TEST\"");
    EXPECT_EQ(fmt::format("{}", identifier), "TEST");
    EXPECT_TRUE(identifier.isCaseSensitive());
    EXPECT_EQ(identifier.getOriginalString(), "\"TEST\"");
}

TEST_F(IdentifierTest, Equality)
{
    constexpr auto identifier1 = Identifier::parse("test");
    constexpr auto identifier2 = Identifier::parse("\"TEST\"");
    EXPECT_EQ(identifier1, identifier2);
    EXPECT_EQ(identifier2, identifier1);
}

TEST_F(IdentifierTest, Inequality)
{
    constexpr auto identifier1 = Identifier::parse("test");
    constexpr auto identifier2 = Identifier::parse("\"Test\"");
    EXPECT_NE(identifier1, identifier2);
    EXPECT_NE(identifier2, identifier1);
}

TEST_F(IdentifierTest, NotParseInvalidIdentifier)
{
    const auto identifier1 = Identifier::tryParse("\"Test");
    const auto identifier2 = Identifier::tryParse("Test\"");
    const auto identifier3 = Identifier::tryParse("T.est");
    const auto identifier4 = Identifier::tryParse("");
    EXPECT_FALSE(identifier1.has_value());
    EXPECT_FALSE(identifier2.has_value());
    EXPECT_FALSE(identifier2.has_value());
    EXPECT_FALSE(identifier4.has_value());
}

TEST_F(IdentifierTest, ParseCorrectInsensitiveList)
{
    const auto idListExpected = IdentifierList::tryParse("Source.TeSt");
    EXPECT_TRUE(idListExpected.has_value());
    const auto& idList = idListExpected.value();
    EXPECT_EQ(std::ranges::size(idList), 2);
    EXPECT_EQ(fmt::format("{}", *std::ranges::begin(idList)), "SOURCE");
    EXPECT_EQ(fmt::format("{}", *(std::ranges::begin(idList) + 1)), "TEST");
    EXPECT_EQ(fmt::format("{}", idList), "SOURCE.TEST");
}

TEST_F(IdentifierTest, ParseCorrectSensitiveList)
{
    const auto idListExpected = IdentifierList::tryParse("\"Source\".\"TeSt\"");
    EXPECT_TRUE(idListExpected.has_value());
    const auto& idList = idListExpected.value();
    EXPECT_EQ(std::ranges::size(idList), 2);
    EXPECT_EQ(fmt::format("{}", *std::ranges::begin(idList)), "Source");
    EXPECT_EQ(fmt::format("{}", *(std::ranges::begin(idList) + 1)), "TeSt");
    EXPECT_EQ(fmt::format("{}", idList), "Source.TeSt");
}

TEST_F(IdentifierTest, ParseCorrectMixedSensitiveList)
{
    const auto idListExpected = IdentifierList::tryParse("Source.\"TeSt\"");
    EXPECT_TRUE(idListExpected.has_value());
    const auto& idList = idListExpected.value();
    EXPECT_EQ(std::ranges::size(idList), 2);
    EXPECT_EQ(fmt::format("{}", *std::ranges::begin(idList)), "SOURCE");
    EXPECT_EQ(fmt::format("{}", *(std::ranges::begin(idList) + 1)), "TeSt");
    EXPECT_EQ(fmt::format("{}", idList), "SOURCE.TeSt");
}

TEST_F(IdentifierTest, NotParseEmptyList)
{
    const auto idListExpected = IdentifierList::tryParse("");
    EXPECT_FALSE(idListExpected.has_value());
}

TEST_F(IdentifierTest, NotParseDotWrongList)
{
    const auto idListExpected1 = IdentifierList::tryParse("\"Sou.\".Test");
    const auto idListExpected2 = IdentifierList::tryParse("\"Source.Test");
    EXPECT_FALSE(idListExpected1.has_value());
    EXPECT_FALSE(idListExpected2.has_value());
}

TEST_F(IdentifierTest, ConcatListWithOne)
{
    const auto idList2 = IdentifierList::create(Identifier::parse("Source"), Identifier::parse("Test"));
    const auto idList3 = idList2 + Identifier::parse("attr");
    EXPECT_EQ(std::ranges::size(idList3), 3);
    EXPECT_EQ(fmt::format("{}", *std::ranges::begin(idList3)), "SOURCE");
    EXPECT_EQ(fmt::format("{}", *(std::ranges::begin(idList3) + 1)), "TEST");
    EXPECT_EQ(fmt::format("{}", *(std::ranges::begin(idList3) + 2)), "ATTR");
    EXPECT_EQ(fmt::format("{}", idList3), "SOURCE.TEST.ATTR");
}

TEST_F(IdentifierTest, ConcatListWithList)
{
    const auto idList21 = IdentifierList::create(Identifier::parse("Source"), Identifier::parse("Test"));
    const auto idList22 = IdentifierList::create(Identifier::parse("Attr"), Identifier::parse("sub"));
    const auto idList3 = idList21 + idList22;
    EXPECT_EQ(std::ranges::size(idList3), 4);
    EXPECT_EQ(fmt::format("{}", *std::ranges::begin(idList3)), "SOURCE");
    EXPECT_EQ(fmt::format("{}", *(std::ranges::begin(idList3) + 1)), "TEST");
    EXPECT_EQ(fmt::format("{}", *(std::ranges::begin(idList3) + 2)), "ATTR");
    EXPECT_EQ(fmt::format("{}", *(std::ranges::begin(idList3) + 3)), "SUB");
    EXPECT_EQ(fmt::format("{}", idList3), "SOURCE.TEST.ATTR.SUB");
}

TEST_F(IdentifierTest, IdSpanLookup)
{
    const auto idList31 = IdentifierList::create(Identifier::parse("Source"), Identifier::parse("Test"), Identifier::parse("Attr"));
    const auto idList32 = IdentifierList::create(Identifier::parse("Source"), Identifier::parse("Test"), Identifier::parse("Attr2"));
    const std::unordered_set<std::span<const Identifier>, std::hash<std::span<const Identifier>>, IdentifierList::SpanEquals<>> idMap{
        std::span{idList31.begin() + 1, idList31.end()},
        std::span{idList32.begin() + 1, idList32.end()},
        std::span{idList31.begin(), idList31.end() - 1},
    };

    EXPECT_TRUE(idMap.contains(std::span{idList31.begin() + 1, idList31.end()}));
    EXPECT_TRUE(idMap.contains(std::span{idList32.begin() + 1, idList32.end()}));
    EXPECT_TRUE(idMap.contains(std::span{idList31.begin(), idList31.end() - 1}));
    EXPECT_TRUE(idMap.contains(std::span{idList32.begin(), idList32.end() - 1}));
    EXPECT_FALSE(idMap.contains(std::span{idList31.begin(), idList31.end()}));
    EXPECT_FALSE(idMap.contains(std::span{idList32.begin(), idList32.end()}));
}

///NOLINTEND(bugprone-unchecked-optional-access)

}
