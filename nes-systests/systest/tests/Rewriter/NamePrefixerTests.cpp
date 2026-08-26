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

#include <array>
#include <filesystem>
#include <string>
#include <utility>

#include <gtest/gtest.h>

#include <Model/RunnableTestFile.hpp>
#include <Rewriter/NamePrefixer.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <BaseUnitTest.hpp>
#include <ErrorHandling.hpp>
#include <TemporaryDirectory.hpp>

namespace NES
{

class NamePrefixerTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite()
    {
        Logger::setupLogging("NamePrefixer.log", LogLevel::LOG_DEBUG);
        NES_INFO("Setup NamePrefixer test class.");
    }

    static void TearDownTestSuite() { NES_INFO("Tear down NamePrefixer test class."); }

    const std::filesystem::path rootPath{"/discovery"};
    const TestFileKeyFactory root{rootPath};
};

TEST(RestoreNamesTest, RestoresEveryRegisteredName)
{
    const OriginalNames names{{"TESTKEY_SINKONETUPLE", "SINKONETUPLE"}, {"TESTKEY_ONETUPLE", "ONETUPLE"}};
    EXPECT_EQ(restoreNames("SINK(TESTKEY_SINKONETUPLE) <- SOURCE(TESTKEY_ONETUPLE)", names), "SINK(SINKONETUPLE) <- SOURCE(ONETUPLE)");
}

TEST(RestoreNamesTest, KeepsTextWithoutRegisteredNames)
{
    const OriginalNames names{{"TESTKEY_ONETUPLE", "ONETUPLE"}};
    EXPECT_EQ(restoreNames("SINK(SINKONETUPLE)", names), "SINK(SINKONETUPLE)");
}

/// A declared name may start with the key itself, so stripping the prefix textually would eat into the name.
TEST(RestoreNamesTest, RestoresANameThatStartsLikeThePrefix)
{
    const OriginalNames names{{"ORDERS_ORDERS_INPUT", "ORDERS_INPUT"}, {"ORDERS_INPUT", "INPUT"}};
    EXPECT_EQ(restoreNames("SOURCE(ORDERS_ORDERS_INPUT) SOURCE(ORDERS_INPUT)", names), "SOURCE(ORDERS_INPUT) SOURCE(INPUT)");
}

TEST(RestoreNamesTest, LeavesAnUnregisteredNameAlone)
{
    const OriginalNames names{{"ORDERS_ORDERS_INPUT", "ORDERS_INPUT"}};
    EXPECT_EQ(
        restoreNames("PROJECTION(fields: [ORDERS_TOTAL, ORDERS_ORDERS_INPUT2])", names),
        "PROJECTION(fields: [ORDERS_TOTAL, ORDERS_ORDERS_INPUT2])");
}

/// A quoted name may hold regex syntax such as `+`.
TEST(RestoreNamesTest, MatchesAQuotedNameWithRegexSyntaxLiterally)
{
    const OriginalNames names{{"TESTKEY_a+b", "a+b"}, {"TESTKEY_Input Stream", "Input Stream"}};
    EXPECT_EQ(
        restoreNames("SOURCE(TESTKEY_a+b) SOURCE(TESTKEY_aab) SINK(TESTKEY_Input Stream)", names),
        "SOURCE(a+b) SOURCE(TESTKEY_aab) SINK(Input Stream)");
}

TEST(RestoreNamesTest, RestoresTheQualifierOfAFieldReference)
{
    const OriginalNames names{{"TESTKEY_S", "S"}};
    EXPECT_EQ(restoreNames("PROJECTION(fields: [TESTKEY_S.ID])", names), "PROJECTION(fields: [S.ID])");
}

TEST_F(NamePrefixerTest, DuplicateStemsKeyApartByDirectory)
{
    static constexpr std::array DuplicatedStems
        = {"Nexmark",
           "DEBS",
           "YahooStreamingBenchmark",
           "LinearRoadBenchmark",
           "LinearRoadBenchmarkTCP",
           "ClusterMonitoring",
           "Manufacturing",
           "Nexmark_with_varsized",
           "YahooStreamingBenchmark_with_varsized"};

    for (const auto* stem : DuplicatedStems)
    {
        const auto fileName = std::string{stem} + ".test";
        const auto inBenchmark = root.deriveKeyOf(rootPath / "benchmark" / fileName, 0, 1).value();
        const auto inBenchmarkSmall = root.deriveKeyOf(rootPath / "benchmark_small" / fileName, 0, 1).value();
        EXPECT_NE(inBenchmark, inBenchmarkSmall) << "stem '" << stem << "' collides across directories";
    }
}

TEST_F(NamePrefixerTest, VariantSuffixKeepsKeysApart)
{
    EXPECT_NE(
        root.deriveKeyOf(rootPath / "benchmark/Nexmark.test", 0, 1).value(),
        root.deriveKeyOf(rootPath / "benchmark/Nexmark_with_varsized.test", 0, 1).value());
}

/// A corpus stem that starts with a digit and holds hyphens is not a legal unquoted identifier.
TEST_F(NamePrefixerTest, IllegalStemsSanitizedToLegalIdentifiers)
{
    EXPECT_EQ(
        root.deriveKeyOf(rootPath / "regression/2025-09-10_BrokenNotFieldAccInJoin.test", 0, 1).value(),
        "REGRESSION_D_2025_2D_09_2D_10__BROKENNOTFIELDACCINJOIN");
}

/// A stem that starts with a digit at the discovery root has no directory in front of it, so it gets a legal leading character.
TEST_F(NamePrefixerTest, LeadingDigitStemGetsLegalLeadingCharacter)
{
    EXPECT_EQ(
        root.deriveKeyOf(rootPath / "2025-09-10_BrokenNotFieldAccInJoin.test", 0, 1).value(),
        "_32_025_2D_09_2D_10__BROKENNOTFIELDACCINJOIN");
}

/// The encoding distinguishes distinct paths even when they differ only in characters that an identifier cannot hold.
/// A run with those files would otherwise reject one of them because of duplication.
TEST_F(NamePrefixerTest, KeysStayApartForPathsThatSanitizeAlike)
{
    EXPECT_NE(root.deriveKeyOf(rootPath / "foo-bar.test", 0, 1).value(), root.deriveKeyOf(rootPath / "foo_bar.test", 0, 1).value());
    EXPECT_NE(root.deriveKeyOf(rootPath / "foo-bar.test", 0, 1).value(), root.deriveKeyOf(rootPath / "foo/bar.test", 0, 1).value());
    EXPECT_NE(root.deriveKeyOf(rootPath / "foo_bar.test", 0, 1).value(), root.deriveKeyOf(rootPath / "foo/bar.test", 0, 1).value());
    EXPECT_NE(root.deriveKeyOf(rootPath / "a_/b.test", 0, 1).value(), root.deriveKeyOf(rootPath / "a/_b.test", 0, 1).value());
}

/// A file with a single part keeps its own key, and every further part gets a suffix of its own.
TEST_F(NamePrefixerTest, PartKeysStayDistinct)
{
    EXPECT_EQ(root.deriveKeyOf(rootPath / "benchmark/Nexmark.test", 0, 1).value(), "BENCHMARK_D_NEXMARK");
    EXPECT_EQ(root.deriveKeyOf(rootPath / "benchmark/Nexmark.test", 0, 2).value(), "BENCHMARK_D_NEXMARK_C0");
    EXPECT_EQ(root.deriveKeyOf(rootPath / "benchmark/Nexmark.test", 1, 2).value(), "BENCHMARK_D_NEXMARK_C1");
}

/// No encoded path ends in a bare part suffix, because a lone underscore only occurs inside a token.
TEST_F(NamePrefixerTest, PartKeysCannotCollideWithFileKeys)
{
    EXPECT_NE(root.deriveKeyOf(rootPath / "x_c0.test", 0, 1).value(), root.deriveKeyOf(rootPath / "x.test", 0, 2).value());
}

/// Otherwise every key would depend on how the run was invoked.
/// The test changes into the root's parent, because a relative root resolves from the current directory.
TEST_F(NamePrefixerTest, RelativeDiscoveryRootKeysLikeTheAbsoluteOne)
{
    const Testing::TemporaryDirectory sandbox;
    const std::filesystem::path relativeRoot{"nes-systests"};
    std::filesystem::create_directories(sandbox.get() / relativeRoot);

    const auto previous = std::filesystem::current_path();
    std::filesystem::current_path(sandbox.get());
    const auto absoluteRoot = std::filesystem::current_path() / relativeRoot;
    const auto testFile = absoluteRoot / "benchmark" / "Nexmark.test";

    EXPECT_EQ(TestFileKeyFactory{relativeRoot}.deriveKeyOf(testFile, 0, 1).value(), "BENCHMARK_D_NEXMARK");
    EXPECT_EQ(
        TestFileKeyFactory{relativeRoot}.deriveKeyOf(testFile, 0, 1).value(),
        TestFileKeyFactory{absoluteRoot}.deriveKeyOf(testFile, 0, 1).value());

    std::filesystem::current_path(previous);
}

TEST_F(NamePrefixerTest, RelativeDiscoveryRootKeysTheSameWhenItDoesNotExist)
{
    const Testing::TemporaryDirectory sandbox;
    const auto previous = std::filesystem::current_path();
    std::filesystem::current_path(sandbox.get());

    const std::filesystem::path relativeRoot{"absent"};
    const auto testFile = std::filesystem::current_path() / relativeRoot / "benchmark" / "Nexmark.test";

    EXPECT_EQ(TestFileKeyFactory{relativeRoot}.deriveKeyOf(testFile, 0, 1).value(), "BENCHMARK_D_NEXMARK");

    std::filesystem::current_path(previous);
}

TEST_F(NamePrefixerTest, KeysATestFileOutsideTheDiscoveryRootByItsAbsolutePath)
{
    EXPECT_EQ(root.deriveKeyOf("/elsewhere/benchmark/Nexmark.test", 0, 1).value(), "_D_ELSEWHERE_D_BENCHMARK_D_NEXMARK");
    EXPECT_EQ(root.deriveKeyOf("/elsewhere/benchmark/Nexmark.test", 1, 3).value(), "_D_ELSEWHERE_D_BENCHMARK_D_NEXMARK_C1");
    EXPECT_NE(
        root.deriveKeyOf("/elsewhere/benchmark/Nexmark.test", 0, 1).value(),
        root.deriveKeyOf(rootPath / "benchmark/Nexmark.test", 0, 1).value());
}

/// A spelling that only one file claims passes, and so does one that two parts of a file claim under their own keys.
TEST_F(NamePrefixerTest, RejectsASpellingThatTwoFilesDeclare)
{
    PrefixedNameOwners owners;
    owners.claim(OriginalNames{{"A_D_B_S", "D_B_S"}, {"A_OUT", "OUT"}}, rootPath / "a.test");
    owners.claim(OriginalNames{{"A_D_B_OUT", "OUT"}}, rootPath / "a/b.test");
    EXPECT_THROW(owners.claim(OriginalNames{{"A_D_B_S", "S"}}, rootPath / "a/b.test"), Exception);
}

/// An unquoted name gets the same prefixed spelling however it is cased, because the catalog compares it case-insensitively.
/// A repeat returns the same spelling rather than registering a second entry.
TEST_F(NamePrefixerTest, DeclareIsCaseInsensitiveAndIdempotent)
{
    NameRegistry registry{root.deriveKeyOf(rootPath / "benchmark/Nexmark.test", 0, 1)};
    const auto lower = registry.declare("bid");
    EXPECT_EQ(lower.getOriginalString(), "BENCHMARK_D_NEXMARK_BID");
    EXPECT_EQ(registry.declare("BID"), lower);
    EXPECT_EQ(registry.declare("bid"), lower);
}

/// A name that the test quoted keeps its case and punctuation, and only quotes preserve that in a statement.
TEST_F(NamePrefixerTest, QuotesAPrefixedNameThatWasQuoted)
{
    NameRegistry registry{root.deriveKeyOf(rootPath / "benchmark/Nexmark.test", 0, 1)};
    EXPECT_EQ(registry.declare(R"("INPUT STREAM")").getOriginalString(), R"("BENCHMARK_D_NEXMARK_INPUT STREAM")");
    EXPECT_EQ(registry.declare("bid").getOriginalString(), "BENCHMARK_D_NEXMARK_BID");
}

TEST_F(NamePrefixerTest, SealMapsPrefixedNamesBackToTheDeclaredSpelling)
{
    NameRegistry registry{root.deriveKeyOf(rootPath / "benchmark/Nexmark.test", 0, 1)};
    registry.declare("bid");
    registry.declare(R"("Input Stream")");
    EXPECT_EQ(
        std::move(registry).seal().collectOriginalNames(),
        (OriginalNames{{"BENCHMARK_D_NEXMARK_BID", "BID"}, {"BENCHMARK_D_NEXMARK_Input Stream", "Input Stream"}}));
}

/// The grammar admits any quoted text, and the registry rejects it before the catalog sees it.
TEST_F(NamePrefixerTest, RejectsADeclaredNameThatIsNotALegalIdentifier)
{
    NameRegistry registry{root.deriveKeyOf(rootPath / "benchmark/Nexmark.test", 0, 1)};
    EXPECT_THROW(registry.declare(R"("")"), Exception);
    EXPECT_THROW(registry.declare(R"("a.b")"), Exception);
}

}
