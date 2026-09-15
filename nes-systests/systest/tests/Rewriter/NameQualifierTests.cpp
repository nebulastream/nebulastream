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
#include <tuple>
#include <utility>

#include <gtest/gtest.h>

#include <Rewriter/NameQualifier.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <BaseUnitTest.hpp>
#include <ErrorHandling.hpp>
#include <TemporaryDirectory.hpp>

namespace NES
{

class NameQualifierTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite()
    {
        Logger::setupLogging("NameQualifier.log", LogLevel::LOG_DEBUG);
        NES_INFO("Setup NameQualifier test class.");
    }

    static void TearDownTestSuite() { NES_INFO("Tear down NameQualifier test class."); }

    const std::filesystem::path rootPath{"/discovery"};
    const DiscoveryRoot root{rootPath};
};

TEST(UnqualifiedTest, RemovesEveryOccurrenceOfThePrefix)
{
    EXPECT_EQ(unqualified("SINK(TESTKEY_SINKONETUPLE) <- SOURCE(TESTKEY_ONETUPLE)", "TESTKEY_"), "SINK(SINKONETUPLE) <- SOURCE(ONETUPLE)");
}

TEST(UnqualifiedTest, KeepsTextWithoutPrefix)
{
    EXPECT_EQ(unqualified("SINK(SINKONETUPLE)", "TESTKEY_"), "SINK(SINKONETUPLE)");
}

TEST(UnqualifiedTest, RemovesAdjacentOccurrences)
{
    EXPECT_EQ(unqualified("A_A_NAME", "A_"), "NAME");
}

/// Asserts that the directory prefix separates duplicate file stems (i.e. without the .test extension).
TEST_F(NameQualifierTest, DuplicateStemsKeyApartByDirectory)
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
        const auto inBenchmark = root.keyOf(rootPath / "benchmark" / fileName, 0, 1).value();
        const auto inBenchmarkSmall = root.keyOf(rootPath / "benchmark_small" / fileName, 0, 1).value();
        EXPECT_NE(inBenchmark, inBenchmarkSmall) << "stem '" << stem << "' collides across directories";
    }
}

TEST_F(NameQualifierTest, VariantSuffixKeepsKeysApart)
{
    EXPECT_NE(
        root.keyOf(rootPath / "benchmark/Nexmark.test", 0, 1).value(),
        root.keyOf(rootPath / "benchmark/Nexmark_with_varsized.test", 0, 1).value());
}

/// A corpus stem that starts with a digit and holds hyphens is not a legal unquoted identifier.
TEST_F(NameQualifierTest, IllegalStemsSanitizedToLegalIdentifiers)
{
    EXPECT_EQ(
        root.keyOf(rootPath / "regression/2025-09-10_BrokenNotFieldAccInJoin.test", 0, 1).value(),
        "REGRESSION_D_2025_2D_09_2D_10__BROKENNOTFIELDACCINJOIN");
}

/// A stem that starts with a digit at the discovery root has no directory in front of it, so it gets a legal leading character.
TEST_F(NameQualifierTest, LeadingDigitStemGetsLegalLeadingCharacter)
{
    EXPECT_EQ(
        root.keyOf(rootPath / "2025-09-10_BrokenNotFieldAccInJoin.test", 0, 1).value(), "_32_025_2D_09_2D_10__BROKENNOTFIELDACCINJOIN");
}

/// The encoding distinguishes distinct paths even when they differ only in characters that an identifier cannot hold.
/// A run with those files would otherwise reject one of them because of duplication.
TEST_F(NameQualifierTest, KeysStayApartForPathsThatSanitizeAlike)
{
    EXPECT_NE(root.keyOf(rootPath / "foo-bar.test", 0, 1).value(), root.keyOf(rootPath / "foo_bar.test", 0, 1).value());
    EXPECT_NE(root.keyOf(rootPath / "foo-bar.test", 0, 1).value(), root.keyOf(rootPath / "foo/bar.test", 0, 1).value());
    EXPECT_NE(root.keyOf(rootPath / "foo_bar.test", 0, 1).value(), root.keyOf(rootPath / "foo/bar.test", 0, 1).value());
    EXPECT_NE(root.keyOf(rootPath / "a_/b.test", 0, 1).value(), root.keyOf(rootPath / "a/_b.test", 0, 1).value());
}

/// A file with a single part keeps its own key, and every further part gets a suffix of its own.
TEST_F(NameQualifierTest, PartKeysStayDistinct)
{
    EXPECT_EQ(root.keyOf(rootPath / "benchmark/Nexmark.test", 0, 1).value(), "BENCHMARK_D_NEXMARK");
    EXPECT_EQ(root.keyOf(rootPath / "benchmark/Nexmark.test", 0, 2).value(), "BENCHMARK_D_NEXMARK_C0");
    EXPECT_EQ(root.keyOf(rootPath / "benchmark/Nexmark.test", 1, 2).value(), "BENCHMARK_D_NEXMARK_C1");
}

/// A file named like another file's part does not take that part's key.
/// No encoded path ends in a bare part suffix, because a lone underscore only occurs inside a token.
TEST_F(NameQualifierTest, PartKeysCannotCollideWithFileKeys)
{
    EXPECT_NE(root.keyOf(rootPath / "x_c0.test", 0, 1).value(), root.keyOf(rootPath / "x.test", 0, 2).value());
}

/// A discovery root given as a relative path addresses the same files as the absolute path to it, and a discovered test file is
/// always absolute.
/// Both have to produce the same key, or the key of every file in the run depends on how the run was invoked.
///
/// A relative path only resolves while the process runs where that directory exists, so the test moves there
/// rather than assuming the directory that the run happened to start in.
TEST_F(NameQualifierTest, RelativeDiscoveryRootKeysLikeTheAbsoluteOne)
{
    /// A relative root only becomes absolute when its first element exists, so the sandbox has to hold it.
    const Testing::TemporaryDirectory sandbox;
    const std::filesystem::path relativeRoot{"nes-systests"};
    std::filesystem::create_directories(sandbox.get() / relativeRoot);

    const auto previous = std::filesystem::current_path();
    std::filesystem::current_path(sandbox.get());
    const auto absoluteRoot = std::filesystem::current_path() / relativeRoot;
    const auto testFile = absoluteRoot / "benchmark" / "Nexmark.test";

    EXPECT_EQ(DiscoveryRoot{relativeRoot}.keyOf(testFile, 0, 1).value(), "BENCHMARK_D_NEXMARK");
    EXPECT_EQ(DiscoveryRoot{relativeRoot}.keyOf(testFile, 0, 1).value(), DiscoveryRoot{absoluteRoot}.keyOf(testFile, 0, 1).value());

    std::filesystem::current_path(previous);
}

/// A file that is not under the discovery root has no key, so this should throw.
TEST_F(NameQualifierTest, RejectsATestFileOutsideTheDiscoveryRoot)
{
    EXPECT_THROW(std::ignore = root.keyOf("/elsewhere/benchmark/Nexmark.test", 0, 1), Exception);
}

/// An unquoted name qualifies the same however it is cased, because the catalog compares it case-insensitively.
/// A repeat returns the same spelling rather than registering a second entry.
TEST_F(NameQualifierTest, DeclareIsCaseInsensitiveAndIdempotent)
{
    NameRegistry registry{root.keyOf(rootPath / "benchmark/Nexmark.test", 0, 1)};
    const auto lower = registry.declare("bid");
    EXPECT_EQ(lower.getOriginalString(), "BENCHMARK_D_NEXMARK_BID");
    EXPECT_EQ(registry.declare("BID"), lower);
    EXPECT_EQ(registry.declare("bid"), lower);
}

/// A name that the test quoted keeps its case and punctuation, and only quotes preserve that in a statement.
TEST_F(NameQualifierTest, QuotesAQualifiedNameThatWasQuoted)
{
    NameRegistry registry{root.keyOf(rootPath / "benchmark/Nexmark.test", 0, 1)};
    EXPECT_EQ(registry.declare(R"("INPUT STREAM")").getOriginalString(), R"("BENCHMARK_D_NEXMARK_INPUT STREAM")");
    EXPECT_EQ(registry.declare("bid").getOriginalString(), "BENCHMARK_D_NEXMARK_BID");
}

/// The sealed names hold the prefix that qualifying used, so a consumer can strip it from a printed plan again.
TEST_F(NameQualifierTest, SealContainsTheQualifyingPrefix)
{
    NameRegistry registry{root.keyOf(rootPath / "benchmark/Nexmark.test", 0, 1)};
    registry.declare("bid");
    EXPECT_EQ(std::move(registry).seal().qualifyingPrefix(), "BENCHMARK_D_NEXMARK_");
}

/// The grammar admits any text between quotes, so a test file can declare a name that no identifier can hold.
/// The file is malformed, and the registry reports it rather than passing the name on to the catalog.
TEST_F(NameQualifierTest, RejectsADeclaredNameThatIsNotALegalIdentifier)
{
    NameRegistry registry{root.keyOf(rootPath / "benchmark/Nexmark.test", 0, 1)};
    EXPECT_THROW(registry.declare(R"("")"), Exception);
    EXPECT_THROW(registry.declare(R"("a.b")"), Exception);
}

}
