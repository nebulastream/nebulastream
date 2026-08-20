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
#include <cctype>
#include <filesystem>
#include <string>
#include <utility>

#include <gtest/gtest.h>

#include <Rewriter/NameQualifier.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <BaseUnitTest.hpp>
#include <ErrorHandling.hpp>

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

    static std::string keyOf(const std::string& relativePath) { return getTestFileKey(root / relativePath, root); }

    static inline const std::filesystem::path root{"/discovery"};
};

/// The nine stems that appear in more than one directory must key differently, or their catalog entries overwrite each other in the
/// shared catalog.

/// A printed plan holds what the test wrote, so comparing it needs the qualifying prefix taken back off.
TEST(UnqualifiedTest, RemovesEveryOccurrenceOfThePrefix)
{
    EXPECT_EQ(unqualified("SINK(TESTKEY_SINKONETUPLE) <- SOURCE(TESTKEY_ONETUPLE)", "TESTKEY_"), "SINK(SINKONETUPLE) <- SOURCE(ONETUPLE)");
}

/// A plan holding nothing this file qualified comes back word for word.
TEST(UnqualifiedTest, LeavesTextWithoutThePrefixAlone)
{
    EXPECT_EQ(unqualified("SINK(SINKONETUPLE)", "TESTKEY_"), "SINK(SINKONETUPLE)");
}

/// Two prefixes in a row are two occurrences, and both go.
TEST(UnqualifiedTest, RemovesAdjacentOccurrences)
{
    EXPECT_EQ(unqualified("A_A_NAME", "A_"), "NAME");
}

/// An empty text has nothing to take off it.
TEST(UnqualifiedTest, HandlesAnEmptyText)
{
    EXPECT_EQ(unqualified("", "TESTKEY_"), "");
}

/// The directory prefix separates them.
TEST_F(NameQualifierTest, DuplicateStemsKeyApartByDirectory)
{
    static constexpr std::array duplicatedStems
        = {"Nexmark",
           "DEBS",
           "YahooStreamingBenchmark",
           "LinearRoadBenchmark",
           "LinearRoadBenchmarkTCP",
           "ClusterMonitoring",
           "Manufacturing",
           "Nexmark_with_varsized",
           "YahooStreamingBenchmark_with_varsized"};

    for (const auto* stem : duplicatedStems)
    {
        const auto inBenchmark = keyOf(std::string{"benchmark/"} + stem + ".test");
        const auto inBenchmarkSmall = keyOf(std::string{"benchmark_small/"} + stem + ".test");
        EXPECT_NE(inBenchmark, inBenchmarkSmall) << "stem '" << stem << "' collides across directories";
    }
}

/// A plain stem and its `_with_varsized` variant share one directory, so the suffix has to separate them on its own.
TEST_F(NameQualifierTest, VariantSuffixKeepsKeysApart)
{
    EXPECT_NE(keyOf("benchmark/Nexmark.test"), keyOf("benchmark/Nexmark_with_varsized.test"));
}

/// Two corpus stems are not legal unquoted identifiers: a leading digit and hyphens.
/// The key has to come back legal and stable.
TEST_F(NameQualifierTest, IllegalStemsSanitizedToLegalIdentifiers)
{
    EXPECT_EQ(keyOf("regression/2025-09-10_BrokenNotFieldAccInJoin.test"), "REGRESSION_2025_09_10_BROKENNOTFIELDACCINJOIN");
    EXPECT_EQ(keyOf("regression/2026-06-15_RedundantProjectionDropsSwap.test"), "REGRESSION_2026_06_15_REDUNDANTPROJECTIONDROPSSWAP");
}

/// A stem that starts with a digit at the discovery root has no directory in front of it, so it gets a legal leading character.
TEST_F(NameQualifierTest, LeadingDigitStemGetsLegalLeadingCharacter)
{
    const auto key = keyOf("2025-09-10_BrokenNotFieldAccInJoin.test");
    ASSERT_FALSE(key.empty());
    EXPECT_FALSE(std::isdigit(static_cast<unsigned char>(key.front())));
}

/// A key folds to itself: it is already upper case and legal, so deriving a key from it again is a no-op.
TEST_F(NameQualifierTest, KeyIsFoldStable)
{
    const auto key = keyOf("benchmark/Nexmark.test");
    EXPECT_EQ(key, "BENCHMARK_NEXMARK");
    EXPECT_EQ(getTestFileKey(std::filesystem::path{"/x"} / key, "/x"), key);
}

/// A discovery root given as a relative path addresses the same files as the absolute path to it, and a discovered test file is
/// always absolute.
/// Both have to produce the same key, or the key of every file in the run depends on how the run was invoked.
///
/// A relative path only names a directory while the process runs where that directory exists, so the test moves there
/// rather than assuming the directory that the run happened to start in.
TEST_F(NameQualifierTest, RelativeDiscoveryRootKeysLikeTheAbsoluteOne)
{
    const std::filesystem::path relativeRoot{"nes-systests"};
    const auto previous = std::filesystem::current_path();
    const auto sandbox = std::filesystem::temp_directory_path() / "nes-name-qualifier-relative-root";
    std::filesystem::create_directories(sandbox / relativeRoot / "benchmark");
    std::filesystem::current_path(sandbox);

    const auto absoluteRoot = std::filesystem::current_path() / relativeRoot;
    const auto testFile = absoluteRoot / "benchmark" / "Nexmark.test";

    EXPECT_EQ(getTestFileKey(testFile, relativeRoot), "BENCHMARK_NEXMARK");
    EXPECT_EQ(getTestFileKey(testFile, relativeRoot), getTestFileKey(testFile, absoluteRoot));

    std::filesystem::current_path(previous);
    std::error_code errorCode;
    std::filesystem::remove_all(sandbox, errorCode);
}

/// A file that is not under the discovery root has no key, and an unqualified name from one test file collides in the shared
/// catalog with the same name from the next.
/// Report it rather than return a key that qualifies nothing.
TEST_F(NameQualifierTest, RejectsATestFileOutsideTheDiscoveryRoot)
{
    EXPECT_THROW(getTestFileKey("/elsewhere/benchmark/Nexmark.test", root), Exception);
}

/// An unquoted name qualifies the same however it is cased, because the catalog folds it.
/// A repeat returns the same spelling rather than registering a second entry.
TEST_F(NameQualifierTest, DeclareIsCaseInsensitiveAndIdempotent)
{
    NameRegistry registry{"BENCHMARK_NEXMARK"};
    const auto lower = registry.declare("bid");
    EXPECT_EQ(lower, "BENCHMARK_NEXMARK_BID");
    EXPECT_EQ(registry.declare("BID"), lower);
    EXPECT_EQ(registry.declare("bid"), lower);
}

/// A name that the test quoted keeps its case and punctuation, and only quotes preserve that in a statement.
/// Prefixing such a name and emitting it bare produces a statement that does not parse, when the name holds a space.
TEST_F(NameQualifierTest, QuotesAQualifiedNameThatWasQuoted)
{
    NameRegistry registry{"BENCHMARK_NEXMARK"};
    EXPECT_EQ(registry.declare(R"("INPUT STREAM")"), R"("BENCHMARK_NEXMARK_INPUT STREAM")");
    EXPECT_EQ(registry.declare("bid"), "BENCHMARK_NEXMARK_BID");
}

/// The reverse map restores the author's spelling for failure output, and reports names it never saw as unknown rather than inventing one.
TEST_F(NameQualifierTest, OriginalRestoresAuthorSpelling)
{
    NameRegistry registry{"BENCHMARK_NEXMARK"};
    const auto qualified = registry.declare("bid");
    const auto names = std::move(registry).seal();
    ASSERT_TRUE(names.original(qualified).has_value());
    EXPECT_EQ(names.original(qualified).value(), "bid");
    EXPECT_FALSE(names.original("BENCHMARK_NEXMARK_UNSEEN").has_value());
}

}
