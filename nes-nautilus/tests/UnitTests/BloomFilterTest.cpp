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
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <mutex>
#include <random>
#include <span>
#include <unordered_set>
#include <vector>
#include <Interface/Hash/BloomFilterRef.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <gtest/gtest.h> /// NOLINT(misc-include-cleaner): consumed via macros expanded from rapidcheck/gtest.h
#include <TestableBloomFilter.hpp>

#include <rapidcheck.h> /// NOLINT(misc-include-cleaner)
#include <rapidcheck/gtest.h>

/// NOLINTBEGIN(misc-include-cleaner)
namespace NES::Nautilus::Interface
{
namespace
{

using TestUtils::EngineMode;
using TestUtils::TestableBloomFilter;

/// Configured FP rate range drawn per property, as a [min, max] numerator over FP_RATES_RATIO.
constexpr std::array<uint64_t, 2> FP_RATES = {1, 2000};
constexpr double FP_RATES_RATIO = 10'000;

/// Draws a false-positive rate in [FP_RATES[0], FP_RATES[1]] / FP_RATES_RATIO. We need this weird conversion,
/// as rapidcheck does not support inRange<double>(), c.f., https://github.com/emil-e/rapidcheck/issues/134
rc::Gen<double> genFpRate()
{
    return rc::gen::map(
        rc::gen::inRange(FP_RATES[0], FP_RATES[1]),
        [](const uint64_t numerator) -> double { return static_cast<double>(numerator) / FP_RATES_RATIO; });
}

/// `expectedEntries` values that bracket the integer edges of the sizing math: the empty filter, the
/// single-word boundary, and counts so large that the double-valued bit count no longer fits in a uint64_t.
constexpr std::array<uint64_t, 8> INTERESTING_ENTRY_COUNTS = {0, 1, 2, 63, 64, 65, 1ULL << 32U, std::numeric_limits<uint64_t>::max()};

/// FP rates outside what genFpRate's ratio can express: small enough to blow the bit count past uint64_t
/// when paired with a large entry count, and large enough to collapse it below the 64-bit floor.
constexpr std::array<double, 4> INTERESTING_FP_RATES = {1e-12, 1e-6, 0.5, 0.99};

/// Sizing parameter range (the `expectedEntries` argument to BloomFilterParams::compute).
/// 0 is included to exercise the empty-filter edge case from the old `edgeCaseZeroExpectedEntries` test.
constexpr uint64_t MIN_NUM_KEYS_SIZING = 0;
constexpr uint64_t MAX_NUM_KEYS_SIZING = 100000;

/// Unique key pool size range. 1 keeps the same-key-multiple-inserts shape from the old test alive.
constexpr uint64_t MIN_NUM_UNIQUE = 1;
constexpr uint64_t MAX_NUM_UNIQUE = 50000;

/// Multiplier on numUnique that produces numTotal. With max numUnique 50k and max multiplier 20, numTotal can reach 1M.
constexpr uint64_t MIN_TOTAL_MULTIPLIER = 1;
constexpr uint64_t MAX_TOTAL_MULTIPLIER = 20;

/// Sizings that bracket the collision spectrum and that `BloomFilterParams::compute` can never return:
/// 64 bits with 10 hash positions saturates after a handful of inserts (FP rate ~1, so `mightContain` is
/// constant-true in practice), while 2^23 bits with a single hash position stays near-empty (FP rate ~0,
/// so nearly every negative query is answered correctly). The no-false-negative invariant must hold at
/// both ends; the FP-rate bound needs no special casing because it is derived from the actual sizing.
constexpr std::array<BloomFilterParams, 3> EXTREME_PARAMS = {
    BloomFilterParams{.bitCount = 64, .hashCount = 10},
    BloomFilterParams{.bitCount = 64, .hashCount = 1},
    BloomFilterParams{.bitCount = 1ULL << 23U, .hashCount = 1},
};

/// Lower bound on the number of true-negative samples needed before we trust the measured FP rate.
constexpr size_t MIN_FP_SAMPLES_FOR_RATE_CHECK = 200;

/// Split between "insert this fraction of the sampled sequence" and "query the rest as
/// not-inserted candidates" inside `insertAndCheckProperty`. Hoisted as constexpr so the ratio is
/// easy to retune without hunting for the literal.
constexpr uint64_t INSERT_RATIO_NUMERATOR = 9;
constexpr uint64_t INSERT_RATIO_DENOMINATOR = 10;

/// Tolerance applied to the theoretical FP rate before asserting. 2x absorbs sampling noise on the
/// upper end; the additive 0.05 absorbs the floor case where theoretical < 0.01 and a handful of
/// FP hits could otherwise push the ratio over 2x by themselves.
constexpr double FP_RATE_TOLERANCE_FACTOR = 2.0;
constexpr double FP_RATE_SLACK = 0.05;

/// Theoretical FP rate for a saturated filter with n distinct items in m bits and k hash positions.
/// Reference: standard Bloom-filter analysis, e.g. Broder & Mitzenmacher, "Network Applications of Bloom Filters".
double theoreticalFpRate(uint64_t numEntries, uint64_t numBits, uint64_t numHashes)
{
    if (numEntries == 0 || numBits == 0)
    {
        return 0.0;
    }
    const auto exponent = -static_cast<double>(numHashes) * static_cast<double>(numEntries) / static_cast<double>(numBits);
    return std::pow(1.0 - std::exp(exponent), static_cast<double>(numHashes));
}

/// Draws either the `compute`-derived sizing for (numKeysForSizing, fpRate) or one of the collision
/// extremes, so every property that sizes a filter also runs against the saturated and the near-empty end.
BloomFilterParams drawBloomFilterParams(uint64_t numKeysForSizing, double fpRate)
{
    const auto idx = *rc::gen::inRange<size_t>(0, EXTREME_PARAMS.size() + 1);
    return idx < EXTREME_PARAMS.size() ? EXTREME_PARAMS.at(idx) : BloomFilterParams::compute(numKeysForSizing, fpRate);
}

/// Build a pool of `numUnique` distinct uint64_t keys plus a length-`numTotal` sample drawn with
/// replacement from the pool. A single rapidcheck-shrunk seed feeds the deterministic RNG so that
/// shrinking remains effective while bulk generation stays out of rapidcheck's per-value path.
struct KeyStream
{
    std::vector<uint64_t> uniquePool;
    std::vector<uint64_t> allKeys;
};

KeyStream generateKeyStream(uint64_t numUnique, uint64_t numTotal, uint64_t seed)
{
    KeyStream out;
    std::mt19937_64 rng{seed};

    std::unordered_set<uint64_t> seen;
    seen.reserve(numUnique);
    out.uniquePool.reserve(numUnique);
    while (out.uniquePool.size() < numUnique)
    {
        const auto candidate = rng();
        if (seen.insert(candidate).second)
        {
            out.uniquePool.push_back(candidate);
        }
    }

    out.allKeys.reserve(numTotal);
    std::uniform_int_distribution<size_t> indexDist{0, numUnique - 1};
    for (uint64_t i = 0; i < numTotal; ++i)
    {
        out.allKeys.push_back(out.uniquePool[indexDist(rng)]);
    }
    return out;
}

/// Property: inserting a set of keys then querying produces no false negatives — every inserted key is
/// reported present — and the measured false-positive rate stays within the theoretical bound for the
/// actual saturation. Single-threaded: one filter, one bit array.
void insertAndCheckProperty(EngineMode mode)
{
    const auto numKeysForSizing = *rc::gen::inRange<uint64_t>(MIN_NUM_KEYS_SIZING, MAX_NUM_KEYS_SIZING + 1);
    const auto fpRate = *genFpRate();
    const auto numUnique = *rc::gen::inRange<uint64_t>(MIN_NUM_UNIQUE, MAX_NUM_UNIQUE + 1);
    const auto totalMultiplier = *rc::gen::inRange<uint64_t>(MIN_TOTAL_MULTIPLIER, MAX_TOTAL_MULTIPLIER + 1);
    const auto numTotal = numUnique * totalMultiplier;
    const auto seed = *rc::gen::noShrink(rc::gen::arbitrary<uint64_t>());
    const auto bfParams = drawBloomFilterParams(numKeysForSizing, fpRate);

    NES_INFO(
        "insertAndCheck mode={} numKeysSizing={} fpRate={} numUnique={} numTotal={} seed={} bits={} hashes={}",
        mode == EngineMode::Compiler ? "Compiler" : "Interpreter",
        numKeysForSizing,
        fpRate,
        numUnique,
        numTotal,
        seed,
        bfParams.bitCount,
        bfParams.hashCount);

    const auto stream = generateKeyStream(numUnique, numTotal, seed);

    /// 90/10 split on the sampled sequence (not the unique pool); the two halves may share underlying values.
    const uint64_t splitIdx = std::max<uint64_t>(1, (numTotal * INSERT_RATIO_NUMERATOR) / INSERT_RATIO_DENOMINATOR);
    const std::span<const uint64_t> toInsert{stream.allKeys.data(), splitIdx};
    const std::span<const uint64_t> notInsertedQueries{stream.allKeys.data() + splitIdx, stream.allKeys.size() - splitIdx};

    TestableBloomFilter filter{bfParams, mode};

    /// Insert every key and assert it is immediately reported present (a BloomFilter has no false negatives).
    for (const auto key : toInsert)
    {
        filter.add(key);
        RC_ASSERT(filter.mightContain(key));
    }

    /// Post-insert sweep: every distinct inserted key must still be present.
    const std::unordered_set<uint64_t> insertedDistinct{toInsert.begin(), toInsert.end()};
    for (const auto key : insertedDistinct)
    {
        RC_ASSERT(filter.mightContain(key));
    }

    /// FP-rate measurement: drop notInserted entries that happen to coincide with an inserted value,
    /// then measure the false-positive ratio against the theoretical bound for the actual saturation.
    std::vector<uint64_t> trueNegatives;
    trueNegatives.reserve(notInsertedQueries.size());
    for (const auto key : notInsertedQueries)
    {
        if (!insertedDistinct.contains(key))
        {
            trueNegatives.push_back(key);
        }
    }

    if (trueNegatives.size() >= MIN_FP_SAMPLES_FOR_RATE_CHECK)
    {
        size_t fpCount = 0;
        for (const auto key : trueNegatives)
        {
            if (filter.mightContain(key))
            {
                ++fpCount;
            }
        }
        const double actualFpRate = static_cast<double>(fpCount) / static_cast<double>(trueNegatives.size());
        const double theoretical = theoreticalFpRate(insertedDistinct.size(), bfParams.bitCount, bfParams.hashCount);
        const double tolerance = (FP_RATE_TOLERANCE_FACTOR * theoretical) + FP_RATE_SLACK;
        NES_INFO(
            "FP rate: actual={} theoretical={} tolerance={} samples={} configured={}",
            actualFpRate,
            theoretical,
            tolerance,
            trueNegatives.size(),
            fpRate);
        RC_ASSERT(actualFpRate <= tolerance);
    }
    else
    {
        NES_INFO("Skipping FP rate check: only {} true negative samples (need {})", trueNegatives.size(), MIN_FP_SAMPLES_FOR_RATE_CHECK);
    }
}

/// Property: zeroing the bit array reverts every previously-inserted key to mightContain=false.
/// Single-threaded so the clear is uncontested; the multi-threaded race surface is covered by the
/// main property above.
void clearResetsFilterProperty(EngineMode mode)
{
    const auto numKeysForSizing = *rc::gen::inRange<uint64_t>(MIN_NUM_KEYS_SIZING, MAX_NUM_KEYS_SIZING + 1);
    const auto fpRate = *genFpRate();
    const auto numKeys = *rc::gen::inRange<uint64_t>(1, uint64_t{1000} + 1);
    const auto seed = *rc::gen::arbitrary<uint64_t>();

    TestableBloomFilter filter{drawBloomFilterParams(numKeysForSizing, fpRate), mode};

    const auto stream = generateKeyStream(numKeys, numKeys, seed);
    for (const auto key : stream.uniquePool)
    {
        filter.add(key);
    }
    for (const auto key : stream.uniquePool)
    {
        RC_ASSERT(filter.mightContain(key));
    }

    filter.clear();

    for (const auto key : stream.uniquePool)
    {
        RC_ASSERT(!filter.mightContain(key));
    }
}

/// Property: BloomFilterParams::compute returns a sane, non-degenerate sizing for every (numKeys, fpRate)
/// input - including numKeys = 0 and the integer extremes, where the double-valued sizing math would
/// otherwise overflow the uint64_t bit count and wrap the derived allocation size.
/// Covers the old `edgeCaseZeroExpectedEntries` and `gettersReturnReasonableValues`.
void sizingProperty()
{
    const auto numKeys = *rc::gen::oneOf(
        rc::gen::elementOf(INTERESTING_ENTRY_COUNTS),
        rc::gen::arbitrary<uint64_t>(),
        rc::gen::inRange<uint64_t>(0, MAX_NUM_KEYS_SIZING + 1));
    const auto fpRate = *rc::gen::oneOf(rc::gen::elementOf(INTERESTING_FP_RATES), genFpRate());
    const auto params = BloomFilterParams::compute(numKeys, fpRate);
    RC_ASSERT(params.bitCount >= 64);
    RC_ASSERT(params.hashCount >= 1);
    RC_ASSERT(params.allocationByteCount() >= 8);
    /// The allocation has to cover every bit the filter indexes; a wrapped bit or byte count shows up here.
    RC_ASSERT(params.allocationByteCount() * 8 >= params.bitCount);
}

/// Set up logging exactly once for the whole process. rapidcheck re-runs each RC_GTEST_PROP body
/// ~100 times, and the underlying Rust `tracing` dispatcher can only be installed as the global
/// default once. Otherwise, we would get a `SetGlobalDefaultError ... already been set` warning.
void ensureLoggingSetup()
{
    static std::once_flag flag;
    std::call_once(flag, [] { Logger::setupLogging("BloomFilterPropertyTest.log", LogLevel::LOG_DEBUG); });
}

}

/// One RC_GTEST_PROP per (property, engine mode) so a failure on one backend doesn't mask the other
/// and rapidcheck shrinks each backend's failing input independently.
RC_GTEST_PROP(BloomFilterPropertyTest, insertAndCheckCompiler, ())
{
    ensureLoggingSetup();
    insertAndCheckProperty(EngineMode::Compiler);
}

RC_GTEST_PROP(BloomFilterPropertyTest, insertAndCheckInterpreter, ())
{
    ensureLoggingSetup();
    insertAndCheckProperty(EngineMode::Interpreter);
}

RC_GTEST_PROP(BloomFilterPropertyTest, clearResetsFilterCompiler, ())
{
    ensureLoggingSetup();
    clearResetsFilterProperty(EngineMode::Compiler);
}

RC_GTEST_PROP(BloomFilterPropertyTest, clearResetsFilterInterpreter, ())
{
    ensureLoggingSetup();
    clearResetsFilterProperty(EngineMode::Interpreter);
}

RC_GTEST_PROP(BloomFilterPropertyTest, sizing, ())
{
    sizingProperty();
}

}

/// NOLINTEND(misc-include-cleaner)
