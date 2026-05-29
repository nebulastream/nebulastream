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

#include <algorithm>
#include <array>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <random>
#include <span>
#include <unordered_set>
#include <utility>
#include <vector>
#include <Interface/Hash/BloomFilterRef.hpp>
#include <Interface/Hash/MurMur3HashFunction.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <gtest/gtest.h> /// NOLINT(misc-include-cleaner): consumed via macros expanded from rapidcheck/gtest.h
#include <nautilus/Engine.hpp>
#include <options.hpp>

#include <rapidcheck.h> /// NOLINT(misc-include-cleaner)
#include <rapidcheck/gtest.h>

/// NOLINTBEGIN(misc-include-cleaner)
namespace NES::Nautilus::Interface
{
namespace
{

enum class EngineMode : std::uint8_t
{
    Interpreter,
    Compiler
};

nautilus::engine::NautilusEngine makeEngine(EngineMode mode)
{
    nautilus::engine::Options options;
    options.setOption("engine.Compilation", mode == EngineMode::Compiler);
    options.setOption("mlir.enableMultithreading", false);
    return nautilus::engine::NautilusEngine{options};
}

/// Configured FP rates drawn per property.
constexpr std::array<double, 6> FP_RATES = {0.0001, 0.001, 0.01, 0.05, 0.1, 0.2};

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

uint64_t divRoundUp(const uint64_t dividend, const uint64_t divisor)
{
    return static_cast<uint64_t>(std::ceil(static_cast<double>(dividend) / static_cast<double>(divisor)));
}

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

/// Wraps the Nautilus-compiled add/mightContain pair. Engine setup and function registration happen
/// once per instance so the hot loop is just function-call overhead. The `enabled` flag picks
/// between the real BloomFilter variant and the no-op variant
class TestableBloomFilter
{
public:
    /// NOLINTBEGIN(performance-unnecessary-value-param)
    TestableBloomFilter(bool enabled, BloomFilterParams params, EngineMode mode)
        : params{params}
        , bits(divRoundUp(params.bitCount, 64), 0)
        , engine{std::make_unique<nautilus::engine::NautilusEngine>(makeEngine(mode))}
        , addFn{engine->registerFunction(std::function(
              [enabled, params](nautilus::val<uint64_t*> bitsPtr, nautilus::val<uint64_t> key)
              {
                  const MurMur3HashFunction hashFunc;
                  const HashFunction& hashFunction = hashFunc;
                  auto hash = hashFunction.calculate(VarVal{key});
                  const auto bloomFilter
                      = BloomFilterRef::create(enabled, params.bitCount, params.hashCount, std::make_unique<MurMur3HashFunction>());
                  bloomFilter.add(bitsPtr, hash);
              }))}
        , mightContainFn{engine->registerFunction(std::function(
              [enabled, params](nautilus::val<uint64_t*> bitsPtr, nautilus::val<uint64_t> key) -> nautilus::val<bool>
              {
                  const MurMur3HashFunction hashFunc;
                  const HashFunction& hashFunction = hashFunc;
                  auto hash = hashFunction.calculate(VarVal{key});
                  const auto bloomFilter
                      = BloomFilterRef::create(enabled, params.bitCount, params.hashCount, std::make_unique<MurMur3HashFunction>());
                  return bloomFilter.mightContain(bitsPtr, hash);
              }))}
    {
    }

    /// NOLINTEND(performance-unnecessary-value-param)

    TestableBloomFilter(const TestableBloomFilter&) = delete;
    TestableBloomFilter& operator=(const TestableBloomFilter&) = delete;
    TestableBloomFilter(TestableBloomFilter&&) = default;
    TestableBloomFilter& operator=(TestableBloomFilter&&) = default;
    ~TestableBloomFilter() = default;

    void add(uint64_t key) { addFn(bits.data(), key); }

    void add(uint64_t* externalBits, uint64_t key) { addFn(externalBits, key); }

    bool mightContain(uint64_t key) { return mightContainFn(bits.data(), key); }

    bool mightContain(uint64_t* externalBits, uint64_t key) { return mightContainFn(externalBits, key); }

    void clear() { std::ranges::fill(bits, uint64_t{0}); }

private:
    BloomFilterParams params;
    std::vector<uint64_t> bits;
    std::unique_ptr<nautilus::engine::NautilusEngine> engine;
    nautilus::engine::CallableFunction<void, uint64_t*, uint64_t> addFn;
    nautilus::engine::CallableFunction<nautilus::val<bool>, uint64_t*, uint64_t> mightContainFn;
};

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
    const auto fpRate = *rc::gen::elementOf(FP_RATES);
    const auto numUnique = *rc::gen::inRange<uint64_t>(MIN_NUM_UNIQUE, MAX_NUM_UNIQUE + 1);
    const auto totalMultiplier = *rc::gen::inRange<uint64_t>(MIN_TOTAL_MULTIPLIER, MAX_TOTAL_MULTIPLIER + 1);
    const auto numTotal = numUnique * totalMultiplier;
    const auto seed = *rc::gen::arbitrary<uint64_t>();

    NES_INFO(
        "insertAndCheck mode={} numKeysSizing={} fpRate={} numUnique={} numTotal={} seed={}",
        mode == EngineMode::Compiler ? "Compiler" : "Interpreter",
        numKeysForSizing,
        fpRate,
        numUnique,
        numTotal,
        seed);

    const auto stream = generateKeyStream(numUnique, numTotal, seed);

    /// 90/10 split on the sampled sequence (not the unique pool); the two halves may share underlying values.
    const uint64_t splitIdx = std::max<uint64_t>(1, (numTotal * INSERT_RATIO_NUMERATOR) / INSERT_RATIO_DENOMINATOR);
    const std::span<const uint64_t> toInsert{stream.allKeys.data(), splitIdx};
    const std::span<const uint64_t> notInsertedQueries{stream.allKeys.data() + splitIdx, stream.allKeys.size() - splitIdx};

    const auto bfParams = BloomFilterParams::compute(numKeysForSizing, fpRate);
    TestableBloomFilter filter{true, bfParams, mode};

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
    const auto fpRate = *rc::gen::elementOf(FP_RATES);
    const auto numKeys = *rc::gen::inRange<uint64_t>(1, uint64_t{1000} + 1);
    const auto seed = *rc::gen::arbitrary<uint64_t>();

    const auto bfParams = BloomFilterParams::compute(numKeysForSizing, fpRate);
    TestableBloomFilter filter{true, bfParams, mode};

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

/// Property: a disabled BloomFilter's `mightContain` returns true for every input. The bit pointer is never
/// dereferenced, so we pass a single sentinel and a rapidcheck-drawn vector of keys. Params are unused
/// by the no-op variant — the smallest valid sizing (64,1) is passed only to satisfy the unified ctor.
void noOpAlwaysReturnsTrueProperty(EngineMode mode)
{
    TestableBloomFilter noOp{false, BloomFilterParams{.bitCount = 64, .hashCount = 1}, mode};
    const auto keys = *rc::gen::container<std::vector<uint64_t>>(rc::gen::arbitrary<uint64_t>());
    uint64_t sentinel{0};
    for (const auto key : keys)
    {
        RC_ASSERT(noOp.mightContain(&sentinel, key));
    }
}

/// Property: BloomFilterParams::compute returns a sane, non-degenerate sizing for every (numKeys, fpRate)
/// input - including numKeys = 0. Covers the old `edgeCaseZeroExpectedEntries` and `gettersReturnReasonableValues`.
void sizingProperty()
{
    const auto numKeys = *rc::gen::inRange<uint64_t>(0, MAX_NUM_KEYS_SIZING + 1);
    const auto fpRate = *rc::gen::elementOf(FP_RATES);
    const auto params = BloomFilterParams::compute(numKeys, fpRate);
    RC_ASSERT(params.bitCount >= 64);
    RC_ASSERT(params.hashCount >= 1);
    RC_ASSERT(params.allocationByteCount() >= 8);
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

RC_GTEST_PROP(BloomFilterPropertyTest, noOpAlwaysReturnsTrueCompiler, ())
{
    ensureLoggingSetup();
    noOpAlwaysReturnsTrueProperty(EngineMode::Compiler);
}

RC_GTEST_PROP(BloomFilterPropertyTest, noOpAlwaysReturnsTrueInterpreter, ())
{
    ensureLoggingSetup();
    noOpAlwaysReturnsTrueProperty(EngineMode::Interpreter);
}

RC_GTEST_PROP(BloomFilterPropertyTest, sizing, ())
{
    sizingProperty();
}

}

/// NOLINTEND(misc-include-cleaner)
