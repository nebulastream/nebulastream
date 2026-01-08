#include <rapidcheck.h>
#include <DataStructures/HashMap.hpp>
#include <gtest/gtest.h>
#include <rapidcheck/gtest.h>

#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/BufferManager.hpp>
#include <nlohmann/detail/input/parser.hpp>

namespace NES
{

auto genKey(uint32_t keySize)
{
    return rc::gen::map(
        rc::gen::container<std::vector<uint8_t>>(keySize, rc::gen::arbitrary<uint8_t>()),
        [](const std::vector<uint8_t>& v)
        {
            std::vector<std::byte> result;
            result.reserve(v.size());
            for (auto byte : v)
            {
                result.push_back(static_cast<std::byte>(byte));
            }
            return result;
        });
}

auto genValue(uint32_t valueSize)
{
    return rc::gen::map(
        rc::gen::container<std::vector<uint8_t>>(valueSize, rc::gen::arbitrary<uint8_t>()),
        [](const std::vector<uint8_t>& v)
        {
            std::vector<std::byte> result;
            result.reserve(v.size());
            for (auto byte : v)
            {
                result.push_back(static_cast<std::byte>(byte));
            }
            return result;
        });
}

auto genOptions(size_t bufferSize)
{
    return rc::gen::build<HashMap::Options>(
        rc::gen::set(&HashMap::Options::keySize, rc::gen::inRange<uint32_t>(1, bufferSize / 256)),
        rc::gen::set(&HashMap::Options::valueSize, rc::gen::inRange<uint32_t>(1, bufferSize / 128)));
}

struct HashMapTestContext
{
    std::shared_ptr<NES::AbstractBufferProvider> bufferProvider;
    HashMap::Options options;
    NES::TupleBuffer mapBuffer;

    HashMapTestContext(size_t bufferSize, HashMap::Options opts)
        : bufferProvider(BufferManager::create(bufferSize, 100000))
        , options(opts)
        , mapBuffer(HashMap::createHashMap(*bufferProvider, options))
    {
    }

    HashMap map() { return HashMap::load(mapBuffer); }

    static auto hash(std::span<const std::byte> key) -> size_t
    {
        return std::hash<std::string_view>{}({reinterpret_cast<const char*>(key.data()), key.size()});
    }
};

RC_GTEST_PROP(HashMapProperties, GetAfterInsertReturnsValue, ())
{
    auto bufferSize = *rc::gen::inRange<size_t>(4096, 65536);
    auto options = *genOptions(bufferSize);

    HashMapTestContext ctx(bufferSize, options);

    auto key = *genKey(options.keySize);
    auto value = *genValue(options.valueSize);

    ctx.map().insert(key, value, *ctx.bufferProvider, HashMapTestContext::hash);
    auto result = ctx.map().get(key, HashMapTestContext::hash);

    RC_ASSERT(result.has_value());
    RC_ASSERT(std::ranges::equal(*result, value));
}

RC_GTEST_PROP(HashMapProperties, HandlesHashCollisionsCorrectly, ())
{
    auto options = HashMap::Options{.keySize = 8, .valueSize = 16};
    HashMapTestContext ctx(4096, options);

    auto badHash = [](std::span<const std::byte>) -> size_t { return 42; };

    std::map<std::vector<std::byte>, std::vector<std::byte>> model;
    auto number_of_keys = *rc::gen::inRange(short(1), short(2048));
    for (int16_t i = 0; i < number_of_keys; ++i)
    {
        auto key = *genKey(options.keySize);
        auto value = *genValue(options.valueSize);
        ctx.map().insert(key, value, *ctx.bufferProvider, badHash);
        model[key] = value;
    }

    for (const auto& [k, v] : model)
    {
        auto result = ctx.map().get(k, badHash);
        RC_ASSERT(result.has_value());
        RC_ASSERT(std::ranges::equal(*result, v));
    }
}

RC_GTEST_PROP(HashMapProperties, SizeMatchesNumberOfUniqueKeys, ())
{
    auto options = HashMap::Options{.keySize = 4, .valueSize = 8};
    HashMapTestContext ctx(8192, options);

    auto numOps = *rc::gen::inRange(0, 50);
    std::set<std::vector<std::byte>> uniqueKeys;

    for (int i = 0; i < numOps; ++i)
    {
        auto key = *genKey(options.keySize);
        auto value = *genValue(options.valueSize);
        ctx.map().insert(key, value, *ctx.bufferProvider, HashMapTestContext::hash);
        uniqueKeys.insert(key);
    }

    RC_ASSERT(ctx.map().size() == uniqueKeys.size());
}

RC_GTEST_PROP(HashMapProperties, HandlesHighLoadFactor, ())
{
    auto options = HashMap::Options{.keySize = 4, .valueSize = 4};
    HashMapTestContext ctx(4096, options);

    // Rough estimate: HashMapHeader ~32 bytes, Bucket ~8 bytes
    constexpr size_t estimatedHeaderSize = 32;
    constexpr size_t estimatedBucketSize = 8;
    size_t approxBuckets = (4096 - estimatedHeaderSize) / estimatedBucketSize;
    size_t insertCount = *rc::gen::inRange<size_t>(approxBuckets / 2, approxBuckets);

    std::map<std::vector<std::byte>, std::vector<std::byte>> model;

    for (size_t i = 0; i < insertCount; ++i)
    {
        auto key = *genKey(options.keySize);
        auto value = *genValue(options.valueSize);
        ctx.map().insert(key, value, *ctx.bufferProvider, HashMapTestContext::hash);
        model[key] = value;
    }

    for (const auto& [k, v] : model)
    {
        auto result = ctx.map().get(k, HashMapTestContext::hash);
        RC_ASSERT(result.has_value());
        RC_ASSERT(std::ranges::equal(*result, v));
    }
}

} // namespace NES