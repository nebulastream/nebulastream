#include <rapidcheck.h>
#include <DataStructures/HashMap.hpp>

#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/BufferManager.hpp>
#include <Util/TypeTraits.hpp>
#include <gtest/gtest.h>
#include <nlohmann/detail/input/parser.hpp>
#include <rapidcheck/gtest.h>
#include <select.hpp>
#include "../../cmake-build-release2/vcpkg_installed/x64-linux-none-local/include/nautilus/Engine.hpp"
#include "DataTypes/DataTypeProvider.hpp"
#include "DataTypes/Schema.hpp"

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

RC_GTEST_PROP(HashMapProperties, CompareToModel, ())
{
    auto bufferSize = *rc::gen::inRange<size_t>(4096, 65536);
    auto options = *genOptions(bufferSize);

    HashMapTestContext ctx(bufferSize, options);
    std::map<std::vector<std::byte>, std::vector<std::byte>> model{};

    auto numberOfInsertions = *rc::gen::inRange<size_t>(1, 1024);
    for (size_t i = 0; i < numberOfInsertions; ++i)
    {
        auto key = *genKey(options.keySize);
        auto value = *genValue(options.valueSize);
        model.insert_or_assign(std::vector(key.begin(), key.end()), std::vector(value.begin(), value.end()));
        ctx.map().insert(key, value, *ctx.bufferProvider, HashMapTestContext::hash);
    }

    for (auto entry : model)
    {
        auto result = ctx.map().get(entry.first, HashMapTestContext::hash);
        RC_ASSERT(result.has_value());
        RC_ASSERT(std::ranges::equal(*result, entry.second));
    }
}

RC_GTEST_PROP(HashMapProperties, CompareToModelIterator, ())
{
    auto bufferSize = *rc::gen::inRange<size_t>(4096, 65536);
    auto options = *genOptions(bufferSize);

    HashMapTestContext ctx(bufferSize, options);
    std::map<std::vector<std::byte>, std::vector<std::byte>> model{};

    auto numberOfInsertions = *rc::gen::inRange<size_t>(1, 1024);
    for (size_t i = 0; i < numberOfInsertions; ++i)
    {
        auto key = *genKey(options.keySize);
        auto value = *genValue(options.valueSize);
        model.insert_or_assign(std::vector(key.begin(), key.end()), std::vector(value.begin(), value.end()));
        ctx.map().insert(key, value, *ctx.bufferProvider, HashMapTestContext::hash);
    }

    std::map<std::vector<std::byte>, std::vector<std::byte>> result{};
    for (auto entry : ctx.map())
    {
        result[std::vector<std::byte>(entry.key.begin(), entry.key.end())] = std::vector<std::byte>(entry.value.begin(), entry.value.end());
    }
    RC_ASSERT(std::ranges::equal(result, model));
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

// Generator for a valid DataType from your allowed set
rc::Gen<DataType::Type> genDataType()
{
    return rc::gen::element(DataType::Type::UINT32, DataType::Type::FLOAT32, DataType::Type::INT64);
}

// Generator for unique field names
rc::Gen<std::string> genFieldName(int index)
{
    return rc::gen::just("field_" + std::to_string(index));
}

rc::Gen<NES::Schema> genSchemaStrict(size_t maxByteSize)
{
    return rc::gen::mapcat(
        rc::gen::inRange<size_t>(1, 15), // num fields
        [=](size_t n)
        {
            return rc::gen::exec(
                [=]()
                {
                    NES::Schema schema{};
                    for (size_t i = 0; i < n; ++i)
                    {
                        schema.addField("f" + std::to_string(i), DataTypeProvider::provideDataType(*genDataType(), false));
                    }
                    RC_PRE(schema.getSizeOfSchemaInBytes() <= maxByteSize);
                    return schema;
                });
        });
}

// Generate raw values as a variant
using RawValue = std::variant<uint32_t, float, int64_t /* ... */>;

rc::Gen<RawValue> genRawValueForType(DataType::Type type)
{
    switch (type)
    {
        case DataType::Type::UINT32:
            return rc::gen::map(rc::gen::arbitrary<uint32_t>(), [](uint32_t v) -> RawValue { return v; });
        case DataType::Type::FLOAT32:
            return rc::gen::map(rc::gen::arbitrary<float>(), [](float v) -> RawValue { return v; });
        case DataType::Type::INT64:
            return rc::gen::map(rc::gen::arbitrary<int64_t>(), [](int64_t v) -> RawValue { return v; });
        default:
            throw std::runtime_error("Unsupported type");
    }
}

rc::Gen<std::vector<RawValue>> genRawValuesForSchema(const NES::Schema& schema)
{
    return rc::gen::exec(
        [&]()
        {
            std::vector<RawValue> values;
            for (const auto& field : schema.getFields())
            {
                values.push_back(*genRawValueForType(field.dataType.type));
            }
            return values;
        });
}

// Convert at test time, outside the generator
std::vector<VarVal> toVarVals(const std::vector<RawValue>& raw)
{
    std::vector<VarVal> result;
    result.reserve(raw.size());

    for (const auto& v : raw)
    {
        std::visit([&result](auto&& val) { result.push_back(VarVal(val)); }, v);
    }
    return result;
}

void append(nautilus::val<std::vector<RawValue>*> result, VarVal v)
{
    v.customVisit(
        [&]<typename T>(const T& t)
        {
            if constexpr (requires(T* t) { typename T::raw_type; })
            {
                if constexpr (VariantMember<typename T::raw_type, RawValue>)
                {
                    nautilus::invoke(+[](std::vector<RawValue>* v, typename T::raw_type t) { v->emplace_back(t); }, result, t);
                }
                else
                {
                    throw std::runtime_error("Unsupported type");
                }
            }
            else
            {
                throw std::runtime_error("Unsupported type");
            }

            return VarVal(t);
        });
}

void append(nautilus::val<std::vector<std::vector<RawValue>>*> result, VarVal v, nautilus::val<size_t> index)
{
    auto resultVector = nautilus::invoke(+[](std::vector<std::vector<RawValue>>* v, size_t index) { return &v->at(index); }, result, index);
    append(resultVector, v);
}

RC_GTEST_PROP(HashMapReference, InsertAndRetrieve, ())
{
    cpptrace::register_terminate_handler();
    nautilus::engine::Options options;
    options.setOption("engine.Compilation", true);
    nautilus::engine::NautilusEngine engine(options);

    auto bm = BufferManager::create();
    auto keySchema = *genSchemaStrict(128);
    auto valueSchema = *genSchemaStrict(256);

    auto map = HashMap::createHashMap(
        *bm,
        HashMap::Options{
            .keySize = static_cast<uint32_t>(keySchema.getSizeOfSchemaInBytes()),
            .valueSize = static_cast<uint32_t>(valueSchema.getSizeOfSchemaInBytes())});
    auto keys = *genRawValuesForSchema(keySchema);
    auto values = *genRawValuesForSchema(valueSchema);

    auto fn = engine.registerFunction(
        std::function<void(
            nautilus::val<TupleBuffer*> hashmap, nautilus::val<AbstractBufferProvider*>, nautilus::val<std::vector<RawValue>*>)>(
            [&keySchema, &valueSchema, &keys, &values](
                nautilus::val<TupleBuffer*> hashmap,
                nautilus::val<AbstractBufferProvider*> bufferProvider,
                nautilus::val<std::vector<RawValue>*> result)
            {
                HashMapRef ref(hashmap, keySchema, valueSchema);
                HashMapRef::Key key{.types = keySchema, .keys = toVarVals(keys)};
                HashMapRef::Value value{.types = valueSchema, .values = toVarVals(values)};
                ref.insert(key, value, bufferProvider);

                HashMapRef::Value retrieveValue{.types = valueSchema, .values = std::vector<VarVal>{}};
                if (ref.get(key, retrieveValue))
                {
                    for (nautilus::static_val<size_t> index = 0; index < values.size(); ++index)
                    {
                        append(result, retrieveValue.values[index]);
                    }
                }
            }));

    std::vector<RawValue> result;
    fn(std::addressof(map), bm.get(), &result);
    RC_ASSERT(result == values);
}

RC_GTEST_PROP(HashMapReference, InsertAndIterate, ())
{
    cpptrace::register_terminate_handler();
    nautilus::engine::Options options;
    options.setOption("engine.Compilation", true);
    options.setOption("dump.all", true);
    options.setOption("dump.file", true);
    nautilus::engine::NautilusEngine engine(options);

    auto bm = BufferManager::create();
    auto keySchema = *genSchemaStrict(128);
    auto valueSchema = *genSchemaStrict(256);

    auto map = HashMap::createHashMap(
        *bm,
        HashMap::Options{
            .keySize = static_cast<uint32_t>(keySchema.getSizeOfSchemaInBytes()),
            .valueSize = static_cast<uint32_t>(valueSchema.getSizeOfSchemaInBytes())});

    std::vector<std::pair<std::vector<RawValue>, std::vector<RawValue>>> inserts;
    auto numberOfElements = *rc::gen::inRange<size_t>(1, 1024);

    for (size_t i = 0; i < numberOfElements; ++i)
    {
        inserts.emplace_back(*genRawValuesForSchema(keySchema), *genRawValuesForSchema(valueSchema));
    }

    auto fn = engine.registerFunction(std::function<void(
                                          nautilus::val<TupleBuffer*> hashmap,
                                          nautilus::val<AbstractBufferProvider*>,
                                          nautilus::val<std::vector<std::vector<RawValue>>*>)>(
        [&keySchema, &valueSchema, &inserts](
            nautilus::val<TupleBuffer*> hashmap,
            nautilus::val<AbstractBufferProvider*> bufferProvider,
            nautilus::val<std::vector<std::vector<RawValue>>*> result)
        {
            HashMapRef ref(hashmap, keySchema, valueSchema);
            for (const auto& [key, value] : nautilus::static_iterable(inserts))
            {
                ref.insert(HashMapRef::Key{keySchema, toVarVals(key)}, HashMapRef::Value{valueSchema, toVarVals(value)}, bufferProvider);
            }


            nautilus::val<size_t> index = 0;
            for (const auto& [key, value] : ref)
            {
                for (const auto& var_val : value)
                {
                    append(result, var_val, index);
                }
                index = index + 1;
            }
        }));

    std::vector<std::vector<RawValue>> result;
    result.resize(numberOfElements);
    fn(std::addressof(map), bm.get(), &result);
    RC_ASSERT(result.size() > 0);
}

} // namespace NES