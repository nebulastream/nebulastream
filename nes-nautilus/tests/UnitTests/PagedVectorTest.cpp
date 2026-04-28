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

#include <any>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <ranges>
#include <string>
#include <utility>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <DataTypes/Schema.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/DataTypes/VariableSizedData.hpp>
#include <Nautilus/Interface/BufferRef/LowerSchemaProvider.hpp>
#include <Nautilus/Interface/BufferRef/TupleBufferRef.hpp>
#include <Nautilus/Interface/PagedVector/PagedVector.hpp>
#include <Nautilus/Interface/PagedVector/PagedVectorRef.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/BufferManager.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <gtest/gtest.h>
#include <magic_enum/magic_enum.hpp>
#include <nautilus/Engine.hpp>
#include <ErrorHandling.hpp>
#include <function.hpp>
#include <options.hpp>
#include <static.hpp>
#include <val.hpp>

#include <rapidcheck.h>
#include <Util/Ranges.hpp>

#include <fmt/ranges.h>
#include <rapidcheck/gtest.h>

namespace NES
{
namespace
{

enum class EngineMode
{
    Interpreter,
    Compiler
};

nautilus::engine::NautilusEngine makeEngine(EngineMode mode)
{
    nautilus::engine::Options options;
    options.setOption("engine.Compilation", mode == EngineMode::Compiler);
    options.setOption("mlir.enableMultithreading", false);
    return nautilus::engine::NautilusEngine(options);
}

struct DirtyBufferProvider : AbstractBufferProvider
{
    explicit DirtyBufferProvider(std::shared_ptr<BufferManager> bm) : bm(std::move(bm)) { }

    static std::shared_ptr<DirtyBufferProvider> create(size_t bufferSize = 4096, size_t numberOfBuffers = 1000)
    {
        return std::make_shared<DirtyBufferProvider>(BufferManager::create(bufferSize, numberOfBuffers));
    }

    BufferManagerType getBufferManagerType() const override { return bm->getBufferManagerType(); }

    size_t getBufferSize() const override { return bm->getBufferSize(); }

    size_t getNumOfPooledBuffers() const override { return bm->getNumOfPooledBuffers(); }

    size_t getNumOfUnpooledBuffers() const override { return bm->getNumOfUnpooledBuffers(); }

    TupleBuffer getBufferBlocking() override
    {
        auto buffer = bm->getBufferBlocking();
        for (uint32_t& available_memory_area : buffer.getAvailableMemoryArea<uint32_t>())
        {
            available_memory_area = 0xDEADBEEF;
        }
        return buffer;
    }

    std::optional<TupleBuffer> getBufferNoBlocking() override
    {
        auto buffer = bm->getBufferNoBlocking();
        if (buffer)
        {
            for (uint32_t& available_memory_area : buffer->getAvailableMemoryArea<uint32_t>())
            {
                available_memory_area = 0xDEADBEEF;
            }
        }
        return buffer;
    }

    std::optional<TupleBuffer> getBufferWithTimeout(std::chrono::milliseconds timeout_ms) override
    {
        auto buffer = bm->getBufferWithTimeout(timeout_ms);
        if (buffer)
        {
            for (uint32_t& available_memory_area : buffer->getAvailableMemoryArea<uint32_t>())
            {
                available_memory_area = 0xDEADBEEF;
            }
        }
        return buffer;
    }

    std::optional<TupleBuffer> getUnpooledBuffer(size_t bufferSize) override
    {
        auto buffer = bm->getUnpooledBuffer(bufferSize);
        if (buffer)
        {
            for (uint32_t& available_memory_area : buffer->getAvailableMemoryArea<uint32_t>())
            {
                available_memory_area = 0xDEADBEEF;
            }
        }
        return buffer;
    }

    std::shared_ptr<BufferManager> bm;
};

/// The buffer manager uses a fixed 4096-byte buffer size.
/// Schemas whose tuple size exceeds this must be discarded via RC_PRE.
constexpr uint64_t BUFFER_SIZE = 4096;

using AnyVec = std::vector<std::any>;

/// Value types used by the property generators (includes VARSIZED).
const std::vector<DataType::Type> ALL_VALUE_TYPES = {
    DataType::Type::UINT8,
    DataType::Type::UINT16,
    DataType::Type::UINT32,
    DataType::Type::UINT64,
    DataType::Type::INT8,
    DataType::Type::INT16,
    DataType::Type::INT32,
    DataType::Type::INT64,
    DataType::Type::FLOAT32,
    DataType::Type::FLOAT64,
    DataType::Type::VARSIZED,
};

/// Builds a Schema with sequentially named fields ("field0", "field1", ...) from the given DataType vector.
Schema createSchemaFromDataTypes(const std::vector<DataType>& dataTypes)
{
    auto schema = Schema{};

    for (const auto& [typeIdx, dataType] : views::enumerate(dataTypes))
    {
        const auto name = Record::RecordFieldIdentifier("field" + std::to_string(typeIdx));
        schema.addField(name, dataType);
    }
    return schema;
}

/// Generator for a non-empty vector of DataType drawn from a Type pool.
/// Nullability is randomised per field.
rc::Gen<std::vector<DataType>> genDataTypeSchema(std::vector<DataType::Type> pool, size_t minFields = 1, size_t maxFields = 256)
{
    return rc::gen::exec(
        [pool = std::move(pool), minFields, maxFields]()
        {
            const auto numFields = *rc::gen::inRange(minFields, maxFields + 1);
            std::vector<DataType> schema;
            schema.reserve(numFields);
            for (size_t i = 0; i < numFields; ++i)
            {
                const auto idx = *rc::gen::inRange(static_cast<size_t>(0), pool.size());
                const auto nullable = *rc::gen::arbitrary<bool>() ? DataType::NULLABLE::IS_NULLABLE : DataType::NULLABLE::NOT_NULLABLE;
                schema.emplace_back(pool[idx], nullable);
            }
            return schema;
        });
}

template <typename T>
std::any genScalarAny(bool nullable)
{
    if (nullable)
    {
        const bool isNull = *rc::gen::arbitrary<bool>();
        if (isNull)
        {
            return std::any{std::optional<T>{}};
        }
        return std::any{std::optional<T>{*rc::gen::arbitrary<T>()}};
    }
    return std::any{*rc::gen::arbitrary<T>()};
}

/// Generator for a record of arbitrary values matching the given field types.
rc::Gen<AnyVec> genAnyVec(std::vector<DataType> types)
{
    return rc::gen::exec(
        [types = std::move(types)]()
        {
            AnyVec result;
            result.reserve(types.size());
            for (const auto& dt : types)
            {
                switch (dt.type)
                {
                    case DataType::Type::UINT8:
                        result.push_back(genScalarAny<uint8_t>(dt.nullable));
                        break;
                    case DataType::Type::UINT16:
                        result.push_back(genScalarAny<uint16_t>(dt.nullable));
                        break;
                    case DataType::Type::UINT32:
                        result.push_back(genScalarAny<uint32_t>(dt.nullable));
                        break;
                    case DataType::Type::UINT64:
                        result.push_back(genScalarAny<uint64_t>(dt.nullable));
                        break;
                    case DataType::Type::INT8:
                        result.push_back(genScalarAny<int8_t>(dt.nullable));
                        break;
                    case DataType::Type::INT16:
                        result.push_back(genScalarAny<int16_t>(dt.nullable));
                        break;
                    case DataType::Type::INT32:
                        result.push_back(genScalarAny<int32_t>(dt.nullable));
                        break;
                    case DataType::Type::INT64:
                        result.push_back(genScalarAny<int64_t>(dt.nullable));
                        break;
                    case DataType::Type::FLOAT32:
                        result.push_back(genScalarAny<float>(dt.nullable));
                        break;
                    case DataType::Type::FLOAT64:
                        result.push_back(genScalarAny<double>(dt.nullable));
                        break;
                    case DataType::Type::VARSIZED: {
                        if (dt.nullable)
                        {
                            const bool isNull = *rc::gen::arbitrary<bool>();
                            if (isNull)
                            {
                                result.emplace_back(std::optional<std::string>{});
                                break;
                            }
                            auto str = *rc::gen::container<std::string>(rc::gen::inRange<char>(32, 127));
                            if (str.size() > 64)
                                str.resize(64);
                            result.emplace_back(std::optional<std::string>{std::move(str)});
                            break;
                        }
                        auto str = *rc::gen::container<std::string>(rc::gen::inRange<char>(32, 127));
                        if (str.size() > 64)
                            str.resize(64);
                        result.emplace_back(std::move(str));
                        break;
                    }
                    case DataType::Type::BOOLEAN:
                    case DataType::Type::CHAR:
                    case DataType::Type::UNDEFINED:
                        throw TestException("Unsupported type for genAnyVec");
                }
            }
            return result;
        });
}

uint64_t estimateSchemaSize(const std::vector<DataType>& types)
{
    return createSchemaFromDataTypes(types).getSizeOfSchemaInBytes();
}

template <typename T>
int compareTyped(const std::any& lhs, const std::any& rhs, const bool nullable)
{
    if (nullable)
    {
        const auto left = std::any_cast<std::optional<T>>(lhs);
        const auto right = std::any_cast<std::optional<T>>(rhs);
        if (left < right)
        {
            return -1;
        }
        if (left > right)
        {
            return 1;
        }
        return 0;
    }
    else
    {
        const auto left = std::any_cast<T>(lhs);
        const auto right = std::any_cast<T>(rhs);
        if (left < right)
        {
            return -1;
        }
        if (left > right)
        {
            return 1;
        }
        return 0;
    }
}

int compareAnyField(const std::any& lhs, const std::any& rhs, DataType type)
{
    switch (type.type)
    {
        case DataType::Type::UINT8:
            return compareTyped<uint8_t>(lhs, rhs, type.nullable);
        case DataType::Type::UINT16:
            return compareTyped<uint16_t>(lhs, rhs, type.nullable);
        case DataType::Type::UINT32:
            return compareTyped<uint32_t>(lhs, rhs, type.nullable);
        case DataType::Type::UINT64:
            return compareTyped<uint64_t>(lhs, rhs, type.nullable);
        case DataType::Type::INT8:
            return compareTyped<int8_t>(lhs, rhs, type.nullable);
        case DataType::Type::INT16:
            return compareTyped<int16_t>(lhs, rhs, type.nullable);
        case DataType::Type::INT32:
            return compareTyped<int32_t>(lhs, rhs, type.nullable);
        case DataType::Type::INT64:
            return compareTyped<int64_t>(lhs, rhs, type.nullable);
        case DataType::Type::FLOAT32:
            return compareTyped<float>(lhs, rhs, type.nullable);
        case DataType::Type::FLOAT64:
            return compareTyped<double>(lhs, rhs, type.nullable);
        case DataType::Type::VARSIZED:
            if (type.nullable)
            {
                const auto& left = std::any_cast<const std::optional<std::string>&>(lhs);
                const auto& right = std::any_cast<const std::optional<std::string>&>(rhs);
                if (left < right)
                    return -1;
                if (left > right)
                    return 1;
                return 0;
            }
            return std::any_cast<const std::string&>(lhs).compare(std::any_cast<const std::string&>(rhs));
        case DataType::Type::BOOLEAN:
        case DataType::Type::CHAR:
        case DataType::Type::UNDEFINED:
            throw TestException("Unsupported type for compareAnyField");
    }
    std::unreachable();
}

bool anyVecsEqual(const AnyVec& lhs, const AnyVec& rhs, const std::vector<DataType>& types)
{
    for (const auto& [l, r, dt] : std::views::zip(lhs, rhs, types))
    {
        if (compareAnyField(l, r, dt) != 0)
        {
            return false;
        }
    }
    return true;
}

/// Trace-time helpers: each template instantiation produces a distinct function pointer,
/// letting the compiled lambdas pull/push typed values through nautilus::invoke callbacks.
template <typename T>
nautilus::val<T> fetchScalarFromAnyVec(const nautilus::val<AnyVec*>& rec, uint64_t fieldIdx, bool nullable)
{
    if (nullable)
    {
        return nautilus::invoke(
            +[](AnyVec* a, uint64_t j) -> T
            {
                const auto& opt = std::any_cast<const std::optional<T>&>((*a)[j]);
                return opt.value_or(T{});
            },
            rec,
            nautilus::val<uint64_t>{fieldIdx});
    }
    return nautilus::invoke(+[](AnyVec* a, uint64_t j) -> T { return std::any_cast<T>((*a)[j]); }, rec, nautilus::val<uint64_t>{fieldIdx});
}

template <typename T>
void storeScalarToAnyVec(
    const nautilus::val<AnyVec*>& out, uint64_t fieldIdx, const nautilus::val<T>& value, bool nullable, const nautilus::val<bool>& isNull)
{
    if (nullable)
    {
        nautilus::invoke(
            +[](AnyVec* a, uint64_t j, T v, bool null) { (*a)[j] = null ? std::any{std::optional<T>{}} : std::any{std::optional<T>{v}}; },
            out,
            nautilus::val<uint64_t>{fieldIdx},
            value,
            isNull);
        return;
    }
    nautilus::invoke(+[](AnyVec* a, uint64_t j, T v) { (*a)[j] = std::any{v}; }, out, nautilus::val<uint64_t>{fieldIdx}, value);
}

void storeVarValToAnyVec(const nautilus::val<AnyVec*>& out, uint64_t i, const VarVal& value, const DataType& dt)
{
    switch (dt.type)
    {
        case DataType::Type::UINT8:
            storeScalarToAnyVec<uint8_t>(out, i, value.getRawValueAs<nautilus::val<uint8_t>>(), dt.nullable, value.isNull());
            break;
        case DataType::Type::UINT16:
            storeScalarToAnyVec<uint16_t>(out, i, value.getRawValueAs<nautilus::val<uint16_t>>(), dt.nullable, value.isNull());
            break;
        case DataType::Type::UINT32:
            storeScalarToAnyVec<uint32_t>(out, i, value.getRawValueAs<nautilus::val<uint32_t>>(), dt.nullable, value.isNull());
            break;
        case DataType::Type::UINT64:
            storeScalarToAnyVec<uint64_t>(out, i, value.getRawValueAs<nautilus::val<uint64_t>>(), dt.nullable, value.isNull());
            break;
        case DataType::Type::INT8:
            storeScalarToAnyVec<int8_t>(out, i, value.getRawValueAs<nautilus::val<int8_t>>(), dt.nullable, value.isNull());
            break;
        case DataType::Type::INT16:
            storeScalarToAnyVec<int16_t>(out, i, value.getRawValueAs<nautilus::val<int16_t>>(), dt.nullable, value.isNull());
            break;
        case DataType::Type::INT32:
            storeScalarToAnyVec<int32_t>(out, i, value.getRawValueAs<nautilus::val<int32_t>>(), dt.nullable, value.isNull());
            break;
        case DataType::Type::INT64:
            storeScalarToAnyVec<int64_t>(out, i, value.getRawValueAs<nautilus::val<int64_t>>(), dt.nullable, value.isNull());
            break;
        case DataType::Type::FLOAT32:
            storeScalarToAnyVec<float>(out, i, value.getRawValueAs<nautilus::val<float>>(), dt.nullable, value.isNull());
            break;
        case DataType::Type::FLOAT64:
            storeScalarToAnyVec<double>(out, i, value.getRawValueAs<nautilus::val<double>>(), dt.nullable, value.isNull());
            break;
        case DataType::Type::VARSIZED: {
            const auto vsd = value.getRawValueAs<VariableSizedData>();
            if (dt.nullable)
            {
                nautilus::invoke(
                    +[](AnyVec* a, uint64_t j, int8_t* ptr, uint64_t len, bool null)
                    {
                        (*a)[j] = null ? std::any{std::optional<std::string>{}}
                                       : std::any{std::optional<std::string>{std::string(reinterpret_cast<const char*>(ptr), len)}};
                    },
                    out,
                    nautilus::val<uint64_t>{i},
                    vsd.getContent(),
                    vsd.getSize(),
                    value.isNull());
                break;
            }
            nautilus::invoke(
                +[](AnyVec* a, uint64_t j, int8_t* ptr, uint64_t len)
                { (*a)[j] = std::any{std::string(reinterpret_cast<const char*>(ptr), len)}; },
                out,
                nautilus::val<uint64_t>{i},
                vsd.getContent(),
                vsd.getSize());
            break;
        }
        case DataType::Type::BOOLEAN:
        case DataType::Type::CHAR:
        case DataType::Type::UNDEFINED:
            throw TestException("Unsupported type for TestablePagedVector");
    }
}

nautilus::val<bool> checkIfNullInAnyVec(const nautilus::val<AnyVec*>& rec, uint64_t fieldIdx, DataType::Type type)
{
    return nautilus::invoke(
        +[](AnyVec* a, uint64_t j, DataType::Type t) -> bool
        {
            const auto& entry = (*a)[j];
            switch (t)
            {
                case DataType::Type::UINT8:
                    return !std::any_cast<std::optional<uint8_t>>(entry).has_value();
                case DataType::Type::UINT16:
                    return !std::any_cast<std::optional<uint16_t>>(entry).has_value();
                case DataType::Type::UINT32:
                    return !std::any_cast<std::optional<uint32_t>>(entry).has_value();
                case DataType::Type::UINT64:
                    return !std::any_cast<std::optional<uint64_t>>(entry).has_value();
                case DataType::Type::INT8:
                    return !std::any_cast<std::optional<int8_t>>(entry).has_value();
                case DataType::Type::INT16:
                    return !std::any_cast<std::optional<int16_t>>(entry).has_value();
                case DataType::Type::INT32:
                    return !std::any_cast<std::optional<int32_t>>(entry).has_value();
                case DataType::Type::INT64:
                    return !std::any_cast<std::optional<int64_t>>(entry).has_value();
                case DataType::Type::FLOAT32:
                    return !std::any_cast<std::optional<float>>(entry).has_value();
                case DataType::Type::FLOAT64:
                    return !std::any_cast<std::optional<double>>(entry).has_value();
                case DataType::Type::VARSIZED:
                    return !std::any_cast<std::optional<std::string>>(entry).has_value();
                default:
                    std::unreachable();
            }
        },
        rec,
        nautilus::val<uint64_t>{fieldIdx},
        nautilus::val<DataType::Type>{type});
}

nautilus::val<AnyVec*> anyVecPushBack(const nautilus::val<std::vector<AnyVec>*>& vec, nautilus::val<size_t> numberOfFields)
{
    return nautilus::invoke(
        +[](std::vector<AnyVec>* a, size_t numberOfFields)
        {
            a->emplace_back(AnyVec(numberOfFields, 0));
            return &a->back();
        },
        vec,
        numberOfFields);
}

VarVal buildVarVal(const nautilus::val<AnyVec*>& rec, uint64_t fieldIdx, DataType dt)
{
    const nautilus::val<bool> isNull = dt.nullable ? checkIfNullInAnyVec(rec, fieldIdx, dt.type) : nautilus::val<bool>{false};
    switch (dt.type)
    {
        case DataType::Type::UINT8:
            return VarVal(fetchScalarFromAnyVec<uint8_t>(rec, fieldIdx, dt.nullable), dt.nullable, isNull);
        case DataType::Type::UINT16:
            return VarVal(fetchScalarFromAnyVec<uint16_t>(rec, fieldIdx, dt.nullable), dt.nullable, isNull);
        case DataType::Type::UINT32:
            return VarVal(fetchScalarFromAnyVec<uint32_t>(rec, fieldIdx, dt.nullable), dt.nullable, isNull);
        case DataType::Type::UINT64:
            return VarVal(fetchScalarFromAnyVec<uint64_t>(rec, fieldIdx, dt.nullable), dt.nullable, isNull);
        case DataType::Type::INT8:
            return VarVal(fetchScalarFromAnyVec<int8_t>(rec, fieldIdx, dt.nullable), dt.nullable, isNull);
        case DataType::Type::INT16:
            return VarVal(fetchScalarFromAnyVec<int16_t>(rec, fieldIdx, dt.nullable), dt.nullable, isNull);
        case DataType::Type::INT32:
            return VarVal(fetchScalarFromAnyVec<int32_t>(rec, fieldIdx, dt.nullable), dt.nullable, isNull);
        case DataType::Type::INT64:
            return VarVal(fetchScalarFromAnyVec<int64_t>(rec, fieldIdx, dt.nullable), dt.nullable, isNull);
        case DataType::Type::FLOAT32:
            return VarVal(fetchScalarFromAnyVec<float>(rec, fieldIdx, dt.nullable), dt.nullable, isNull);
        case DataType::Type::FLOAT64:
            return VarVal(fetchScalarFromAnyVec<double>(rec, fieldIdx, dt.nullable), dt.nullable, isNull);
        case DataType::Type::VARSIZED: {
            auto ptr = nautilus::invoke(
                +[](AnyVec* a, uint64_t j, bool nullable) -> int8_t*
                {
                    if (nullable)
                    {
                        const auto& opt = std::any_cast<const std::optional<std::string>&>((*a)[j]);
                        if (!opt.has_value())
                            return nullptr;
                        return const_cast<int8_t*>(reinterpret_cast<const int8_t*>(opt->data()));
                    }
                    const auto& s = std::any_cast<const std::string&>((*a)[j]);
                    return const_cast<int8_t*>(reinterpret_cast<const int8_t*>(s.data()));
                },
                rec,
                nautilus::val<uint64_t>{fieldIdx},
                nautilus::val<bool>{dt.nullable});
            auto len = nautilus::invoke(
                +[](AnyVec* a, uint64_t j, bool nullable) -> uint64_t
                {
                    if (nullable)
                    {
                        const auto& opt = std::any_cast<const std::optional<std::string>&>((*a)[j]);
                        return opt.has_value() ? opt->size() : 0;
                    }
                    return std::any_cast<const std::string&>((*a)[j]).size();
                },
                rec,
                nautilus::val<uint64_t>{fieldIdx},
                nautilus::val<bool>{dt.nullable});
            return VarVal(VariableSizedData(ptr, len), dt.nullable, isNull);
        }
        case DataType::Type::BOOLEAN:
        case DataType::Type::CHAR:
        case DataType::Type::UNDEFINED:
            break;
    }
    throw TestException("Unsupported type for TestablePagedVector");
}

/// Mirrors std::vector<AnyVec> but internally operates on a real PagedVector.
/// insert and readAt are Nautilus-compiled and invoked via function-pointer dispatch.
class TestablePagedVector
{
public:
    TestablePagedVector(
        const std::vector<DataType>& fieldTypes, AbstractBufferProvider& bufferManager, EngineMode mode = EngineMode::Compiler)
        : dataTypes_(fieldTypes), bufferManager_(bufferManager)
    {
        const auto schema = createSchemaFromDataTypes(dataTypes_);
        projections_ = schema.getFieldNames();
        auto layout = std::make_shared<BasicTupleLayout>(schema);

        nautilus::engine::Options options;
        options.setOption("engine.Compilation", true);
        options.setOption("mlir.enableMultithreading", false);
        engine_ = std::make_unique<nautilus::engine::NautilusEngine>(makeEngine(mode));

        pagedVector_ = bufferManager.getUnpooledBuffer(PagedVector::getMainBufferSize()).value();
        PagedVector::init(pagedVector_, bufferManager.getBufferSize(), layout->getTupleSize());

        pushbackFn_.emplace(engine_->registerFunction(std::function(
            [layout, dataTypes = dataTypes_, projections = projections_](
                nautilus::val<TupleBuffer*> pv, nautilus::val<AbstractBufferProvider*> bm, nautilus::val<AnyVec*> rec)
            {
                Record record;
                /// static_val ensures each field iteration gets a distinct trace tag.
                for (nautilus::static_val<uint64_t> i = 0; i < dataTypes.size(); ++i)
                {
                    record.write(projections[i], buildVarVal(rec, i, dataTypes[i]));
                }
                PagedVectorRef pvRef(NautilusBorrowedBuffer::load(pv), layout);
                pvRef.push_back(record, bm);
            })));

        readAtFn_.emplace(engine_->registerFunction(std::function(
            [layout, dataTypes = dataTypes_, projections = projections_](
                nautilus::val<TupleBuffer*> pv, nautilus::val<uint64_t> index, nautilus::val<AnyVec*> out)
            {
                const PagedVectorRef pvRef(NautilusBorrowedBuffer::load(pv), layout);
                auto record = pvRef.at(index);
                for (nautilus::static_val<uint64_t> i = 0; i < dataTypes.size(); ++i)
                {
                    storeVarValToAnyVec(out, i, record.read(projections[i]), dataTypes[i]);
                }
            })));

        readAll_.emplace(engine_->registerFunction(std::function(
            [layout, dataTypes = dataTypes_, projections = projections_](
                nautilus::val<TupleBuffer*> pv, nautilus::val<std::vector<AnyVec>*> outVec)
            {
                const PagedVectorRef pvRef(NautilusBorrowedBuffer::load(pv), layout);
                for (const auto& record : pvRef)
                {
                    auto out = anyVecPushBack(outVec, nautilus::val<size_t>(layout->getNumberOfFields()));
                    for (nautilus::static_val<uint64_t> i = 0; i < dataTypes.size(); ++i)
                    {
                        storeVarValToAnyVec(out, i, record.read(projections[i]), dataTypes[i]);
                    }
                }
            })));
    }

    TestablePagedVector(const TestablePagedVector&) = delete;
    TestablePagedVector& operator=(const TestablePagedVector&) = delete;
    TestablePagedVector(TestablePagedVector&&) = default;
    TestablePagedVector& operator=(TestablePagedVector&&) = delete;

    void push_back(const AnyVec& record) { (*pushbackFn_)(&pagedVector_, &bufferManager_, const_cast<AnyVec*>(&record)); }

    AnyVec readAt(uint64_t index)
    {
        AnyVec out(dataTypes_.size());
        (*readAtFn_)(&pagedVector_, index, &out);
        return out;
    }

    std::vector<AnyVec> toVector()
    {
        const auto numEntries = PagedVector::load(pagedVector_).getTotalNumberOfRecords();
        std::vector<AnyVec> out;
        out.reserve(numEntries);
        (*readAll_)(&pagedVector_, &out);
        return out;
    }

    void concatMove(TestablePagedVector& other)
    {
        auto otherPagedVector = PagedVector::load(other.pagedVector_);
        PagedVector::load(pagedVector_).movePagesFrom(otherPagedVector);
    }

    void concatCopy(TestablePagedVector& other)
    {
        auto otherPagedVector = PagedVector::load(other.pagedVector_);
        PagedVector::load(pagedVector_).copyPagesFrom(bufferManager_, otherPagedVector);
    }

    uint64_t size() const { return PagedVector::load(pagedVector_).getTotalNumberOfRecords(); }

    PagedVector raw() { return PagedVector::load(pagedVector_); }

private:
    std::vector<DataType> dataTypes_;
    TupleBuffer pagedVector_;
    AbstractBufferProvider& bufferManager_;
    std::vector<Record::RecordFieldIdentifier> projections_;
    std::unique_ptr<nautilus::engine::NautilusEngine> engine_;
    std::optional<nautilus::engine::CallableFunction<void, TupleBuffer*, AbstractBufferProvider*, AnyVec*>> pushbackFn_;
    std::optional<nautilus::engine::CallableFunction<void, TupleBuffer*, uint64_t, AnyVec*>> readAtFn_;
    std::optional<nautilus::engine::CallableFunction<void, TupleBuffer*, std::vector<AnyVec>*>> readAll_;
};

/// Insert N items into a PagedVector, then iterate and compare against reference.
void insertAndIterateProperty(EngineMode mode)
{
    const auto fieldTypes = *genDataTypeSchema(ALL_VALUE_TYPES, 1, 128);
    RC_PRE(estimateSchemaSize(fieldTypes) < BUFFER_SIZE);

    const auto numberOfItems = *rc::gen::inRange<uint64_t>(0, 501);

    NES_INFO("Property insertAndIterate: fields={}, N={}, field_types={}", fieldTypes.size(), numberOfItems, fmt::join(fieldTypes, ", "));

    auto bufferManager = DirtyBufferProvider::create();
    TestablePagedVector pv(fieldTypes, *bufferManager, mode);

    std::vector<AnyVec> reference;
    reference.reserve(numberOfItems);
    for (uint64_t i = 0; i < numberOfItems; ++i)
    {
        auto record = *genAnyVec(fieldTypes);
        reference.push_back(record);
        pv.push_back(record);
    }

    NES_INFO("insertAndIterate: PagedVector has {} entries", pv.size());
    RC_ASSERT(pv.size() == reference.size());

    auto actual = pv.toVector();
    RC_ASSERT(actual.size() == reference.size());
    for (size_t i = 0; i < actual.size(); ++i)
    {
        RC_ASSERT(anyVecsEqual(actual[i], reference[i], fieldTypes));
    }
}

/// Insert N items into a PagedVector, then read each by index and compare against reference.
void insertAndReadByIndexProperty(EngineMode mode)
{
    const auto fieldTypes = *genDataTypeSchema(ALL_VALUE_TYPES, 1, 128);
    RC_PRE(estimateSchemaSize(fieldTypes) < BUFFER_SIZE);

    const auto numberOfItems = *rc::gen::inRange<uint64_t>(0, 501);

    NES_INFO("Property insertAndReadByIndex: fields={}, N={}", fieldTypes.size(), numberOfItems);

    auto bufferManager = DirtyBufferProvider::create();
    TestablePagedVector pv(fieldTypes, *bufferManager, mode);

    std::vector<AnyVec> reference;
    reference.reserve(numberOfItems);
    for (uint64_t i = 0; i < numberOfItems; ++i)
    {
        auto record = *genAnyVec(fieldTypes);
        reference.push_back(record);
        pv.push_back(record);
    }

    RC_ASSERT(pv.size() == reference.size());

    for (uint64_t idx = 0; idx < reference.size(); ++idx)
    {
        auto actual = pv.readAt(idx);
        RC_ASSERT(anyVecsEqual(actual, reference[idx], fieldTypes));
    }
}

/// Create K PagedVectors, insert items, concatMove via movePagesFrom, verify against concatenated reference.
void concatMoveProperty(EngineMode mode)
{
    const auto fieldTypes = *genDataTypeSchema(ALL_VALUE_TYPES, 1, 128);
    RC_PRE(estimateSchemaSize(fieldTypes) < BUFFER_SIZE);

    const auto numVectors = *rc::gen::inRange<uint64_t>(1, 5);

    NES_INFO("Property concatMove: fields={}, numVectors={}", fieldTypes.size(), numVectors);

    auto bufferManager = DirtyBufferProvider::create();

    std::vector<TestablePagedVector> pagedVectors;
    std::vector<AnyVec> fullReference;

    for (uint64_t v = 0; v < numVectors; ++v)
    {
        const auto itemCount = *rc::gen::inRange<uint64_t>(0, 201);
        pagedVectors.emplace_back(fieldTypes, *bufferManager, mode);
        for (uint64_t i = 0; i < itemCount; ++i)
        {
            auto record = *genAnyVec(fieldTypes);
            fullReference.push_back(record);
            pagedVectors.back().push_back(record);
        }
        NES_INFO("concatMove: vector {} has {} entries", v, pagedVectors.back().size());
    }

    for (uint64_t v = 1; v < numVectors; ++v)
    {
        pagedVectors[0].concatMove(pagedVectors[v]);
        RC_ASSERT(pagedVectors[v].raw().getStatus() == PagedVector::INVALID_PV);
    }

    NES_INFO("concatMove: merged vector has {} entries, reference has {} entries", pagedVectors[0].size(), fullReference.size());
    RC_ASSERT(pagedVectors[0].size() == fullReference.size());

    auto actual = pagedVectors[0].toVector();
    RC_ASSERT(actual.size() == fullReference.size());
    for (size_t i = 0; i < actual.size(); ++i)
    {
        RC_ASSERT(anyVecsEqual(actual[i], fullReference[i], fieldTypes));
    }
}

void concatCopyProperty(EngineMode mode)
{
    const auto fieldTypes = *genDataTypeSchema(ALL_VALUE_TYPES, 1, 128);
    RC_PRE(estimateSchemaSize(fieldTypes) < BUFFER_SIZE);
    const auto numVectors = *rc::gen::inRange<uint64_t>(1, 5);
    NES_INFO("Property concatCopy: fields={}, numVectors={}", fieldTypes.size(), numVectors);
    auto bufferManager = DirtyBufferProvider::create();
    std::vector<TestablePagedVector> pagedVectors;
    std::vector<AnyVec> fullReference;

    for (uint64_t v = 0; v < numVectors; ++v)
    {
        const auto itemCount = *rc::gen::inRange<uint64_t>(0, 201);
        pagedVectors.emplace_back(fieldTypes, *bufferManager, mode);
        for (uint64_t i = 0; i < itemCount; ++i)
        {
            auto record = *genAnyVec(fieldTypes);
            fullReference.push_back(record);
            pagedVectors.back().push_back(record);
        }
        NES_INFO("concatCopy: vector {} has {} entries", v, pagedVectors.back().size());
    }

    /// Snapshot each source vector's contents before any copy so we can verify that copyPagesFrom does not mutate or share memory with the sources.
    std::vector<std::vector<AnyVec>> sourceSnapshots;
    sourceSnapshots.reserve(numVectors);
    for (uint64_t v = 0; v < numVectors; ++v)
        sourceSnapshots.push_back(pagedVectors[v].toVector());

    for (uint64_t v = 1; v < numVectors; ++v)
    {
        auto numPagesPrevOther = pagedVectors[v].raw().getNumberOfPages();
        auto numTuplesPrevOther = pagedVectors[v].size();
        auto numPagesPrevFirst = pagedVectors[0].raw().getNumberOfPages();
        auto numTuplesPrevFirst = pagedVectors[0].size();
        pagedVectors[0].concatCopy(pagedVectors[v]);
        RC_ASSERT(pagedVectors[v].raw().getStatus() == PagedVector::VALID_PV);
        RC_ASSERT(pagedVectors[v].raw().getNumberOfPages() == numPagesPrevOther);
        RC_ASSERT(pagedVectors[v].size() == numTuplesPrevOther);
        RC_ASSERT(pagedVectors[0].raw().getNumberOfPages() == numPagesPrevFirst + numPagesPrevOther);
        RC_ASSERT(pagedVectors[0].size() == numTuplesPrevFirst + numTuplesPrevOther);

        /// Verify the source's records are intact after being copied into the destination, catching any accidental memory sharing between source and destination pages.
        auto sourceActual = pagedVectors[v].toVector();
        RC_ASSERT(sourceActual.size() == sourceSnapshots[v].size());
        for (size_t i = 0; i < sourceActual.size(); ++i)
        {
            RC_ASSERT(anyVecsEqual(sourceActual[i], sourceSnapshots[v][i], fieldTypes));
        }
    }

    NES_INFO("concatCopy: merged vector has {} entries, reference has {} entries", pagedVectors[0].size(), fullReference.size());
    RC_ASSERT(pagedVectors[0].size() == fullReference.size());
    auto actual = pagedVectors[0].toVector();
    RC_ASSERT(actual.size() == fullReference.size());
    for (size_t i = 0; i < actual.size(); ++i)
    {
        RC_ASSERT(anyVecsEqual(actual[i], fullReference[i], fieldTypes));
    }
}


} /// anonymous namespace

RC_GTEST_PROP(PagedVectorPropertyTest, insertAndIterate, ())
{
    Logger::setupLogging("PagedVectorPropertyTest.log", LogLevel::LOG_DEBUG);
    insertAndIterateProperty(EngineMode::Compiler);
    insertAndIterateProperty(EngineMode::Interpreter);
}

RC_GTEST_PROP(PagedVectorPropertyTest, insertAndReadByIndex, ())
{
    Logger::setupLogging("PagedVectorPropertyTest.log", LogLevel::LOG_DEBUG);
    insertAndReadByIndexProperty(EngineMode::Compiler);
    insertAndReadByIndexProperty(EngineMode::Interpreter);
}

RC_GTEST_PROP(PagedVectorPropertyTest, concatMovePagedVector, ())
{
    Logger::setupLogging("PagedVectorPropertyTest.log", LogLevel::LOG_DEBUG);
    concatMoveProperty(EngineMode::Compiler);
    concatMoveProperty(EngineMode::Interpreter);
}

RC_GTEST_PROP(PagedVectorPropertyTest, concatCopyPagedVector, ())
{
    Logger::setupLogging("PagedVectorPropertyTest.log", LogLevel::LOG_DEBUG);
    concatCopyProperty(EngineMode::Compiler);
    concatCopyProperty(EngineMode::Interpreter);
}

}
