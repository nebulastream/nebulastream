//
// Created by ls on 11/30/24.
//

#include "RowLayout.hpp"
namespace NES::RowLayout
{
template <typename T>
struct MemoryLayout
{
};

#define ScalarTypeLayout(Physical) \
    template <> \
    struct MemoryLayout<Physical> \
    { \
        static NES::Nautilus::VarVal load(const Physical&, nautilus::val<int8_t*> address) \
        { \
            return Nautilus::ScalarVarVal(PhysicalNautilusMapping<Physical>::NautilusType{ \
                *static_cast<nautilus::val<typename PhysicalNautilusMapping<Physical>::NautilusType::raw_type*>>(address)}); \
        } \
        static void store(const Physical&, nautilus::val<int8_t*> address, NES::Nautilus::VarVal v) \
        { \
            *static_cast<nautilus::val<typename PhysicalNautilusMapping<Physical>::NautilusType::raw_type*>>(address) \
                = v.assumeScalar().cast<PhysicalNautilusMapping<Physical>::NautilusType>(); \
        } \
    }

ScalarTypeLayout(UInt8);
ScalarTypeLayout(Int8);
ScalarTypeLayout(Char);
ScalarTypeLayout(UInt64);

Nautilus::VarVal load(const PhysicalScalarType& pst, nautilus::val<int8_t*> address)
{
    return std::visit([&]<typename T>(const T& t) -> Nautilus::VarVal { return MemoryLayout<T>::load(t, address); }, pst.value);
}
void store(const PhysicalScalarType& pst, nautilus::val<int8_t*> address, Nautilus::VarVal v)
{
    std::visit([&]<typename T>(const T& t) { MemoryLayout<T>::store(t, address, v); }, pst.value);
}

template <>
struct MemoryLayout<Scalar>
{
    static NES::Nautilus::VarVal load(const Scalar& scalar, nautilus::val<int8_t*> address, TupleBufferRef)
    {
        return RowLayout::load(scalar.inner, address);
    }
    static void store(
        const Scalar& scalar,
        nautilus::val<int8_t*> address,
        NES::Nautilus::VarVal v,
        TupleBufferRef,
        nautilus::val<Memory::AbstractBufferProvider*>)
    {
        RowLayout::store(scalar.inner, address, v);
    }
};

template <>
struct MemoryLayout<FixedSize>
{
    static NES::Nautilus::VarVal load(const FixedSize& fixedSize, nautilus::val<int8_t*> address, TupleBufferRef)
    {
        std::vector<Nautilus::ScalarVarVal> values;
        values.reserve(fixedSize.numberOfElements);
        for (nautilus::static_val<size_t> i = 0; i < fixedSize.numberOfElements; i++)
        {
            values.emplace_back(RowLayout::load(fixedSize.inner, address).assumeScalar());
            address += fixedSize.inner.size();
        }
        return Nautilus::VarVal(Nautilus::FixedSizeVal(std::move(values)));
    }

    static void store(
        const FixedSize& fixedSize,
        nautilus::val<int8_t*> address,
        NES::Nautilus::VarVal v,
        TupleBufferRef,
        nautilus::val<Memory::AbstractBufferProvider*>)
    {
        auto& fixedSizeVal = v.assumeFixedSize();
        for (nautilus::static_val<size_t> i = 0; i < fixedSize.numberOfElements; i++)
        {
            RowLayout::store(fixedSize.inner, address, fixedSizeVal.values[i]);
            address += fixedSize.inner.size();
        }
    }
};

template <>
struct MemoryLayout<VariableSize>
{
    static NES::Nautilus::VarVal load(const VariableSize& type, nautilus::val<int8_t*> address, TupleBufferRef tb)
    {
        std::vector<Nautilus::ScalarVarVal> values;
        auto index = nautilus::val<unsigned>(*static_cast<nautilus::val<uint32_t*>>(address));
        auto [buffer, size] = tb.loadChildBuffer(type.inner, index);
        auto actualVariableSize = nautilus::val<unsigned>(*buffer.uncheckedCast<uint32_t>());
        return Nautilus::VarVal(
            Nautilus::VariableSizeVal(buffer.advanceBytes(4), actualVariableSize * type.inner.size(), actualVariableSize));
    }

    static void store(
        const VariableSize&,
        nautilus::val<int8_t*> address,
        NES::Nautilus::VarVal v,
        TupleBufferRef tb,
        nautilus::val<Memory::AbstractBufferProvider*> provider)
    {
        auto& variableSize = v.assumeVariableSize();
        auto [index, buffer, size] = tb.allocateChildBuffer<int8_t>(provider);
        *static_cast<nautilus::val<uint32_t*>>(address) = index;
        *static_cast<nautilus::val<uint32_t*>>(buffer) = variableSize.numberOfElements;
        buffer += 4;
        nautilus::memcpy(buffer, variableSize.data.cast<void>(), variableSize.size);
    }
};

void store(
    const PhysicalType& pst,
    nautilus::val<int8_t*> address,
    Nautilus::VarVal v,
    TupleBufferRef tb,
    nautilus::val<Memory::AbstractBufferProvider*> provider)
{
    std::visit([&]<typename T>(const T& t) { MemoryLayout<T>::store(t, address, v, tb, provider); }, pst.value);
}

Nautilus::VarVal load(const PhysicalType& pst, nautilus::val<int8_t*> address, TupleBufferRef tb)
{
    return std::visit([&]<typename T>(const T& t) -> Nautilus::VarVal { return MemoryLayout<T>::load(t, address, tb); }, pst.value);
}
}


NES::RowMemoryRecordAccessor::RowMemoryRecordAccessor(const PhysicalSchema& schema) : layout(schema), tb(std::nullopt)
{
}
NES::RowMemoryRecordAccessor::RowLayoutSchema::RowLayoutSchema(const PhysicalSchema& schema)
{
    tupleSize = 0;
    for (size_t index = 0; const auto& [fieldName, type] : schema.fields)
    {
        indexes[fieldName] = index++;
        tupleOffset.push_back(tupleSize);
        physicalTypes.push_back(type);
        tupleSize += type.size();
    }
}
NES::Nautilus::VarVal NES::RowMemoryRecordAccessor::operator[](const FieldName& fieldName)
{
    return operator[](layout.indexes[fieldName]);
}
NES::Nautilus::VarVal NES::RowMemoryRecordAccessor::operator[](size_t index)
{
    auto address = current + layout.tupleOffset[index];
    return RowLayout::load(layout.physicalTypes[index], address, *tb);
}
void NES::RowMemoryRecordAccessor::write(
    const FieldName& fieldName, Nautilus::VarVal v, nautilus::val<Memory::AbstractBufferProvider*> bufferProvider)
{
    write(layout.indexes[fieldName], v, bufferProvider);
}
void NES::RowMemoryRecordAccessor::write(size_t index, Nautilus::VarVal v, nautilus::val<Memory::AbstractBufferProvider*> bufferProvider)
{
    auto address = current + layout.tupleOffset[index];
    RowLayout::store(layout.physicalTypes[index], address, v, *tb, bufferProvider);
}
void NES::RowMemoryRecordAccessor::setBuffer(TupleBufferRef tb)
{
    this->tb = tb;
    this->base = this->tb->getBuffer<int8_t>();
    this->current = base;
}
void NES::RowMemoryRecordAccessor::advance(nautilus::val<size_t> index)
{
    current = base + index * layout.tupleSize;
}