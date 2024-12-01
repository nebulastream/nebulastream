//
// Created by ls on 12/1/24.
//

#include "CSVOutputFormatter.hpp"
#include <ranges>
namespace NES::CSV
{
template <typename T>
struct MemoryLayout
{
};

#define ScalarTypeLayout(Physical) \
    template <> \
    struct MemoryLayout<Physical> \
    { \
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

void store(const PhysicalScalarType& pst, nautilus::val<int8_t*> address, Nautilus::VarVal v)
{
    std::visit([&]<typename T>(const T& t) { MemoryLayout<T>::store(t, address, v); }, pst.value);
}

template <>
struct MemoryLayout<Scalar>
{
    static void store(
        const Scalar& scalar,
        nautilus::val<int8_t*> address,
        NES::Nautilus::VarVal v,
        TupleBufferRef,
        nautilus::val<Memory::AbstractBufferProvider*>)
    {
        CSV::store(scalar.inner, address, v);
    }
};

template <>
struct MemoryLayout<FixedSize>
{
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
            CSV::store(fixedSize.inner, address, fixedSizeVal.values[i]);
            address += fixedSize.inner.size();
        }
    }
};

template <>
struct MemoryLayout<VariableSize>
{
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
}

namespace NES
{

nautilus::val<bool> CSVOutputFormatter::append(TupleBufferRef output, std::vector<Nautilus::VarVal> fields)
{
    auto bytesLeft = output.getBufferSize() - output.getNumberOfTuples();
    auto buffer = Nautilus::ScalarVarValPtr(output.getBuffer<char>()).advanceBytes(output.getBufferSize());
    for (const auto& [value, field] : std::views::zip(fields, schema.fields))
    {
        CSV::store(field.first, buffer, value, bytesLeft);
    }
}

}