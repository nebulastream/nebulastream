//
// Created by ls on 11/30/24.
//

#include <cstddef>
#include <cstdint>
#include <unordered_set>
#include <variant>
#include <API/Schema.hpp>
#include <Runtime/BufferManager.hpp>
#include <Util/Logger/Logger.hpp>
#include <nautilus/Engine.hpp>
#include <nautilus/std/ostream.h>
#include <nautilus/std/sstream.h>
#include <nautilus/std/string.h>
#include <nautilus/val.hpp>
#include <nautilus/val_ptr.hpp>
#include <std/cstring.h>
#include <BaseUnitTest.hpp>
#include <ErrorHandling.hpp>
#include "CSVOutputFormatter.hpp"
#include "MemoryRecordAccessor.hpp"
#include "PhysicalToNautilusType.hpp"
#include "PhysicalTypes.hpp"
#include "RowLayout.hpp"
#include "TupleBufferRef.hpp"
#include "VarVal.hpp"

namespace NES
{

void fail()
{
    INVARIANT(false, "Something has failed");
}


class MemoryLayoutTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestCase()
    {
        Logger::setupLogging("MemoryLayoutTest.log", LogLevel::LOG_DEBUG);
        NES_INFO("Setup MemoryLayoutTest test class.");
    }
};

std::string generate_hexdump(const unsigned char* data, size_t length, size_t bytes_per_line = 16)
{
    std::stringstream hexdump;
    hexdump << std::hex << std::setfill('0');

    for (size_t i = 0; i < length; i += bytes_per_line)
    {
        // Print offset
        hexdump << std::setw(8) << i << ": ";

        // Print hex representation
        for (size_t j = 0; j < bytes_per_line; ++j)
        {
            if (i + j < length)
            {
                hexdump << std::setw(2) << static_cast<int>(data[i + j]) << " ";
            }
            else
            {
                hexdump << "   ";
            }
        }

        hexdump << " |";

        // Print ASCII representation
        for (size_t j = 0; j < bytes_per_line; ++j)
        {
            if (i + j < length)
            {
                unsigned char ch = data[i + j];
                hexdump << (std::isprint(ch) ? static_cast<char>(ch) : '.');
            }
        }

        hexdump << "|\n";
    }

    return hexdump.str();
}

namespace Util
{
template <typename T>
Nautilus::VarVal scalar(T t)
{
    return Nautilus::ScalarVarVal(nautilus::val<T>(t));
}

template <typename T>
Nautilus::VarVal fixedSize(std::initializer_list<T> ts)
{
    std::vector<Nautilus::ScalarVarVal> nautilusTs;
    for (const auto& t : ts)
    {
        nautilusTs.emplace_back(nautilus::val<T>(t));
    }
    return Nautilus::VarVal(Nautilus::FixedSizeVal(nautilusTs));
}

template <typename... T>
Nautilus::VarVal fixedSize(T... t)
{
    return fixedSize({t...});
}

template <typename T>
Nautilus::VarVal variableSize(T* t, size_t n)
{
    return Nautilus::VariableSizeVal(Nautilus::ScalarVarValPtr(nautilus::val<T*>(t)), n, n * sizeof(T));
}
}

TEST_F(MemoryLayoutTest, IsThisWorking)
{
    auto bm = Memory::BufferManager::create();
    PhysicalSchema schema{
        {std::make_pair<FieldName, PhysicalType>(FieldName("A"), PhysicalType{Scalar{PhysicalScalarType{UInt64{}}}}),
         std::make_pair<FieldName, PhysicalType>(FieldName("B"), PhysicalType{FixedSize{PhysicalScalarType{UInt8{}}, 4}})}};
    auto buffer = bm->getBufferBlocking();

    TupleBufferRef tb(std::addressof(buffer));
    MemoryAccessor accessor(std::make_unique<RowMemoryRecordAccessor>(schema), tb, schema);

    accessor[0].write(FieldName("A"), Util::scalar<uint64_t>(1), bm.get());
    accessor[0].write(FieldName("B"), Util::fixedSize<uint8_t>({1, 2, 3, 4}), bm.get());

    accessor[1].write(FieldName("A"), Util::scalar<uint64_t>(2), bm.get());
    accessor[2].write(FieldName("A"), Util::scalar<uint64_t>(3), bm.get());

    EXPECT_EQ(accessor[1][FieldName("A")], Util::scalar<uint64_t>(2));
    EXPECT_EQ(accessor[0][FieldName("B")].assumeFixedSize().values[2], Util::scalar<uint64_t>(3).assumeScalar());

    std::cout << generate_hexdump(buffer.getBuffer<unsigned char>(), buffer.getBufferSize());
}

TEST_F(MemoryLayoutTest, DoesItCompile)
{
    auto bm = Memory::BufferManager::create();
    PhysicalSchema schema{
        {std::make_pair<FieldName, PhysicalType>(FieldName("A"), PhysicalType{Scalar{PhysicalScalarType{UInt64{}}}}),
         std::make_pair<FieldName, PhysicalType>(FieldName("B"), PhysicalType{FixedSize{PhysicalScalarType{UInt8{}}, 4}}),
         std::make_pair<FieldName, PhysicalType>(FieldName("C"), PhysicalType{VariableSize{PhysicalScalarType{Char{}}}})}};


    nautilus::engine::Options options;
    options.setOption("engine.Compilation", true);
    nautilus::engine::NautilusEngine engine(options);
    auto fn = engine.registerFunction(std::function(
        [&schema](
            nautilus::val<NES::Memory::TupleBuffer*> buffer,
            nautilus::val<Memory::AbstractBufferProvider*> provider,
            nautilus::val<char*> c,
            nautilus::val<size_t> length)
        {
            TupleBufferRef tb(buffer);
            MemoryAccessor accessor(std::make_unique<RowMemoryRecordAccessor>(schema), tb, schema);
            Nautilus::VariableSizeVal var{c, length, length};

            auto arrayVal = Nautilus::FixedSizeVal{
                {nautilus::val<uint8_t>(1), nautilus::val<uint8_t>(2), nautilus::val<uint8_t>(3), nautilus::val<uint8_t>(4)}};

            accessor[0].write(FieldName("A"), nautilus::val<uint64_t>(1), provider);
            accessor[0].write(FieldName("B"), arrayVal, provider);
            accessor[0].write(FieldName("C"), var, provider);

            auto record = accessor.at(0);
            accessor.append(record, provider);
            accessor.append(record, provider);
            accessor.append(record, provider);

            accessor[1].write(FieldName("A"), nautilus::val<uint64_t>(2), provider);
            accessor[2].write(FieldName("A"), nautilus::val<uint64_t>(3), provider);


            if (accessor[2][FieldName("C")] == accessor[1][FieldName("C")])
            {
                nautilus::invoke(
                    +[](char* message) { NES_INFO("{}", std::string_view(message)); },
                    accessor[2][FieldName("C")].assumeVariableSize().data.cast<char>());
                return accessor[2][FieldName("C")].assumeVariableSize().data.cast<char>();
            }

            return accessor[0][FieldName("C")].assumeVariableSize().data.cast<char>();
        }));

    auto buffer = bm->getBufferBlocking();
    std::string s = "I am a long string";
    char* c = fn(std::addressof(buffer), bm.get(), s.data(), s.size() + 1);
    std::cout << c << std::endl;
    std::cout << generate_hexdump(buffer.getBuffer<unsigned char>(), buffer.getBufferSize());
}


TEST_F(MemoryLayoutTest, DoesItCompile2)
{
    using namespace Util;

    auto bm = Memory::BufferManager::create();
    PhysicalSchema inputSchema{
        {std::make_pair(FieldName("A"), PhysicalType{Scalar{PhysicalScalarType{UInt64{}}}}),
         std::make_pair(FieldName("B"), PhysicalType{FixedSize{PhysicalScalarType{UInt8{}}, 4}}),
         std::make_pair(FieldName("C"), PhysicalType{VariableSize{PhysicalScalarType{Char{}}}})}};

    PhysicalSchema outputSchema1{{std::make_pair(FieldName("B"), PhysicalType{FixedSize{PhysicalScalarType{UInt8{}}, 4}})}};

    PhysicalSchema outputSchema2{
        {std::make_pair(FieldName("A"), PhysicalType{Scalar{PhysicalScalarType{UInt64{}}}}),
         std::make_pair(FieldName("C"), PhysicalType{VariableSize{PhysicalScalarType{Char{}}}})}};

    nautilus::engine::Options options;
    options.setOption("engine.Compilation", true);
    nautilus::engine::NautilusEngine const engine(options);


    auto project = engine.registerFunction(std::function(
        [&inputSchema, &outputSchema1, &outputSchema2](
            nautilus::val<NES::Memory::TupleBuffer*> inputBuffer,
            nautilus::val<Memory::AbstractBufferProvider*> provider,
            nautilus::val<NES::Memory::TupleBuffer*> outputBuffer1,
            nautilus::val<NES::Memory::TupleBuffer*> outputBuffer2)
        {
            TupleBufferRef inputBufferRef(inputBuffer);
            MemoryAccessor inputAccessor(std::make_unique<RowMemoryRecordAccessor>(inputSchema), inputBufferRef, inputSchema);

            TupleBufferRef outputBufferRef1(outputBuffer1);
            MemoryAccessor outputAccessor1(std::make_unique<RowMemoryRecordAccessor>(outputSchema1), outputBufferRef1, outputSchema1);

            TupleBufferRef outputBufferRef2(outputBuffer2);
            MemoryAccessor outputAccessor2(std::make_unique<RowMemoryRecordAccessor>(outputSchema2), outputBufferRef2, outputSchema2);

            for (nautilus::val<size_t> i = 0; i < inputBufferRef.getNumberOfTuples(); i++)
            {
                auto record1 = inputAccessor.at(i, {FieldName("B")});
                auto record2 = inputAccessor.at(i, {FieldName("A"), FieldName("C")});
                outputAccessor1.append(record1, provider);
                outputAccessor2.append(record2, provider);
            }
        }));

    auto buffer = bm->getBufferBlocking();
    auto outputBuffer1 = bm->getBufferBlocking();
    auto outputBuffer2 = bm->getBufferBlocking();

    MemoryAccessor inputAccessor(
        std::make_unique<RowMemoryRecordAccessor>(inputSchema), TupleBufferRef(std::addressof(buffer)), inputSchema);

    std::string s = "I am a long string";
    inputAccessor.append({scalar<uint64_t>(1), fixedSize<uint8_t>({1, 2, 3, 4}), variableSize(s.data(), s.size() + 1)}, bm.get());
    inputAccessor.append({scalar<uint64_t>(2), fixedSize<uint8_t>({1, 2, 3, 4}), variableSize(s.data(), s.size() + 1)}, bm.get());
    inputAccessor.append({scalar<uint64_t>(3), fixedSize<uint8_t>({1, 2, 3, 4}), variableSize(s.data(), s.size() + 1)}, bm.get());

    project(std::addressof(buffer), bm.get(), std::addressof(outputBuffer1), std::addressof(outputBuffer2));

    std::cout << generate_hexdump(buffer.getBuffer<unsigned char>(), 64);
    std::cout << generate_hexdump(outputBuffer1.getBuffer<unsigned char>(), 64);
    std::cout << generate_hexdump(outputBuffer2.getBuffer<unsigned char>(), 64);
}
}