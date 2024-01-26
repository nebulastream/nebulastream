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

#include <API/Schema.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/MemoryLayout/DynamicTupleBuffer.hpp>
#include <Runtime/MemoryLayout/RowLayout.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <SchemaBuffer.hpp>

using NESSchema = NES::Schema;

template<typename... T>
using Schema = NES::Unikernel::Schema<T...>;
template<typename T>
using Field = NES::Unikernel::Field<T>;
using Schema1 = Schema<Field<UINT8>, Field<FLOAT32>, Field<TEXT>, Field<UINT32>, Field<INT64>>;

NES::Runtime::BufferManagerPtr TheBufferManager = nullptr;
int main() {
    TheBufferManager = std::make_shared<NES::Runtime::BufferManager>(8192);
    auto tb = TheBufferManager->getBufferBlocking();
    auto buffer = NES::Unikernel::SchemaBuffer<Schema1, 8192>::of(tb);

    buffer.writeTuple(std::make_tuple(256, 42.4f, "This is a var length Text", 32, -64));
    buffer.writeTuple(std::make_tuple(28, -42.4f, "This is a shorter Text", UINT16_MAX + 1, INT64_MIN));

    auto schema = NESSchema::create(NES::Schema::MemoryLayoutType::ROW_LAYOUT)
                      ->addField("uint8", NES::BasicType::UINT8)
                      ->addField("float32", NES::BasicType::FLOAT32)
                      ->addField("text", NES::BasicType::TEXT)
                      ->addField("uint32", NES::BasicType::UINT32)
                      ->addField("int64", NES::BasicType::INT64);

    auto dbuffer = NES::Runtime::MemoryLayouts::DynamicTupleBuffer::createDynamicTupleBuffer(tb, schema);

    NES_ASSERT(dbuffer.getNumberOfTuples() == 2, "Expected 2 tuples");
    NES_ASSERT(dbuffer.getBuffer().getNumberOfChildrenBuffer() == 2, "Expected two child buffer");

    NES_ASSERT(dbuffer[0]["uint8"].read<uint8_t>() == 0, "Expected 256 to be truncated to 0");
    NES_ASSERT(dbuffer[0]["float32"].read<float>() == 42.4f, "float is incorrect");
    NES_ASSERT(dbuffer[0]["text"].read<uint32_t>() == 0, "expected text to be at child buffer 0");
    NES_ASSERT(*dbuffer.getBuffer().loadChildBuffer(0).getBuffer<uint32_t>() == strlen("This is a var length Text"),
               "Text values does not have propper size");
    NES_ASSERT(
        std::string_view((dbuffer.getBuffer().loadChildBuffer(0).getBuffer<char>() + 4), strlen("This is a var length Text"))
            == std::string("This is a var length Text"),
        "Text value does not match");
    NES_ASSERT(dbuffer[0]["uint32"].read<uint32_t>() == 32, "u32 is incorrect");
    NES_ASSERT(dbuffer[0]["int64"].read<int64_t>() == -64, "i64 is incorrect");

    NES_ASSERT(dbuffer[1]["uint8"].read<uint8_t>() == 28, "Expected 256 to be truncated to 0");
    NES_ASSERT(dbuffer[1]["float32"].read<float>() == -42.4f, "float is incorrect");
    NES_ASSERT(dbuffer[1]["text"].read<uint32_t>() == 1, "expected text to be at child buffer 0");
    NES_ASSERT(*dbuffer.getBuffer().loadChildBuffer(1).getBuffer<uint32_t>() == strlen("This is a shorter Text"),
               "Text values does not have propper size");
    NES_ASSERT(std::string_view((dbuffer.getBuffer().loadChildBuffer(1).getBuffer<char>() + 4), strlen("This is a shorter Text"))
                   == std::string("This is a shorter Text"),
               "Text value does not match");
    NES_ASSERT(dbuffer[1]["uint32"].read<uint32_t>() == UINT16_MAX + 1, "u32 is incorrect");
    NES_ASSERT(dbuffer[1]["int64"].read<int64_t>() == INT64_MIN, "i64 is incorrect");

    std::apply(
        [](uint8_t u8, float f32, std::string text, uint32_t u32, int64_t i64) {
            NES_ASSERT(u8 == 0, "u8 incorrect");
            NES_ASSERT(f32 == 42.4f, "float incorrect");
            NES_ASSERT(text == "This is a var length Text", "text incorrect");
            NES_ASSERT(u32 == 32, "u32 incorrect");
            NES_ASSERT(i64 == -64, "u64 incorrect");
        },
        buffer.readTuple(0));
}