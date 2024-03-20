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

#ifndef NES_NES_RUNTIME_TESTS_INCLUDE_TPCH_TABLE_HPP_
#define NES_NES_RUNTIME_TESTS_INCLUDE_TPCH_TABLE_HPP_
#include <Runtime/BufferManager.hpp>
#include <Runtime/MemoryLayout/MemoryLayout.hpp>
#include <Util/TestTupleBuffer.hpp>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <utility>
namespace NES::Runtime {

class TableBuilder;

/**
 * @brief This provides a table abstraction for testing, revisit this if we have a similar abstraction in the runtime.
 * A table stores a set of tuple buffers (chunks).
 */
class Table {
  public:
    Table(MemoryLayouts::MemoryLayoutPtr layout) : layout(std::move(layout)){};

    std::vector<TupleBuffer>& getChunks() { return chunks; }
    MemoryLayouts::MemoryLayoutPtr& getLayout() { return layout; }

    static void store(std::unique_ptr<Table>& table, std::string tableDir) {
        for (size_t i = 0; i < table->chunks.size(); i++) {
            auto& chunk = table->chunks[i];
            std::ofstream outputFile;
            auto chunkPath = fmt::format("{}/chunk_{}.nes", tableDir, i);
            outputFile.open(chunkPath, std::ios::out | std::ofstream::binary);
            auto numberOfTuple = chunk.getNumberOfTuples();
            outputFile.write((char*) (&numberOfTuple), sizeof(numberOfTuple));
            outputFile.write(chunk.getBuffer<char>(), (size_t) chunk.getBufferSize());
            outputFile.flush();
            outputFile.close();
        }
    }

    static std::unique_ptr<Table>
    load(const std::string& tableDir, const Runtime::BufferManagerPtr& bm, const MemoryLayouts::MemoryLayoutPtr& layout) {
        auto table = std::make_unique<Table>(layout);
        for (const auto& chunkFile : std::filesystem::directory_iterator(tableDir)) {
            if (!chunkFile.is_regular_file()) {
                continue;
            }

            auto buffer = bm->getBufferNoBlocking();
            if (!buffer.has_value()) {
                NES_THROW_RUNTIME_ERROR("BufferManager is out of buffers");
            }
            auto& chunk = table->addChunk(buffer.value());
            std::ifstream input(chunkFile.path(), std::ios::binary | std::ios::ate);
            auto chunkFileSize = (unsigned) input.tellg();
            NES_ASSERT(chunkFileSize <= chunk.getBufferSize() + sizeof(uint64_t),
                       "The chunk file cant be lager then the allocated buffer");
            input.seekg(0);
            uint64_t numberOfTuple;
            input.read((char*) &numberOfTuple, sizeof(numberOfTuple));
            input.read(chunk.getBuffer<char>(), chunkFileSize);
            chunk.setNumberOfTuples(numberOfTuple);
        }
        return table;
    }

  private:
    friend TableBuilder;
    TupleBuffer& addChunk(TupleBuffer& buffer) { return chunks.emplace_back(buffer); }
    MemoryLayouts::MemoryLayoutPtr layout;
    std::vector<TupleBuffer> chunks;
};

/**
 * @brief Builder to create tables.
 */
class TableBuilder {
  public:
    TableBuilder(Runtime::BufferManagerPtr bm, const MemoryLayouts::MemoryLayoutPtr& layout)
        : bm(std::move(bm)), table(std::make_unique<Table>(layout)) {
        appendBuffer();
    }

    std::unique_ptr<Table> finishTable() { return std::move(table); }

    template<typename... Types>
    void append(std::tuple<Types...> values) {

        uint64_t fieldIndex = 0;
        auto numberOfRecords = currentBuffer->getNumberOfTuples();
        if (numberOfRecords == currentBuffer->getCapacity()) {
            appendBuffer();
            numberOfRecords = currentBuffer->getNumberOfTuples();
        }
        auto recordReference = (*currentBuffer)[numberOfRecords];
        std::apply(
            [&](auto&&... fieldValue) {
                (recordReference[fieldIndex++].write(fieldValue), ...);
            },
            values);
        currentBuffer->setNumberOfTuples(currentBuffer->getNumberOfTuples() + 1);
    };

  private:
    void appendBuffer() {
        auto buffer = bm->getBufferNoBlocking();
        if (!buffer.has_value()) {
            NES_THROW_RUNTIME_ERROR("BufferManager is out of buffers");
        }
        auto& chunk = table->addChunk(buffer.value());
        currentBuffer = std::make_unique<MemoryLayouts::TestTupleBuffer>(table->layout, chunk);
    }

  private:
    Runtime::BufferManagerPtr bm;
    std::unique_ptr<Table> table;
    std::unique_ptr<MemoryLayouts::TestTupleBuffer> currentBuffer;
};

}// namespace NES::Runtime

#endif//NES_NES_RUNTIME_TESTS_INCLUDE_TPCH_TABLE_HPP_
