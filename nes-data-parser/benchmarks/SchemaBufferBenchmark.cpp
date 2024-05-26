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

#include <API/AttributeField.hpp>
#include <API/Schema.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <FormattedDataGenerator.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/Parsers/CSVParser.hpp>
#include <Sources/Parsers/JSONParser.hpp>
#include <Util/TestTupleBuffer.hpp>
#include <benchmark/benchmark.h>
#include <generation/CSVAdapter.hpp>
#include <generation/JSONAdapter.hpp>
#include <generation/SchemaBuffer.hpp>
#include <random>

using namespace NES::DataParser;

[[maybe_unused]] static void BM_stringstream(benchmark::State& state) {
    auto data = generateCSVData<uint8_t, float, std::string, uint32_t, int64_t>(1);
    std::istringstream iss(data);
    for (auto _ : state) {
        uint8_t a;
        float b;
        std::string c;
        uint32_t d;
        int64_t f;

        char ignore;

        iss >> a >> ignore >> b >> ignore >> ignore >> c >> ignore >> ignore >> d >> ignore >> f >> ignore;
    }
}

[[maybe_unused]] static void BM_ScanLib(benchmark::State& state) {
    using namespace std::literals;
    auto data = generateCSVData<uint8_t, float, std::string, uint32_t, uint64_t>(4);
    for (auto _ : state) {
        auto inputData = std::string_view{data.data(), data.size()};
        while (!inputData.empty()) {
            auto [left, tuple] = CSV::CSVAdapter<SchemaBuffer<
                StaticSchema<Field<UINT8, "a">, Field<FLOAT32, "b">, Field<TEXT, "c">, Field<UINT32, "d">, Field<INT64, "e">>,
                8192>>::parseAndAdvance(inputData);
            inputData = left;
        }
    }
}

[[maybe_unused]] static void BM_JsonLib(benchmark::State& state) {
    using namespace std::literals;
    auto data = generateJSONData<uint8_t, float, std::string, uint32_t, uint64_t>(4);
    for (auto _ : state) {
        auto inputData = std::string_view{data.data(), data.size()};
        while (!inputData.empty()) {
            auto [left, tuple] = JSON::JSONAdapter<SchemaBuffer<
                StaticSchema<Field<UINT8, "a">, Field<FLOAT32, "b">, Field<TEXT, "c">, Field<UINT32, "d">, Field<INT64, "e">>,
                8192>>::parseAndAdvance(inputData);
            inputData = left;
        }
    }
}

[[maybe_unused]] static void BM_SchemaBuffer(benchmark::State& state) {
    using Schema = StaticSchema<Field<UINT8, "a">, Field<FLOAT32, "b">, Field<TEXT, "c">, Field<UINT32, "d">, Field<INT64, "e">>;

    auto data =
        generateRawData<uint8_t, float, std::string, uint32_t, int64_t>(NES::DataParser::SchemaBuffer<Schema, 8192>::Capacity);

    auto bufferManager = std::make_shared<NES::Runtime::BufferManager>(8192);
    auto buffer = bufferManager->getBufferBlocking();

    for (auto _ : state) {
        auto buffer = bufferManager->getBufferBlocking();
        for (const auto& tuple : data) {
            NES::DataParser::SchemaBuffer<Schema, 8192>(buffer).writeTuple(tuple);
        }
    }
}

[[maybe_unused]] static void BM_SchemaBufferCSV(benchmark::State& state) {
    using Schema = StaticSchema<Field<UINT8, "a">, Field<FLOAT32, "b">, Field<TEXT, "c">, Field<UINT32, "d">, Field<INT64, "e">>;

    auto data =
        generateCSVData<uint8_t, float, std::string, uint32_t, int64_t>(NES::DataParser::SchemaBuffer<Schema, 8192>::Capacity);

    auto bufferManager = std::make_shared<NES::Runtime::BufferManager>(8192);
    auto buffer = bufferManager->getBufferBlocking();

    for (auto _ : state) {
        std::string_view dataLeft(data);
        auto buffer = bufferManager->getBufferBlocking();
        dataLeft = CSV::CSVAdapter<NES::DataParser::SchemaBuffer<Schema, 8192>>::parseIntoBuffer(dataLeft, buffer);
        assert((buffer.getNumberOfTuples() == NES::DataParser::SchemaBuffer<Schema, 8192>::Capacity));
    }
}

[[maybe_unused]] static void BM_SchemaBufferJSON(benchmark::State& state) {
    using Schema = StaticSchema<Field<UINT8, "a">, Field<FLOAT32, "b">, Field<UINT32, "c">, Field<INT64, "d">>;

    auto data = generateJSONData<uint8_t, float, uint32_t, int64_t>(SchemaBuffer<Schema, 8192>::Capacity);

    auto bufferManager = std::make_shared<NES::Runtime::BufferManager>(8192);
    auto buffer = bufferManager->getBufferBlocking();

    for (auto _ : state) {
        std::string_view dataLeft(data);
        auto buffer = bufferManager->getBufferBlocking();
        dataLeft = JSON::JSONAdapter<SchemaBuffer<Schema, 8192>>::parseIntoBuffer(dataLeft, buffer);
        assert((buffer.getNumberOfTuples() == NES::DataParser::SchemaBuffer<Schema, 8192>::Capacity));
    }
}

[[maybe_unused]] static void BM_TestTupleBufferJSON(benchmark::State& state) {
    using Schema = StaticSchema<Field<UINT8, "a">, Field<FLOAT32, "b">, Field<TEXT, "c">, Field<UINT32, "d">, Field<INT64, "e">>;
    auto schema = NES::Schema::create(NES::Schema::MemoryLayoutType::ROW_LAYOUT)
                      ->addField("a", NES::BasicType::UINT8)
                      ->addField("b", NES::BasicType::FLOAT32)
                      ->addField("c", NES::BasicType::TEXT)
                      ->addField("d", NES::BasicType::UINT32)
                      ->addField("e", NES::BasicType::INT64);

    std::vector<NES::PhysicalTypePtr> physicalTypes;
    NES::DefaultPhysicalTypeFactory defaultPhysicalTypeFactory = NES::DefaultPhysicalTypeFactory();
    for (const NES::AttributeFieldPtr& field : schema->fields) {
        auto physicalField = defaultPhysicalTypeFactory.getPhysicalType(field->getDataType());
        physicalTypes.push_back(physicalField);
    }

    auto parser =
        std::make_shared<NES::JSONParser>(schema->getSize(), std::vector<std::string>{"a", "b", "c", "d", "e"}, physicalTypes);
    auto data =
        generateJSONData<uint8_t, float, std::string, uint32_t, int64_t>(NES::DataParser::SchemaBuffer<Schema, 8192>::Capacity);

    auto bufferManager = std::make_shared<NES::Runtime::BufferManager>(8192);
    auto buffer = bufferManager->getBufferBlocking();

    for (auto _ : state) {
        auto data_view = std::string_view(data);
        auto buffer = bufferManager->getBufferBlocking();
        auto dynamicBuffer = NES::Runtime::MemoryLayouts::TestTupleBuffer::createTestTupleBuffer(buffer, schema);
        size_t tuplerCounter = 0;
        while (!data_view.empty()) {
            auto newLineIndex = data_view.find_first_of("\n");
            auto line = data_view.substr(0, newLineIndex);
            data_view = data_view.substr(newLineIndex + 1);
            parser->writeInputTupleToTupleBuffer(line, tuplerCounter++, dynamicBuffer, schema, bufferManager);
        }
    }
}

[[maybe_unused]] static void BM_TestTupleBufferCSV(benchmark::State& state) {
    using Schema = StaticSchema<Field<UINT8, "a">, Field<FLOAT32, "b">, Field<TEXT, "c">, Field<UINT32, "d">, Field<INT64, "e">>;
    auto schema = NES::Schema::create(NES::Schema::MemoryLayoutType::ROW_LAYOUT)
                      ->addField("uint8", NES::BasicType::UINT8)
                      ->addField("float32", NES::BasicType::FLOAT32)
                      ->addField("text", NES::BasicType::TEXT)
                      ->addField("uint32", NES::BasicType::UINT32)
                      ->addField("int64", NES::BasicType::INT64);

    std::vector<NES::PhysicalTypePtr> physicalTypes;
    NES::DefaultPhysicalTypeFactory defaultPhysicalTypeFactory = NES::DefaultPhysicalTypeFactory();
    for (const NES::AttributeFieldPtr& field : schema->fields) {
        auto physicalField = defaultPhysicalTypeFactory.getPhysicalType(field->getDataType());
        physicalTypes.push_back(physicalField);
    }

    auto parser = std::make_shared<NES::CSVParser>(schema->getSize(), physicalTypes, ",");
    auto data =
        generateCSVData<uint8_t, float, std::string, uint32_t, int64_t>(NES::DataParser::SchemaBuffer<Schema, 8192>::Capacity);

    auto bufferManager = std::make_shared<NES::Runtime::BufferManager>(8192);
    auto buffer = bufferManager->getBufferBlocking();

    for (auto _ : state) {
        auto data_view = std::string_view(data);
        auto buffer = bufferManager->getBufferBlocking();
        auto dynamicBuffer = NES::Runtime::MemoryLayouts::TestTupleBuffer::createTestTupleBuffer(buffer, schema);
        size_t tuplerCounter = 0;
        while (!data_view.empty()) {
            auto newLineIndex = data_view.find_first_of("\n");
            auto line = data_view.substr(0, newLineIndex);
            data_view = data_view.substr(newLineIndex + 1);
            parser->writeInputTupleToTupleBuffer(line, tuplerCounter++, dynamicBuffer, schema, bufferManager);
        }
    }
}

[[maybe_unused]] static void BM_TestTupleBuffer(benchmark::State& state) {
    using Schema = StaticSchema<Field<UINT8, "a">, Field<FLOAT32, "b">, Field<TEXT, "c">, Field<UINT32, "d">, Field<INT64, "e">>;
    auto schema = NES::Schema::create(NES::Schema::MemoryLayoutType::ROW_LAYOUT)
                      ->addField("uint8", NES::BasicType::UINT8)
                      ->addField("float32", NES::BasicType::FLOAT32)
                      ->addField("text", NES::BasicType::TEXT)
                      ->addField("uint32", NES::BasicType::UINT32)
                      ->addField("int64", NES::BasicType::INT64);

    auto bufferManager = std::make_shared<NES::Runtime::BufferManager>(8192);
    auto data = generateRawData<uint8_t, float, std::string, uint32_t, int64_t>(SchemaBuffer<Schema, 8192>::Capacity);

    for (auto _ : state) {
        auto buffer = bufferManager->getBufferBlocking();
        auto dynamicBuffer = NES::Runtime::MemoryLayouts::TestTupleBuffer::createTestTupleBuffer(buffer, schema);
        for (size_t index = 0; const auto& tuple : data) {
            dynamicBuffer[index][0].write<uint8_t>(std::get<0>(tuple));
            dynamicBuffer[index][1].write<float>(std::get<1>(tuple));
            dynamicBuffer[index].writeVarSized(2u, std::get<2>(tuple), bufferManager.get());
            dynamicBuffer[index][3].write<uint32_t>(std::get<3>(tuple));
            dynamicBuffer[index][4].write<int64_t>(std::get<4>(tuple));
            index++;
        }
        // buffer.setNumberOfTuples(0);
    }
}

BENCHMARK(BM_stringstream);
BENCHMARK(BM_JsonLib);
BENCHMARK(BM_ScanLib);
BENCHMARK(BM_SchemaBuffer);
BENCHMARK(BM_TestTupleBuffer);

BENCHMARK(BM_SchemaBufferCSV);
BENCHMARK(BM_SchemaBufferJSON);
BENCHMARK(BM_TestTupleBufferJSON);
BENCHMARK(BM_TestTupleBufferCSV);

BENCHMARK_MAIN();
