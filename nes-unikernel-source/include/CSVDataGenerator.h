//
// Created by ls on 27.09.23.
//

#ifndef NES_CSVDATAGENERATOR_H
#define NES_CSVDATAGENERATOR_H
#include "Runtime/MemoryLayout/DynamicTupleBuffer.hpp"
#include <DataGeneration/DataGenerator.hpp>
#include <fstream>

class CSVDataGenerator : public NES::Benchmark::DataGeneration::DataGenerator {
    CSVDataGenerator(std::ifstream file, NES::SchemaPtr schema, bool repeat) : file(std::move(file)), schema(std::move(schema)), repeat(repeat) {}
    std::ifstream file;
    NES::SchemaPtr schema;
    bool repeat;

  public:
    std::vector<NES::Runtime::TupleBuffer> createData(size_t numberOfBuffers, size_t bufferSize) override;
    NES::SchemaPtr getSchema() override;
    NES::Configurations::SchemaTypePtr getSchemaType() override;
    std::string getName() override;
    std::string toString() override;
    static std::unique_ptr<CSVDataGenerator> create(const char* filename, bool repeat = false);
    bool parseLineIntoBuffer(const std::string& line, NES::Runtime::MemoryLayouts::DynamicTuple tuple);
};

#endif//NES_CSVDATAGENERATOR_H
