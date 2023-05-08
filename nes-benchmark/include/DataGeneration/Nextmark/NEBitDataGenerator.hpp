#ifndef NES_INCLUDE_DATAGENERATORS_NEBIDDATAGENERATOR_HPP_
#define NES_INCLUDE_DATAGENERATORS_NEBIDDATAGENERATOR_HPP_
#include <DataGeneration/DataGenerator.hpp>

namespace NES::Benchmark::DataGeneration {

class NEBitDataGenerator : public DataGenerator {
  public:
    explicit NEBitDataGenerator();

    std::string getName() override;
    std::vector<Runtime::TupleBuffer> createData(size_t numberOfBuffers, size_t bufferSize) override;

    SchemaPtr getSchema() override;
    std::string toString() override;
};

}// namespace

#endif// NES_INCLUDE_DATAGENERATORS_NEBIDDATAGENERATOR_HPP_
