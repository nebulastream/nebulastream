#ifndef NES_INCLUDE_DATAGENERATORS_NEAUCTIONDATAGENERATOR_HPP_
#define NES_INCLUDE_DATAGENERATORS_NEAUCTIONDATAGENERATOR_HPP_
#include <DataGeneration/DataGenerator.hpp>

namespace NES::Benchmark::DataGeneration {

class NEAuctionDataGenerator : public DataGenerator {
  public:
    explicit NEAuctionDataGenerator();

    std::string getName() override;
    std::vector<Runtime::TupleBuffer> createData(size_t numberOfBuffers, size_t bufferSize) override;
    SchemaPtr getSchema() override;
    std::string toString() override;
};

}// namespace

#endif// NES_INCLUDE_DATAGENERATORS_NEAUCTIONDATAGENERATOR_HPP_
