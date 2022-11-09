#ifndef NES_INCLUDE_DATAGENERATORS_YSBDATAGENERATOR_HPP_
#define NES_INCLUDE_DATAGENERATORS_YSBDATAGENERATOR_HPP_
#include "DataGenerator.hpp"
namespace NES::Benchmark::DataGeneration {

class YSBDataGenerator : public DataGenerator {
  public:
    explicit YSBDataGenerator(Runtime::BufferManagerPtr bufferManager);

    std::string getName() override;
    std::vector<Runtime::TupleBuffer> createData(uint64_t numberOfBuffers, uint64_t bufferSize) override;
    SchemaPtr getSchema() override;
};

}// namespace NES

#endif// NES_INCLUDE_DATAGENERATORS_LIGHTSABER_YSBDATAGENERATOR_HPP_
