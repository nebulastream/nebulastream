#ifndef NES_INCLUDE_DATAGENERATORS_LIGHTSABER_SMARTGRIDDATAGENERATOR_HPP_
#define NES_INCLUDE_DATAGENERATORS_LIGHTSABER_SMARTGRIDDATAGENERATOR_HPP_
#include <DataGenerators/DataGenerator.hpp>
namespace NES {

class SmartGridDataGenerator : public DataGenerator {
  public:
    explicit SmartGridDataGenerator(Runtime::BufferManagerPtr bufferManager);

    std::string getName() override;
    std::vector<Runtime::TupleBuffer> createData(uint64_t numberOfBuffers, uint64_t bufferSize) override;
    SchemaPtr getSchema() override;
};

}// namespace NES

#endif// NES_INCLUDE_DATAGENERATORS_LIGHTSABER_SMARTGRIDDATAGENERATOR_HPP_
