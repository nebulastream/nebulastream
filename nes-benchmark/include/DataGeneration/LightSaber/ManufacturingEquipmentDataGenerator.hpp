#ifndef NES_INCLUDE_DATAGENERATORS_LIGHTSABER_MANUFACTURINGEQUIPMENTDATAGENERATOR_HPP_
#define NES_INCLUDE_DATAGENERATORS_LIGHTSABER_MANUFACTURINGEQUIPMENTDATAGENERATOR_HPP_
#include <DataGenerators/DataGenerator.hpp>
namespace NES {

class ManufacturingEquipmentDataGenerator : public DataGenerator {
  public:
    explicit ManufacturingEquipmentDataGenerator(Runtime::BufferManagerPtr bufferManager);

    std::string getName() override;
    std::vector<Runtime::TupleBuffer> createData(uint64_t numberOfBuffers, uint64_t bufferSize) override;
    SchemaPtr getSchema() override;
};

}// namespace NES


#endif//NES_INCLUDE_DATAGENERATORS_LIGHTSABER_MANUFACTURINGEQUIPMENTDATAGENERATOR_HPP_
