#ifndef NES_INCLUDE_DATAGENERATORS_LIGHTSABER_CLUSTERMONITORINGDATAGENERATOR_HPP_
#define NES_INCLUDE_DATAGENERATORS_LIGHTSABER_CLUSTERMONITORINGDATAGENERATOR_HPP_
#include <DataGenerators/DataGenerator.hpp>
namespace NES {

class ClusterMonitoringDataGenerator : public DataGenerator {
  public:
    explicit ClusterMonitoringDataGenerator(Runtime::BufferManagerPtr bufferManager);

    std::string getName() override;
    std::vector<Runtime::TupleBuffer> createData(uint64_t numberOfBuffers, uint64_t bufferSize) override;
    SchemaPtr getSchema() override;
};

}// namespace NES

#endif// NES_INCLUDE_DATAGENERATORS_LIGHTSABER_CLUSTERMONITORINGDATAGENERATOR_HPP_
