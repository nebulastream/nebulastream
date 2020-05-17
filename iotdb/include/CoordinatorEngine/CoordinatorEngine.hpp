#ifndef NES_INCLUDE_COORDINATORENGINE_COORDINATORENGINE_HPP_
#define NES_INCLUDE_COORDINATORENGINE_COORDINATORENGINE_HPP_
#include <memory>
#include <Topology/NESTopologyEntry.hpp>

namespace NES {
class CoordinatorEngine {

  public:
    size_t registerNode(std::string address, size_t numberOfCPUs, std::string nodeProperties, NESNodeType type);

    bool unregisterNode(size_t queryId);

    bool registerPhysicalStream(size_t queryId,
                                std::string sourcetype,
                                std::string sourceconf,
                                size_t sourcefrequency,
                                size_t numberofbufferstoproduce,
                                std::string physicalstreamname,
                                std::string logicalstreamname);

    bool unregisterPhysicalStream(std::string physicalStreamName, std::string logicalStreamName, size_t queryId);

    bool registerLogicalStream(std::string logicalStreamName, std::string schemaString);

    bool unregisterLogicalStream(std::string logicalStreamName);

    bool addParent(size_t childId, size_t parentId);

    bool removeParent(size_t childId, size_t parentId);

};

typedef std::shared_ptr<CoordinatorEngine> CoordinatorEnginePtr;

}

#endif //NES_INCLUDE_COORDINATORENGINE_COORDINATORENGINE_HPP_
