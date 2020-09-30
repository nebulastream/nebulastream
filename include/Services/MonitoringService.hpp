#ifndef NES_INCLUDE_SERVICES_MONITORINGSERVICE_HPP_
#define NES_INCLUDE_SERVICES_MONITORINGSERVICE_HPP_

#include <cpprest/json.h>
#include <memory>

namespace NES {

class Schema;
typedef std::shared_ptr<Schema> SchemaPtr;

class WorkerRPCClient;
typedef std::shared_ptr<WorkerRPCClient> WorkerRPCClientPtr;

class Topology;
typedef std::shared_ptr<Topology> TopologyPtr;

class MonitoringPlan;
typedef std::shared_ptr<MonitoringPlan> MonitoringPlanPtr;

class BufferManager;
typedef std::shared_ptr<BufferManager> BufferManagerPtr;

class TupleBuffer;

class MonitoringService {
  public:
    explicit MonitoringService(WorkerRPCClientPtr workerClient, TopologyPtr topology, BufferManagerPtr bufferManager);

    ~MonitoringService();

    web::json::value getTopologyAsJson() const;

    /**
     * @brief Requests from a remote worker node its monitoring data.
     * @param ipAddress
     * @param grpcPort
     * @param the monitoring plan
     * @param the buffer where the data will be written into
     * @return a tuple with the schema and tuplebuffer
     */
    web::json::value requestMonitoringData(const std::string& ipAddress, int64_t grpcPort, MonitoringPlanPtr plan);

    /**
     * @brief Requests from a remote worker node its monitoring data.
     * @param id of the node
     * @param the monitoring plan
     * @param the buffer where the data will be written into
     * @return a tuple with the schema and tuplebuffer
     */
    web::json::value requestMonitoringData(int64_t nodeId, MonitoringPlanPtr plan);

    /**
     * @brief Requests from all remote worker nodes for monitoring data.
     * @param id of the node
     * @param the monitoring plan
     * @param the buffer where the data will be written into
     * @return a tuple with the schema and tuplebuffer
     */
    web::json::value requestMonitoringDataForAllNodes(MonitoringPlanPtr plan);

  private:
    WorkerRPCClientPtr workerClient;
    TopologyPtr topology;
    BufferManagerPtr bufferManager;
};

typedef std::shared_ptr<MonitoringService> MonitoringServicePtr;

}// namespace NES

#endif//NES_INCLUDE_SERVICES_MONITORINGSERVICE_HPP_
