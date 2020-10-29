#ifndef IMPL_REST_RESTSERVER_H_
#define IMPL_REST_RESTSERVER_H_

#include <string>
#include <memory>

namespace NES {

class RestEngine;
typedef std::shared_ptr<RestEngine> RestEnginePtr;

class NesCoordinator;
typedef std::weak_ptr<NesCoordinator> NesCoordinatorWeakPtr;

class QueryCatalog;
typedef std::shared_ptr<QueryCatalog> QueryCatalogPtr;

class StreamCatalog;
typedef std::shared_ptr<StreamCatalog> StreamCatalogPtr;

class Topology;
typedef std::shared_ptr<Topology> TopologyPtr;

class GlobalExecutionPlan;
typedef std::shared_ptr<GlobalExecutionPlan> GlobalExecutionPlanPtr;

class QueryService;
typedef std::shared_ptr<QueryService> QueryServicePtr;

class MonitoringService;
typedef std::shared_ptr<MonitoringService> MonitoringServicePtr;

class GlobalQueryPlan;
typedef std::shared_ptr<GlobalQueryPlan> GlobalQueryPlanPtr;

/**
 * @brief : This class is responsible for starting the REST server.
 */
class RestServer {

  public:
    /**
    * @brief constructor for rest server
    * @param host as string
    * @param port as uint
    * @param handle to coordinator
     *
   * */
    RestServer(std::string host, uint16_t port, NesCoordinatorWeakPtr coordinator, QueryCatalogPtr queryCatalog, StreamCatalogPtr streamCatalog,
               TopologyPtr topology, GlobalExecutionPlanPtr globalExecutionPlan, QueryServicePtr queryService, MonitoringServicePtr monitoringService,
               GlobalQueryPlanPtr globalQueryPlan);

    ~RestServer();
    /**
   * @brief method to start the rest server
   * @return bool indicating success
   */
    bool start();

    /**
   * @brief method to stop rest server
   * @return bool indicating sucesss
   */
    bool stop();

  private:
    RestEnginePtr restEngine;
    std::string host;
    uint16_t port;
};
}// namespace NES

#endif//IMPL_REST_RESTSERVER_H_
