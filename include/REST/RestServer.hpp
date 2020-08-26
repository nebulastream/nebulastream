#ifndef IMPL_REST_RESTSERVER_H_
#define IMPL_REST_RESTSERVER_H_

#include <REST/RestEngine.hpp>

namespace NES {

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
    RestServer(std::string host, u_int16_t port, NesCoordinatorWeakPtr coordinator, QueryCatalogPtr queryCatalog, StreamCatalogPtr streamCatalog,
               TopologyPtr topology, GlobalExecutionPlanPtr globalExecutionPlan, QueryServicePtr queryService);

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
    u_int16_t port;
};
}// namespace NES

#endif//IMPL_REST_RESTSERVER_H_
