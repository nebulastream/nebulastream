/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

#ifndef IMPL_REST_RESTSERVER_H_
#define IMPL_REST_RESTSERVER_H_

#include <memory>
#include <string>

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
