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

#pragma once

#include <REST/Controller/BaseController.hpp>
#include <Services/QueryService.hpp>
#include <cpprest/http_msg.h>

/*
- * The above undef ensures that NES will compile.
- * There is a 3rd-party library that defines U as a macro for some internal stuff.
- * U is also a template argument of a template function in boost.
- * When the compiler sees them both, it goes crazy.
- * Do not remove the above undef.
- */
#undef U

namespace NES {

class NesCoordinator;
typedef std::shared_ptr<NesCoordinator> NesCoordinatorPtr;
typedef std::weak_ptr<NesCoordinator> NesCoordinatorWeakPtr;

class QueryCatalog;
typedef std::shared_ptr<QueryCatalog> QueryCatalogPtr;

class GlobalExecutionPlan;
typedef std::shared_ptr<GlobalExecutionPlan> GlobalExecutionPlanPtr;

class Topology;
typedef std::shared_ptr<Topology> TopologyPtr;

class QueryController : public BaseController {
  public:
    explicit QueryController(QueryServicePtr queryService, QueryCatalogPtr queryCatalog, TopologyPtr topology, GlobalExecutionPlanPtr globalExecutionPlan);

    ~QueryController() = default;

    /**
     * Handling the Get requests for the query
     * @param path : the url of the rest request
     * @param message : the user message
     */
    void handleGet(std::vector<utility::string_t> path, web::http::http_request message);

    /**
     * Handling the POST requests for the query
     * @param path: the url of the rest request
     * @param message: the user message
     */
    void handlePost(std::vector<utility::string_t> path, web::http::http_request message);

    /**
     * @brief Handle the stop request for the query
     * @param path : the resource path the user wanted to get
     * @param message : the user message
     */
    void handleDelete(std::vector<utility::string_t> path, web::http::http_request message);

  private:
    TopologyPtr topology;
    QueryServicePtr queryService;
    QueryCatalogPtr queryCatalog;
    GlobalExecutionPlanPtr globalExecutionPlan;
};

typedef std::shared_ptr<QueryController> QueryControllerPtr;

}// namespace NES
