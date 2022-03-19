/*
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

#ifndef NES_INCLUDE_REST_CONTROLLER_QUERYCONTROLLER_HPP_
#define NES_INCLUDE_REST_CONTROLLER_QUERYCONTROLLER_HPP_

#include <REST/Controller/BaseController.hpp>
#include <REST/CpprestForwardedRefs.hpp>

namespace NES {

class NesCoordinator;
using NesCoordinatorPtr = std::shared_ptr<NesCoordinator>;
using NesCoordinatorWeakPtr = std::weak_ptr<NesCoordinator>;

class QueryCatalogService;
using QueryCatalogServicePtr = std::shared_ptr<QueryCatalogService>;

class QueryService;
using QueryServicePtr = std::shared_ptr<QueryService>;

class GlobalExecutionPlan;
using GlobalExecutionPlanPtr = std::shared_ptr<GlobalExecutionPlan>;

class QueryController : public BaseController {
  public:
    explicit QueryController(QueryServicePtr queryService,
                             QueryCatalogServicePtr queryCatalogService,
                             GlobalExecutionPlanPtr globalExecutionPlan);

    ~QueryController() = default;

    /**
     * Handling the Get requests for the query
     * @param path : the url of the rest request
     * @param request : the user message
     */
    void handleGet(const std::vector<utility::string_t>& path, web::http::http_request& request) override;

    /**
     * Handling the POST requests for the query
     * @param path: the url of the rest request
     * @param message: the user message
     */
    void handlePost(const std::vector<utility::string_t>& path, web::http::http_request& message) override;

    /**
     * @brief Handle the stop request for the query
     * @param path : the resource path the user wanted to get
     * @param request : the user message
     */
    void handleDelete(const std::vector<utility::string_t>& path, web::http::http_request& request) override;

  private:
    TopologyPtr topology;
    QueryServicePtr queryService;
    QueryCatalogServicePtr queryCatalogService;
    GlobalExecutionPlanPtr globalExecutionPlan;
};

using QueryControllerPtr = std::shared_ptr<QueryController>;

}// namespace NES
#endif  // NES_INCLUDE_REST_CONTROLLER_QUERYCONTROLLER_HPP_
