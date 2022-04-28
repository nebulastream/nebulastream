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
#include <SerializableQueryPlan.pb.h>

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

#ifndef NES_USE_OATPP
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
    /**
     * Checks if provided lineage mode is valid. If not valid, create appropriate reply to http req
     * @param lineageMode
     * @param request
     * @return true if provided lineage mode is valid, else false
     */
    bool validateLineageMode(const std::string& lineageModeString, const web::http::http_request& request);

    /**
     * Checks if provided fault tolerance mode is valid. If not valid, create appropriate reply to http req
     * @param faultToleranceString
     * @param request
     * @return true if provided fault tolerance mode is valid, else false
     */
    bool validateFaultToleranceType(const std::string& faultToleranceString, const web::http::http_request& request);

    /**
     * Validates protobuf message. If not valid, creates appropriate reply to http req.
     * Used only if user submitted a protobuf message
     * @param protobufMessage
     * @param request : used to reply to httpRequest in case protobufMessage is invalid
     * @param body :
     * @return true if protobuf message is valid, else false
     */
    bool validateProtobufMessage(const std::shared_ptr<SubmitQueryRequest>& protobufMessage,
                                 const web::http::http_request& request,
                                 const utility::string_t& body);

    /**
     * Validates user request for post requests to 'execute-query' endpoint. If not valid, creates appropriate reply to http request
     * @param userRequest : string representation of http request
     * @param httpRequest : used to reply to httpRequest in case userRequest is invalid
     * @return true if request is valid, else false
     */
    bool validateUserRequest(web::json::value userRequest, const web::http::http_request& httpRequest);

    /**
     * Validates placement Strategy. If not valid, creates appropriate reply to http request
     * @param placementStrategy
     * @param httpRequest
     * @return true if valid, else false
     */
    bool validatePlacementStrategy(const std::string& placementStrategy, const web::http::http_request& httpRequest);

    /**
     * validates that the URI parameters contains 'queryId' field and that the given queryId exists
     * @param parameters
     * @param httpRequest
     * @return true if 'queryId' parameter exists and query with queryId exists else false
     */
    bool validateURIParametersContainQueryIdAndQueryIdExists(std::map<utility::string_t, utility::string_t> parameters,
                                                             const web::http::http_request& httpRequest);
#endif

    QueryServicePtr queryService;
    QueryCatalogServicePtr queryCatalogService;
    GlobalExecutionPlanPtr globalExecutionPlan;
};

using QueryControllerPtr = std::shared_ptr<QueryController>;

}// namespace NES
#endif// NES_INCLUDE_REST_CONTROLLER_QUERYCONTROLLER_HPP_
