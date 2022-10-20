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

#include <Components/NesCoordinator.hpp>
#include <Exceptions/InvalidQueryException.hpp>
#include <Exceptions/InvalidQueryStatusException.hpp>
#include <Exceptions/MapEntryNotFoundException.hpp>
#include <Exceptions/QueryNotFoundException.hpp>
#include <GRPC/Serialization/QueryPlanSerializationUtil.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Utils/PlanJsonGenerator.hpp>
#include <REST/Controller/QueryController.hpp>
#include <SerializableQueryPlan.pb.h>
#include <Services/QueryCatalogService.hpp>
#include <Services/QueryService.hpp>
#include <Util/Logger/Logger.hpp>
#include <cpprest/http_msg.h>
#include <utility>

namespace NES {

const std::string DEFAULT_TOLERANCE_TYPE = "NONE";

QueryController::QueryController(QueryServicePtr queryService,
                                 QueryCatalogServicePtr queryCatalogService,
                                 GlobalExecutionPlanPtr globalExecutionPlan)
    : queryService(std::move(queryService)), queryCatalogService(std::move(queryCatalogService)),
      globalExecutionPlan(std::move(globalExecutionPlan)) {}

void QueryController::handleGet(const std::vector<utility::string_t>& path, web::http::http_request& request) {

    auto parameters = getParameters(request);
    if (!validateURIParametersContainQueryIdAndQueryIdExists(parameters, request)) {
        return;
    }
    QueryId queryId = std::stoi(parameters.find("queryId")->second);
    try {
        if (path[1] == "execution-plan") {
            NES_INFO("QueryController:: GET execution-plan");
            NES_DEBUG("QueryController:: execution-plan requested queryId: " << queryId);
            // get the execution-plan for given query id
            auto executionPlanJson = PlanJsonGenerator::getExecutionPlanAsJson(globalExecutionPlan, queryId);
            NES_DEBUG("QueryController:: execution-plan: " << executionPlanJson.serialize());
            //Prepare the response
            successMessageImpl(request, executionPlanJson, web::http::status_codes::OK);
            return;
        } else if (path[1] == "query-plan") {
            NES_INFO("QueryController:: GET query-plan");
            try {
                Catalogs::Query::QueryCatalogEntryPtr queryCatalogEntry = queryCatalogService->getEntryForQuery(queryId);
                NES_DEBUG("UtilityFunctions: Getting the json representation of the query plan");
                auto basePlan = PlanJsonGenerator::getQueryPlanAsJson(queryCatalogEntry->getInputQueryPlan());
                //Prepare the response
                successMessageImpl(request, basePlan, web::http::status_codes::OK);
                return;
            } catch (const std::exception& exc) {
                NES_ERROR("QueryController: handleGet -query-plan: Exception occurred while building the query plan"
                          "request:"
                          << exc.what());
                handleException(request, exc);
                return;
            }

        } else if (path[1] == "optimization-phases") {
            NES_INFO("QueryController:: GET query-phases");
            try {
                Catalogs::Query::QueryCatalogEntryPtr queryCatalogEntry = queryCatalogService->getEntryForQuery(queryId);
                auto optimizationPhases = queryCatalogEntry->getOptimizationPhases();
                NES_DEBUG("UtilityFunctions: Getting the json representation of the optimized query plans");
                auto response = web::json::value::object();
                for (auto const& [phaseName, queryPlan] : optimizationPhases) {
                    auto queryPlanJson = PlanJsonGenerator::getQueryPlanAsJson(queryPlan);
                    response[phaseName] = queryPlanJson;
                }
                //Prepare the response
                successMessageImpl(request, response, web::http::status_codes::OK);
                return;
            } catch (const std::exception& exc) {
                NES_ERROR("QueryController: handleGet -query-plan: Exception occurred while retreiving optimization phases"
                          "request:"
                          << exc.what());
                handleException(request, exc);
                return;
            }

        } else if (path[1] == "query-status") {
            NES_INFO("QueryController:: GET query-status");
            try {
                Catalogs::Query::QueryCatalogEntryPtr queryCatalogEntry = queryCatalogService->getEntryForQuery(queryId);
                NES_DEBUG("QueryController:: Getting the json representation of status: queryId="
                          << queryId << " status=" << queryCatalogEntry->getQueryStatusAsString());
                web::json::value result{};
                auto node = web::json::value::object();
                // use the id of the root operator to fill the id field
                node["status"] = web::json::value::string(queryCatalogEntry->getQueryStatusAsString());
                //Prepare the response
                successMessageImpl(request, node, web::http::status_codes::OK);
                return;
            } catch (const std::exception& exc) {
                NES_ERROR("QueryController: handleGet -query-status: Exception occurred while retrieving query status"
                          "request:"
                          << exc.what());
                handleException(request, exc);
            }
        } else {
            resourceNotFoundImpl(request);
        }
    } catch (...) {
        NES_ERROR("RestServer: unknown exception");
        internalServerErrorImpl(request);
    }
}

void QueryController::handlePost(const std::vector<utility::string_t>& path, web::http::http_request& request) {

    if (path[1] == "execute-query") {
        NES_DEBUG(" QueryController: Trying to execute query");
        request.extract_string(true)
            .then([this, request](utility::string_t body) {
                try {
                    //Prepare Input query from user string
                    std::string userRequest(body.begin(), body.end());
                    NES_DEBUG("QueryController: handlePost -execute-query: Request body: " << userRequest
                                                                                           << "try to parse query");
                    web::json::value req = web::json::value::parse(userRequest);
                    NES_DEBUG("QueryController: handlePost -execute-query: get user query");
                    if (!validateUserRequest(req, request)) {
                        return;
                    }

                    std::string userQuery = req.at("userQuery").as_string();
                    std::string placementStrategyName = req.at("placement").as_string();
                    std::string faultToleranceString = DEFAULT_TOLERANCE_TYPE;
                    std::string lineageString = DEFAULT_TOLERANCE_TYPE;
                    if (req.has_field("faultTolerance")) {
                        if (!validateFaultToleranceType(req.at("faultTolerance").as_string(), request)) {
                            return;
                        } else {
                            faultToleranceString = req.at("faultTolerance").as_string();
                        }
                    }
                    if (req.has_field("lineage")) {
                        if (!validateLineageMode(req["lineage"].as_string(), request)) {
                            return;
                        } else {
                            lineageString = req.at("lineage").as_string();
                        }
                    }
                    auto faultToleranceMode = FaultToleranceType::getFromString(faultToleranceString);
                    auto lineageMode = LineageType::getFromString(lineageString);
                    NES_DEBUG("QueryController: handlePost -execute-query: Params: userQuery= "
                              << userQuery << ", placement= " << placementStrategyName
                              << ", faultTolerance= " << faultToleranceString << ", lineage= " << lineageString);
                    QueryId queryId = queryService->validateAndQueueAddQueryRequest(userQuery,
                                                                                    placementStrategyName,
                                                                                    faultToleranceMode,
                                                                                    lineageMode);

                    //Prepare the response
                    web::json::value restResponse{};
                    restResponse["queryId"] = web::json::value::number(queryId);
                    successMessageImpl(request, restResponse, web::http::status_codes::Accepted);
                    return;
                } catch (const InvalidQueryException& exc) {
                    NES_ERROR("QueryController: handlePost -execute-query: Exception occurred during submission of a query "
                              "user request:"
                              << exc.what());
                    web::json::value errorResponse{};
                    auto statusCode = web::http::status_codes::BadRequest;
                    errorResponse["code"] = web::json::value(statusCode);
                    errorResponse["message"] = web::json::value::string(exc.what());
                    errorMessageImpl(request, errorResponse, statusCode);
                    return;
                } catch (const MapEntryNotFoundException& exc) {
                    NES_ERROR("QueryController: handlePost -execute-query: Exception occurred during submission of a query "
                              "user request:"
                              << exc.what());
                    web::json::value errorResponse{};
                    auto statusCode = web::http::status_codes::BadRequest;
                    errorResponse["code"] = web::json::value(statusCode);
                    errorResponse["message"] = web::json::value::string(exc.what());
                    errorMessageImpl(request, errorResponse, statusCode);
                    return;
                } catch (...) {
                    NES_ERROR("RestServer: unknown exception.");
                    internalServerErrorImpl(request);
                    return;
                }
            })
            .wait();
    } else if (path[1] == "execute-query-ex") {
        request.extract_string(true)
            .then([this, request](utility::string_t body) {
                try {
                    NES_DEBUG("QueryController: handlePost -execute-query: Request body: " << body);
                    // decode string into protobuf message
                    std::shared_ptr<SubmitQueryRequest> protobufMessage = std::make_shared<SubmitQueryRequest>();
                    if (!validateProtobufMessage(protobufMessage, request, body)) {
                        return;
                    }
                    // decode protobuf message into c++ obj repr
                    SerializableQueryPlan* queryPlanSerialized = protobufMessage->mutable_queryplan();
                    QueryPlanPtr queryPlan(QueryPlanSerializationUtil::deserializeQueryPlan(queryPlanSerialized));
                    auto* context = protobufMessage->mutable_context();
                    std::string faultToleranceString = DEFAULT_TOLERANCE_TYPE;
                    std::string lineageString = DEFAULT_TOLERANCE_TYPE;
                    if (context->contains("faultTolerance")) {
                        if (!validateFaultToleranceType(context->at("faultTolerance").value(), request)) {
                            return;
                        } else {
                            faultToleranceString = context->at("faultTolerance").value();
                        }
                    }
                    if (context->contains("lineage")) {
                        if (!validateLineageMode(lineageString = context->at("lineage").value(), request)) {
                            return;
                        } else {
                            lineageString = context->at("lineage").value();
                        }
                    }
                    std::string* queryString = protobufMessage->mutable_querystring();
                    std::string placementStrategy = context->at("placement").value();
                    auto faultToleranceMode = FaultToleranceType::getFromString(faultToleranceString);
                    auto lineageMode = LineageType::getFromString(lineageString);
                    QueryId queryId = queryService->addQueryRequest(*queryString,
                                                                    queryPlan,
                                                                    placementStrategy,
                                                                    faultToleranceMode,
                                                                    lineageMode);

                    web::json::value restResponse{};
                    restResponse["queryId"] = web::json::value::number(queryId);
                    successMessageImpl(request, restResponse, web::http::status_codes::Accepted);
                    return;
                } catch (const std::exception& exc) {
                    NES_ERROR(
                        "QueryController: handlePost -execute-query-ex: Exception occurred while building the query plan for "
                        "user request:"
                        << exc.what());
                    web::json::value errorResponse{};
                    auto statusCode = web::http::status_codes::BadRequest;
                    errorResponse["code"] = web::json::value(statusCode);
                    errorResponse["message"] = web::json::value::string(exc.what());
                    errorMessageImpl(request, errorResponse, statusCode);
                    return;
                } catch (...) {
                    NES_ERROR("RestServer: unknown exception.");
                    internalServerErrorImpl(request);
                    return;
                }
            })
            .wait();
    } else {
        resourceNotFoundImpl(request);
    }
}// namespace NES

void QueryController::handleDelete(const std::vector<utility::string_t>& path, web::http::http_request& request) {

    //Extract parameters if any
    auto parameters = getParameters(request);
    if (!validateURIParametersContainQueryIdAndQueryIdExists(parameters, request)) {
        return;
    }
    QueryId queryId = std::stoi(parameters.find("queryId")->second);
    if (path[1] == "stop-query") {
        NES_DEBUG("QueryController: Request received for stopping a query");
        try {
            bool success = queryService->validateAndQueueStopQueryRequest(queryId);
            //Prepare the response
            web::json::value result{};
            result["success"] = web::json::value::boolean(success);
            result["queryId"] = web::json::value::number(queryId);
            successMessageImpl(request, result, web::http::status_codes::Accepted);
            return;
        } catch (QueryNotFoundException const& exc) {
            NES_ERROR("QueryCatalogController: handleDelete -query: Exception occurred while stoping the query " << exc.what());
            web::json::value errorResponse{};
            errorResponse["detail"] = web::json::value::string(exc.what());
            errorResponse["queryId"] = web::json::value::number(queryId);
            errorMessageImpl(request, errorResponse, web::http::status_codes::NotFound);
            return;
        } catch (InvalidQueryStatusException const& exc) {
            NES_ERROR("QueryCatalogController: handleDelete -query: Exception occurred while stoping the query " << exc.what());
            web::json::value errorResponse{};
            errorResponse["detail"] =
                web::json::value::string("Query status was either failed or already stopped: " + std::string(exc.what()));
            errorResponse["queryId"] = web::json::value::number(queryId);
            errorMessageImpl(request, errorResponse);
            return;
        } catch (...) {
            NES_ERROR("QueryCatalogController: unknown exception.");
            internalServerErrorImpl(request);
            return;
        }
    } else {
        resourceNotFoundImpl(request);
    }
}

bool QueryController::validateLineageMode(const std::string& lineageModeString, const web::http::http_request& request) {
    try {
        LineageType::getFromString(lineageModeString);
    } catch (Exceptions::RuntimeException e) {
        NES_ERROR("QueryController: handlePost -execute-query: Invalid Lineage Type provided: " + lineageModeString);
        web::json::value errorResponse{};
        auto statusCode = web::http::status_codes::BadRequest;
        errorResponse["code"] = web::json::value(statusCode);
        errorResponse["message"] =
            web::json::value::string("Invalid Lineage Mode Type provided: " + lineageModeString
                                     + ". Valid Lineage Modes are: 'IN_MEMORY', 'PERSISTENT', 'REMOTE', 'NONE'.");
        errorMessageImpl(request, errorResponse, statusCode);
        return false;
    }
    return true;
}

bool QueryController::validateFaultToleranceType(const std::string& faultToleranceString,
                                                 const web::http::http_request& request) {
    try {
        FaultToleranceType::getFromString(faultToleranceString);
    } catch (Exceptions::RuntimeException e) {
        NES_ERROR("QueryController: handlePost -execute-query: Invalid Fault Tolerance Type provided: " + faultToleranceString);
        web::json::value errorResponse{};
        auto statusCode = web::http::status_codes::BadRequest;
        errorResponse["code"] = web::json::value(statusCode);
        errorResponse["message"] = web::json::value::string(
            "Invalid Fault Tolerance Type provided: " + faultToleranceString
            + ". Valid Fault Tolerance Types are: 'AT_MOST_ONCE', 'AT_LEAST_ONCE', 'EXACTLY_ONCE', 'NONE'.");
        errorMessageImpl(request, errorResponse, statusCode);
        return false;
    }
    return true;
}

bool QueryController::validateProtobufMessage(const std::shared_ptr<SubmitQueryRequest>& protobufMessage,
                                              const web::http::http_request& request,
                                              const utility::string_t& body) {
    if (!protobufMessage->ParseFromArray(body.data(), body.size())) {
        web::json::value errorResponse{};
        auto statusCode = web::http::status_codes::BadRequest;
        errorResponse["code"] = web::json::value(statusCode);
        errorResponse["message"] = web::json::value::string("Invalid Protobuf Message");
        errorMessageImpl(request, errorResponse, statusCode);
        return false;
    }
    auto* context = protobufMessage->mutable_context();
    if (!context->contains("placement")) {
        NES_ERROR("QueryController: handlePost -execute-query: No placement strategy specified. Specify a placement strategy "
                  "using 'placement'.");
        web::json::value errorResponse{};
        auto statusCode = web::http::status_codes::BadRequest;
        errorResponse["code"] = web::json::value(statusCode);
        errorResponse["message"] =
            web::json::value::string("No placement strategy specified. Specify a placement strategy using 'placement'.");
        errorResponse["more_info"] =
            web::json::value::string("https://docs.nebula.stream/cpp/class_n_e_s_1_1_placement_strategy.html");
        errorMessageImpl(request, errorResponse, statusCode);
        return false;
    }
    std::string placementStrategy = context->at("placement").value();
    return validatePlacementStrategy(placementStrategy, request);
}

bool QueryController::validateUserRequest(web::json::value userRequest, const web::http::http_request& httpRequest) {
    if (!userRequest.has_field("userQuery")) {
        NES_ERROR("QueryController: handlePost -execute-query: Wrong key word for user query, use 'userQuery'.");
        web::json::value errorResponse{};
        auto statusCode = web::http::status_codes::BadRequest;
        errorResponse["code"] = web::json::value(statusCode);
        errorResponse["message"] = web::json::value::string("Incorrect or missing key word for user query, use 'userQuery'.");
        errorResponse["more_info"] = web::json::value::string("https://docs.nebula.stream/docs/clients/rest-api/");
        errorMessageImpl(httpRequest, errorResponse, statusCode);
        return false;
    }
    if (!userRequest.has_field("placement")) {
        NES_ERROR("QueryController: handlePost -execute-query: No placement strategy specified. Specify a placement strategy "
                  "using 'placement'.");
        web::json::value errorResponse{};
        auto statusCode = web::http::status_codes::BadRequest;
        errorResponse["code"] = web::json::value(statusCode);
        errorResponse["message"] =
            web::json::value::string("No placement strategy specified. Specify a placement strategy using 'placement'.");

        errorResponse["more_info"] =
            web::json::value::string("https://docs.nebula.stream/cpp/class_n_e_s_1_1_placement_strategy.html");
        errorMessageImpl(httpRequest, errorResponse, statusCode);
        return false;
    }
    std::string placementStrategy = userRequest["placement"].as_string();
    return validatePlacementStrategy(placementStrategy, httpRequest);
}

bool QueryController::validatePlacementStrategy(const std::string& placementStrategy,
                                                const web::http::http_request& httpRequest) {
    try {
        PlacementStrategy::getFromString(placementStrategy);
    } catch (Exceptions::RuntimeException exc) {
        web::json::value errorResponse{};
        auto statusCode = web::http::status_codes::BadRequest;
        errorResponse["code"] = web::json::value(statusCode);
        errorResponse["message"] = web::json::value::string("Invalid Placement Strategy: " + placementStrategy);
        errorResponse["more_info"] =
            web::json::value::string("https://docs.nebula.stream/cpp/class_n_e_s_1_1_placement_strategy.html");
        errorMessageImpl(httpRequest, errorResponse, statusCode);
        return false;
    }
    return true;
}

bool QueryController::validateURIParametersContainQueryIdAndQueryIdExists(
    std::map<utility::string_t, utility::string_t> parameters,
    const web::http::http_request& httpRequest) {
    auto const queryParameter = parameters.find("queryId");
    if (queryParameter == parameters.end()) {
        NES_ERROR("QueryController: Unable to find query ID for the GET request");
        web::json::value errorResponse{};
        auto statusCode = web::http::status_codes::BadRequest;
        errorResponse["code"] = web::json::value(statusCode);
        errorResponse["message"] = web::json::value::string("Parameter queryId must be provided");
        errorResponse["more_info"] = web::json::value::string("https://docs.nebula.stream/docs/clients/rest-api/");
        errorMessageImpl(httpRequest, errorResponse, statusCode);
        return false;
    }
    // get the queryId from user input
    QueryId queryId = std::stoi(queryParameter->second);
    NES_DEBUG("Query Controller: Get the registered query");
    try {
        queryCatalogService->getEntryForQuery(queryId);
    } catch (QueryNotFoundException exc) {
        web::json::value errorResponse{};
        auto statusCode = web::http::status_codes::NotFound;
        errorResponse["code"] = web::json::value(statusCode);
        errorResponse["message"] = web::json::value::string("Provided QueryId: " + std::to_string(queryId) + " does not exist");
        errorResponse["more_info"] = web::json::value::string("https://docs.nebula.stream/docs/clients/rest-api/");
        errorMessageImpl(httpRequest, errorResponse, web::http::status_codes::NotFound);
        return false;
    }
    return true;
}

}// namespace NES
