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

#ifndef NES_NES_COORDINATOR_INCLUDE_REST_CONTROLLER_STATISTICCONTROLLER_HPP_
#define NES_NES_COORDINATOR_INCLUDE_REST_CONTROLLER_STATISTICCONTROLLER_HPP_

#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Common/ValueTypes/BasicValue.hpp>
#include <Operators/Expressions/ConstantValueExpressionNode.hpp>
#include <Operators/Expressions/ExpressionNode.hpp>
#include <Operators/Expressions/LogicalExpressions/EqualsExpressionNode.hpp>
#include <Operators/Expressions/LogicalExpressions/GreaterEqualsExpressionNode.hpp>
#include <Operators/Expressions/LogicalExpressions/GreaterExpressionNode.hpp>
#include <Operators/Expressions/LogicalExpressions/LessEqualsExpressionNode.hpp>
#include <Operators/Expressions/LogicalExpressions/LessExpressionNode.hpp>
#include <REST/Controller/BaseRouterPrefix.hpp>
#include <Statistics/Requests/StatisticCreateRequest.hpp>
#include <Statistics/Requests/StatisticDeleteRequest.hpp>
#include <Statistics/Requests/StatisticProbeRequest.hpp>
#include <Statistics/StatisticCoordinator/StatisticCoordinator.hpp>
#include <oatpp/core/macro/codegen.hpp>
#include <oatpp/core/macro/component.hpp>
#include <oatpp/web/server/api/ApiController.hpp>

#include OATPP_CODEGEN_BEGIN(ApiController)

namespace NES {
class NesCoordinator;
using NesCoordinatorWeakPtr = std::weak_ptr<NesCoordinator>;

class ErrorHandler;
using ErrorHandlerPtr = std::shared_ptr<ErrorHandler>;

namespace Experimental::Statistics {
class StatisticController : public oatpp::web::server::api::ApiController {

  public:
    StatisticController(const std::shared_ptr<ObjectMapper>& objectMapper,
                        const NesCoordinatorWeakPtr& coordinator,
                        const oatpp::String& completeRouterPrefix,
                        const ErrorHandlerPtr& errorHandler)
        : oatpp::web::server::api::ApiController(objectMapper, completeRouterPrefix), coordinator(coordinator),
          errorHandler(errorHandler) {}

    static std::shared_ptr<StatisticController> create(const std::shared_ptr<ObjectMapper>& objectMapper,
                                                       const NesCoordinatorWeakPtr& coordinator,
                                                       const std::string& routerPrefixAddition,
                                                       const ErrorHandlerPtr& errorHandler) {
        oatpp::String completeRouterPrefix = BASE_ROUTER_PREFIX + routerPrefixAddition;
        return std::make_shared<StatisticController>(objectMapper, coordinator, completeRouterPrefix, errorHandler);
    }

    ENDPOINT("POST", "/createStatistic", createStatistic, BODY_STRING(String, request)) {
        try {
            std::string req = request.getValue("{}");
            nlohmann::json jsonRequest = nlohmann::json::parse(req);

            auto logicalSourceName = jsonRequest["logicalSourceName"].get<std::string>();
            auto fieldName = jsonRequest["fieldName"].get<std::string>();
            auto timestampField = jsonRequest["timestampField"].get<std::string>();

            uint32_t collectorTypeNum = jsonRequest["statisticCollectorType"];

            // Convert the string value to the enum value
            NES::Experimental::Statistics::StatisticCollectorType statisticCollectorType;
            switch (collectorTypeNum) {
                case 0: statisticCollectorType = StatisticCollectorType::COUNT_MIN; break;
                case 1: statisticCollectorType = StatisticCollectorType::HYPER_LOG_LOG; break;
                case 2: statisticCollectorType = StatisticCollectorType::DDSKETCH; break;
                case 3: statisticCollectorType = StatisticCollectorType::RESERVOIR; break;
                default: statisticCollectorType = StatisticCollectorType::UNDEFINED; break;
            }

            auto createRequest = StatisticCreateRequest(logicalSourceName, fieldName, timestampField, statisticCollectorType);

            uint64_t queryId = 0;
            if (auto shared_back_reference = coordinator.lock()) {
                queryId = shared_back_reference->getStatCoordinator()->createStatistic(createRequest);
            }

            nlohmann::json response;
            response["queryId"] = queryId;
            return createResponse(Status::CODE_202, response.dump());
        } catch (...) {
            return createResponse(Status::CODE_400, "Malformed Request");
        }
    }

    ENDPOINT("POST", "probeStatistic", probeStatistic, BODY_STRING(String, request)) {
        try {
            std::string req = request.getValue("{}");
            nlohmann::json jsonRequest = nlohmann::json::parse(req);

            auto logicalSourceName = jsonRequest["logicalSourceName"].get<std::string>();
            auto fieldName = jsonRequest["fieldName"].get<std::string>();

            uint32_t collectorTypeNum = jsonRequest["statisticCollectorType"];

            // Convert the string value to the enum value
            NES::Experimental::Statistics::StatisticCollectorType statisticCollectorType;
            switch (collectorTypeNum) {
                case 0: statisticCollectorType = StatisticCollectorType::COUNT_MIN; break;
                case 1: statisticCollectorType = StatisticCollectorType::HYPER_LOG_LOG; break;
                case 2: statisticCollectorType = StatisticCollectorType::DDSKETCH; break;
                case 3: statisticCollectorType = StatisticCollectorType::RESERVOIR; break;
                default: statisticCollectorType = StatisticCollectorType::UNDEFINED; break;
            }

            ExpressionNodePtr leftExpressionPart;
            ExpressionNodePtr rightExpressionPart;

            if (jsonRequest["leftExpressionPart"].is_string()) {
                leftExpressionPart = NES::Attribute(jsonRequest["leftExpressionPart"].get<std::string>()).getExpressionNode();
            } else {
                leftExpressionPart = ConstantValueExpressionNode::create(
                    DataTypeFactory::createBasicValue(BasicType::UINT64,
                                                      std::to_string(jsonRequest["leftExpressionPart"].get<uint64_t>())));
            }

            if (jsonRequest["rightExpressionPart"].is_string()) {
                rightExpressionPart = NES::Attribute(jsonRequest["rightExpressionPart"].get<std::string>()).getExpressionNode();
            } else {
                rightExpressionPart = ConstantValueExpressionNode::create(
                    DataTypeFactory::createBasicValue(BasicType::UINT64,
                                                      std::to_string(jsonRequest["rightExpressionPart"].get<uint64_t>())));
            }

            std::string operation = jsonRequest["operation"].get<std::string>();

            // if min() or similar are added later the operation is empty
            ExpressionNodePtr expression = nullptr;
            if (!operation.empty()) {
                if (operation == "==") {
                    expression = EqualsExpressionNode::create(leftExpressionPart, rightExpressionPart);
                } else if (operation == ">") {
                    expression = GreaterExpressionNode::create(leftExpressionPart, rightExpressionPart);
                } else if (operation == "<") {
                    expression = LessExpressionNode::create(leftExpressionPart, rightExpressionPart);
                } else if (operation == ">=") {
                    expression = GreaterEqualsExpressionNode::create(leftExpressionPart, rightExpressionPart);
                } else if (operation == "<=") {
                    expression = LessEqualsExpressionNode::create(leftExpressionPart, rightExpressionPart);
                }
            }

            std::vector<std::string> physicalSourceNames = jsonRequest["physicalSourceNames"];
            auto startTime = jsonRequest["startTime"].get<time_t>();
            auto endTime = jsonRequest["endTime"].get<time_t>();
            auto merge = jsonRequest["merge"].get<bool>();

            auto probeRequest = StatisticProbeRequest(logicalSourceName,
                                                      fieldName,
                                                      statisticCollectorType,
                                                      expression,
                                                      physicalSourceNames,
                                                      startTime,
                                                      endTime,
                                                      merge);

            std::vector<double> statistics = {};
            if (auto shared_back_reference = coordinator.lock()) {
                statistics = shared_back_reference->getStatCoordinator()->probeStatistic(probeRequest);
            }

            nlohmann::json response;
            response["statistics"] = statistics;
            return createResponse(Status::CODE_202, response.dump());
        } catch (...) {
            return createResponse(Status::CODE_400, "Malformed Request");
        }
    }

    ENDPOINT("DELETE", "deleteStatistic", deleteStatistic, BODY_STRING(String, request)) {
        try {
            std::string req = request.getValue("{}");
            nlohmann::json jsonRequest = nlohmann::json::parse(req);

            auto logicalSourceName = jsonRequest["logicalSourceName"].get<std::string>();
            auto fieldName = jsonRequest["fieldName"].get<std::string>();

            uint32_t collectorTypeNum = jsonRequest["statisticCollectorType"];

            // Convert the string value to the enum value
            NES::Experimental::Statistics::StatisticCollectorType statisticCollectorType;
            switch (collectorTypeNum) {
                case 0: statisticCollectorType = StatisticCollectorType::COUNT_MIN; break;
                case 1: statisticCollectorType = StatisticCollectorType::HYPER_LOG_LOG; break;
                case 2: statisticCollectorType = StatisticCollectorType::DDSKETCH; break;
                case 3: statisticCollectorType = StatisticCollectorType::RESERVOIR; break;
                default: statisticCollectorType = StatisticCollectorType::UNDEFINED; break;
            }

            auto endTime = jsonRequest["endTime"].get<time_t>();
            auto stop = jsonRequest["fieldName"].get<bool>();

            auto deleteRequest = StatisticDeleteRequest(logicalSourceName, fieldName, statisticCollectorType, endTime, stop);

            bool success = false;
            if (auto shared_back_reference = coordinator.lock()) {
                success = shared_back_reference->getStatCoordinator()->deleteStatistic(deleteRequest);
            }

            nlohmann::json response;
            response["success"] = success;
            return createResponse(Status::CODE_202, response.dump());
        } catch (...) {
            return createResponse(Status::CODE_400, "Malformed Request");
        }
    }

  private:
    NesCoordinatorWeakPtr coordinator;
    ErrorHandlerPtr errorHandler;
};
}// namespace Experimental::Statistics
}// namespace NES

#endif//NES_NES_COORDINATOR_INCLUDE_REST_CONTROLLER_STATISTICCONTROLLER_HPP_
