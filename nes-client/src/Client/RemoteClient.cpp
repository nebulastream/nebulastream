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

#include <API/Query.hpp>
#include <Client/ClientException.hpp>
#include <Client/QueryConfig.hpp>
#include <Client/RemoteClient.hpp>
#include <Exceptions/InvalidArgumentException.hpp>
#include <Operators/Serialization/QueryPlanSerializationUtil.hpp>
#include <Operators/Serialization/SchemaSerializationUtil.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <SerializableQueryPlan.pb.h>
#include <Util/Logger/Logger.hpp>
#include <Util/magicenum/magic_enum.hpp>
#include <cpr/cpr.h>
#include <nlohmann/json.hpp>

using namespace std::string_literals;

namespace NES::Client {

RemoteClient::RemoteClient(const std::string& coordinatorHost,
                           uint16_t coordinatorRESTPort,
                           std::chrono::seconds requestTimeout,
                           bool disableLogging)
    : coordinatorHost(coordinatorHost), coordinatorRESTPort(coordinatorRESTPort), requestTimeout(requestTimeout) {
    if (!disableLogging) {
        NES::Logger::setupLogging("nesRemoteClientStarter.log", LogLevel::LOG_DEBUG);
    }

    if (coordinatorHost.empty()) {
        throw ClientException("host name for coordinator is empty");
    }
}

uint64_t RemoteClient::submitQuery(const Query& query, QueryConfig config) {
    auto queryPlan = query.getQueryPlan();
    SubmitQueryRequest request;
    auto* serializedQueryPlan = request.mutable_queryplan();
    QueryPlanSerializationUtil::serializeQueryPlan(queryPlan, serializedQueryPlan, true);

    auto& context = *request.mutable_context();

    auto placement = google::protobuf::Any();
    placement.set_value(std::string(magic_enum::enum_name(config.getPlacementType())));
    context["placement"] = placement;

    std::string message = request.SerializeAsString();
    auto path = "query/execute-query-ex";

    nlohmann::json resultJson;
    auto future = cpr::PostAsync(cpr::Url{getHostName() + path},
                                 cpr::Header{{"Content-Type", "application/json"}},
                                 cpr::Body{message},
                                 cpr::Timeout{requestTimeout});
    NES_DEBUG("RemoteClient::send: {} {}", this->coordinatorHost, this->coordinatorRESTPort);
    future.wait();
    auto response = future.get();
    nlohmann::json result = nlohmann::json::parse(response.text);

    if (response.status_code == cpr::status::HTTP_ACCEPTED) {
        if (result.contains("queryId")) {
            return result["queryId"].get<uint64_t>();
        } else {
            throw ClientException("Invalid response format queryId is not contained in: " + result.dump());
        }
    } else {
        NES_ERROR("Received response with error code: {}", response.status_code);
        if (result.contains("message")) {
            throw ClientException(result["message"]);
        } else {
            throw ClientException("Invalid response format for error message");
        }
    }
}

bool RemoteClient::testConnection() {
    auto path = "connectivity/check";

    auto future = cpr::GetAsync(cpr::Url{getHostName() + path}, cpr::Timeout{requestTimeout});
    NES_DEBUG("RemoteClient::send: {} {}", this->coordinatorHost, this->coordinatorRESTPort);
    future.wait();
    auto response = future.get();
    nlohmann::json result = nlohmann::json::parse(response.text);
    NES_DEBUG("RemoteClient::send: {} {}", this->coordinatorHost, this->coordinatorRESTPort);

    if (result.contains("success")) {
        return result["success"].get<bool>();
    }
    throw ClientException("Invalid response format");
}

std::string RemoteClient::getQueryPlan(uint64_t queryId) {
    auto path = "query/query-plan?queryId=" + std::to_string(queryId);

    auto future = cpr::GetAsync(cpr::Url{getHostName() + path}, cpr::Timeout{requestTimeout});
    NES_DEBUG("RemoteClient::send: {} {}", this->coordinatorHost, this->coordinatorRESTPort);
    future.wait();
    auto response = future.get();
    nlohmann::json result = nlohmann::json::parse(response.text);

    return result.dump();
}

std::string RemoteClient::getQueryExecutionPlan(uint64_t queryId) {
    auto path = "query/execution-plan?queryId=" + std::to_string(queryId);

    auto future = cpr::GetAsync(cpr::Url{getHostName() + path}, cpr::Timeout{requestTimeout});
    NES_DEBUG("RemoteClient::send: {} {}", this->coordinatorHost, this->coordinatorRESTPort);
    future.wait();
    auto response = future.get();
    nlohmann::json result = nlohmann::json::parse(response.text);

    return result.dump();
}

std::string RemoteClient::getQueryStatus(uint64_t queryId) {
    auto path = "query/query-status?queryId=" + std::to_string(queryId);

    auto future = cpr::GetAsync(cpr::Url{getHostName() + path}, cpr::Timeout{requestTimeout});
    NES_DEBUG("RemoteClient::send: {} {}", this->coordinatorHost, this->coordinatorRESTPort);
    future.wait();
    auto response = future.get();
    nlohmann::json result = nlohmann::json::parse(response.text);
    if (response.status_code == cpr::status::HTTP_OK && result.contains("status")) {
        return result["status"].get<std::string>();
    } else if (result.contains("message")) {
        throw ClientException(result["message"]);
    } else {
        throw ClientException("Invalid response format for error message");
    }
}

RemoteClient::QueryStopResult RemoteClient::stopQuery(uint64_t queryId) {
    auto path = "query/stop-query?queryId="s + std::to_string(queryId);

    auto future = cpr::DeleteAsync(cpr::Url{getHostName() + path}, cpr::Timeout{requestTimeout});
    NES_DEBUG("RemoteClient::send: {} {}", this->coordinatorHost, this->coordinatorRESTPort);
    future.wait();
    auto response = future.get();
    nlohmann::json result = nlohmann::json::parse(response.text);
    QueryStopResult r;
    auto withError = !result.contains("success");
    if (withError) {
        r.errorMessage = (result.contains("detail")) ? result["detail"].get<std::string>() : result["message"].get<std::string>();
    }
    r.withError = withError;
    return r;
}

std::string RemoteClient::getTopology() {
    auto path = "topology";

    auto future = cpr::GetAsync(cpr::Url{getHostName() + path}, cpr::Timeout{requestTimeout});
    NES_DEBUG("RemoteClient::send: {} {}", this->coordinatorHost, this->coordinatorRESTPort);
    future.wait();
    auto response = future.get();
    nlohmann::json result = nlohmann::json::parse(response.text);
    return result.dump();
}

std::string RemoteClient::getQueries() {
    auto path = "queryCatalog/allRegisteredQueries";

    nlohmann::json jsonReturn;
    auto future = cpr::GetAsync(cpr::Url{getHostName() + path}, cpr::Timeout{requestTimeout});
    NES_DEBUG("RemoteClient::send: {} {}", this->coordinatorHost, this->coordinatorRESTPort);
    future.wait();
    auto response = future.get();
    nlohmann::json result = nlohmann::json::parse(response.text);
    return result.dump();
}

std::string RemoteClient::getQueries(QueryState status) {
    std::string queryStatus = std::string(magic_enum::enum_name(status));

    cpr::AsyncResponse future =
        cpr::GetAsync(cpr::Url{getHostName() + "queryCatalog/queries"}, cpr::Parameters{{"status", queryStatus}});

    future.wait();
    auto response = future.get();
    nlohmann::json jsonResponse = nlohmann::json::parse(response.text);
    return jsonResponse.dump();
}

std::string RemoteClient::getPhysicalSources(std::string logicalSourceName) {
    NES_ASSERT2_FMT(!logicalSourceName.empty(), "Empty logicalSourceName");
    auto path = "sourceCatalog/allPhysicalSource?logicalSourceName=" + logicalSourceName;

    nlohmann::json jsonReturn;
    auto future = cpr::GetAsync(cpr::Url{getHostName() + path}, cpr::Timeout{requestTimeout});
    NES_DEBUG("RemoteClient::send: {} {}", this->coordinatorHost, this->coordinatorRESTPort);
    future.wait();
    auto response = future.get();
    nlohmann::json result = nlohmann::json::parse(response.text);
    return result.dump();
}

bool RemoteClient::addLogicalSource(const SchemaPtr schema, const std::string& sourceName) {
    auto path = "sourceCatalog/addLogicalSource-ex";

    auto serializableSchema = SchemaSerializationUtil::serializeSchema(schema, new SerializableSchema());
    SerializableNamedSchema request;
    request.set_sourcename(sourceName);
    request.set_allocated_schema(serializableSchema.get());
    std::string msg = request.SerializeAsString();
    request.release_schema();

    nlohmann::json resultJson;
    auto future = cpr::PostAsync(cpr::Url{getHostName() + path},
                                 cpr::Header{{"Content-Type", "application/json"}},
                                 cpr::Body{msg},
                                 cpr::Timeout{requestTimeout});
    NES_DEBUG("RemoteClient::send: {} {}", this->coordinatorHost, this->coordinatorRESTPort);
    future.wait();
    auto response = future.get();
    nlohmann::json result = nlohmann::json::parse(response.text);
    if (response.status_code == cpr::status::HTTP_OK && result.contains("success")) {
        return result["success"].get<bool>();
    } else if (result.contains("message")) {
        throw ClientException(result["message"]);
    } else {
        throw ClientException("Invalid response format for error message");
    }
}

std::string RemoteClient::getLogicalSources() {
    auto path = "sourceCatalog/allLogicalSource";

    nlohmann::json jsonReturn;
    auto future = cpr::GetAsync(cpr::Url{getHostName() + path}, cpr::Timeout{requestTimeout});
    NES_DEBUG("RemoteClient::send: {} {}", this->coordinatorHost, this->coordinatorRESTPort);
    future.wait();
    auto response = future.get();
    nlohmann::json result = nlohmann::json::parse(response.text);
    return result.dump();
}

std::string RemoteClient::getHostName() {
    return "http://" + coordinatorHost + ":" + std::to_string(coordinatorRESTPort) + "/v1/nes/";
}
}// namespace NES::Client
