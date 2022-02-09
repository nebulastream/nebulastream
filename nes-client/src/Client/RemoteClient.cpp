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
#include <Client/RemoteClient.hpp>
#include <Exceptions/InvalidArgumentException.hpp>
#include <GRPC/Serialization/QueryPlanSerializationUtil.hpp>
#include <GRPC/Serialization/SchemaSerializationUtil.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <SerializableQueryPlan.pb.h>
#include <Util/Logger.hpp>
#include <cpprest/http_client.h>

namespace NES::Client {

RemoteClient::RemoteClient(const std::string& coordinatorHost, uint16_t coordinatorRESTPort, std::chrono::seconds requestTimeout)
    : coordinatorHost(coordinatorHost), coordinatorRESTPort(coordinatorRESTPort), requestTimeout(requestTimeout) {
    NES::setupLogging("nesRemoteClientStarter.log", NES::getDebugLevelFromString("LOG_DEBUG"));

    if (coordinatorHost.empty()) {
        throw ClientException("host name for coordinator is empty");
    }
}

uint64_t RemoteClient::submitQuery(const Query& query, const QueryConfig config) {
    auto queryPlan = query.getQueryPlan();
    SubmitQueryRequest request;
    auto* serializedQueryPlan = request.mutable_queryplan();
    QueryPlanSerializationUtil::serializeQueryPlan(queryPlan, serializedQueryPlan, true);

    auto& context = *request.mutable_context();

    auto placement = google::protobuf::Any();
    placement.set_value(PlacementStrategy::toString(config.getPlacementType()));
    context["placement"] = placement;

    auto linageType = google::protobuf::Any();
    linageType.set_value(toString(config.getLineageType()));
    context["linage"] = linageType;

    auto faultToleranceType = google::protobuf::Any();
    faultToleranceType.set_value(toString(config.getFaultToleranceType()));
    context["faultTolerance"] = faultToleranceType;

    std::string message = request.SerializeAsString();
    auto restMethod = web::http::methods::POST;
    auto path = "query/execute-query-ex";

    web::json::value jsonReturn;
    web::http::client::http_client_config cfg;
    cfg.set_timeout(requestTimeout);
    NES_DEBUG("RemoteClient::send: " << this->coordinatorHost << " " << this->coordinatorRESTPort);
    web::http::client::http_client client(getHostName(), cfg);
    client.request(restMethod, path, message)
        .then([](const web::http::http_response& response) {
            return response.extract_json();
        })
        .then([&jsonReturn](const pplx::task<web::json::value>& task) {
            NES_INFO("RemoteClient::send: received response");
            try {
                jsonReturn = task.get();
            } catch (const web::http::http_exception& e) {
                NES_ERROR("RemoteClient::send: error while setting return: " << e.what());
                throw ClientException("RemoteClient::send: error while setting return.");
            }
        })
        .wait();

    uint64_t queryId = 0;// Invalid query
    try {
        queryId = jsonReturn.at("queryId").as_integer();
    } catch (const web::json::json_exception& e) {
        NES_ERROR("RemoteClient::submitQuery: error while parsing queryId: " << e.what());
        throw ClientException("RemoteClient::submitQuery: error while parsing queryId:");
    }
    return queryId;
}

bool RemoteClient::testConnection() {
    auto restMethod = web::http::methods::GET;
    auto path = "connectivity/check";
    auto message = "";

    web::json::value jsonReturn;
    web::http::client::http_client_config cfg;
    cfg.set_timeout(requestTimeout);
    NES_DEBUG("RemoteClient::send: " << this->coordinatorHost << " " << this->coordinatorRESTPort);
    web::http::client::http_client client(getHostName(), cfg);
    client.request(restMethod, path, message)
        .then([](const web::http::http_response& response) {
            return response.extract_json();
        })
        .then([&jsonReturn](const pplx::task<web::json::value>& task) {
            NES_INFO("RemoteClient::send: received response");
            try {
                jsonReturn = task.get();
            } catch (const web::http::http_exception& e) {
                NES_ERROR("RemoteClient::send: error while setting return: " << e.what());
                throw ClientException("RemoteClient::send: error while setting return.");
            }
        })
        .wait();
    return jsonReturn.at("success").as_bool();
}

std::string RemoteClient::getQueryPlan(uint64_t queryId) {
    auto restMethod = web::http::methods::GET;
    auto path = "query/query-plan?queryId=" + std::to_string(queryId);
    auto message = "";

    web::json::value jsonReturn;
    web::http::client::http_client_config cfg;
    cfg.set_timeout(requestTimeout);
    NES_DEBUG("RemoteClient::send: " << this->coordinatorHost << " " << this->coordinatorRESTPort);
    web::http::client::http_client client(getHostName(), cfg);
    client.request(restMethod, path, message)
        .then([](const web::http::http_response& response) {
            return response.extract_json();
        })
        .then([&jsonReturn](const pplx::task<web::json::value>& task) {
            NES_INFO("RemoteClient::send: received response");
            try {
                jsonReturn = task.get();
            } catch (const web::http::http_exception& e) {
                NES_ERROR("RemoteClient::send: error while setting return: " << e.what());
                throw ClientException("RemoteClient::send: error while setting return.");
            }
        })
        .wait();
    return jsonReturn.serialize();
}

std::string RemoteClient::getQueryExecutionPlan(uint64_t queryId) {
    auto restMethod = web::http::methods::GET;
    auto path = "query/execution-plan?queryId=" + std::to_string(queryId);
    auto message = "";

    web::json::value jsonReturn;
    web::http::client::http_client_config cfg;
    cfg.set_timeout(requestTimeout);
    NES_DEBUG("RemoteClient::send: " << this->coordinatorHost << " " << this->coordinatorRESTPort);
    web::http::client::http_client client(getHostName(), cfg);
    client.request(restMethod, path, message)
        .then([](const web::http::http_response& response) {
            return response.extract_json();
        })
        .then([&jsonReturn](const pplx::task<web::json::value>& task) {
            NES_INFO("RemoteClient::send: received response");
            try {
                jsonReturn = task.get();
            } catch (const web::http::http_exception& e) {
                NES_ERROR("RemoteClient::send: error while setting return: " << e.what());
                throw ClientException("RemoteClient::send: error while setting return.");
            }
        })
        .wait();
    return jsonReturn.serialize();
}

bool RemoteClient::stopQuery(uint64_t queryId) {
    auto restMethod = web::http::methods::DEL;
    auto path = "query/stop-query?queryId=" + std::to_string(queryId);
    auto message = "";

    web::json::value jsonReturn;
    web::http::client::http_client_config cfg;
    cfg.set_timeout(requestTimeout);
    NES_DEBUG("RemoteClient::send: " << this->coordinatorHost << " " << this->coordinatorRESTPort);
    web::http::client::http_client client(getHostName(), cfg);
    client.request(restMethod, path, message)
        .then([](const web::http::http_response& response) {
            return response.extract_json();
        })
        .then([&jsonReturn](const pplx::task<web::json::value>& task) {
            NES_INFO("RemoteClient::send: received response");
            try {
                jsonReturn = task.get();
            } catch (const web::http::http_exception& e) {
                NES_ERROR("RemoteClient::send: error while setting return: " << e.what());
                throw ClientException("RemoteClient::send: error while setting return.");
            }
        })
        .wait();
    return jsonReturn.at("success").as_bool();
}

std::string RemoteClient::getTopology() {
    auto restMethod = web::http::methods::GET;
    auto path = "topology";
    auto message = "";

    web::json::value jsonReturn;
    web::http::client::http_client_config cfg;
    cfg.set_timeout(requestTimeout);
    NES_DEBUG("RemoteClient::send: " << this->coordinatorHost << " " << this->coordinatorRESTPort);
    web::http::client::http_client client(getHostName(), cfg);
    client.request(restMethod, path, message)
        .then([](const web::http::http_response& response) {
            return response.extract_json();
        })
        .then([&jsonReturn](const pplx::task<web::json::value>& task) {
            NES_INFO("RemoteClient::send: received response");
            try {
                jsonReturn = task.get();
            } catch (const web::http::http_exception& e) {
                NES_ERROR("RemoteClient::send: error while setting return: " << e.what());
                throw ClientException("RemoteClient::send: error while setting return.");
            }
        })
        .wait();
    return jsonReturn.serialize();
}

std::string RemoteClient::getQueries() {
    auto restMethod = web::http::methods::GET;
    auto path = "queryCatalog/allRegisteredQueries";
    auto message = "";

    web::json::value jsonReturn;
    web::http::client::http_client_config cfg;
    cfg.set_timeout(requestTimeout);
    NES_DEBUG("RemoteClient::send: " << this->coordinatorHost << " " << this->coordinatorRESTPort);
    web::http::client::http_client client(getHostName(), cfg);
    client.request(restMethod, path, message)
        .then([](const web::http::http_response& response) {
            return response.extract_json();
        })
        .then([&jsonReturn](const pplx::task<web::json::value>& task) {
            NES_INFO("RemoteClient::send: received response");
            try {
                jsonReturn = task.get();
            } catch (const web::http::http_exception& e) {
                NES_ERROR("RemoteClient::send: error while setting return: " << e.what());
                throw ClientException("RemoteClient::send: error while setting return.");
            }
        })
        .wait();
    return jsonReturn.serialize();
}

std::string RemoteClient::getQueries(const QueryStatus& status) {
    if (queryStatusToStringMap.find(status) == queryStatusToStringMap.end()) {
        throw InvalidArgumentException("status", "");
    }
    std::string queryStatus = queryStatusToStringMap[status];

    auto restMethod = web::http::methods::POST;
    auto path = "queries?status=" + queryStatus;
    auto message = "";

    web::json::value jsonReturn;
    web::http::client::http_client_config cfg;
    cfg.set_timeout(requestTimeout);
    NES_DEBUG("RemoteClient::send: " << this->coordinatorHost << " " << this->coordinatorRESTPort);
    web::http::client::http_client client(getHostName(), cfg);
    client.request(restMethod, path, message)
        .then([](const web::http::http_response& response) {
            return response.extract_json();
        })
        .then([&jsonReturn](const pplx::task<web::json::value>& task) {
            NES_INFO("RemoteClient::send: received response");
            try {
                jsonReturn = task.get();
            } catch (const web::http::http_exception& e) {
                NES_ERROR("RemoteClient::send: error while setting return: " << e.what());
                throw ClientException("RemoteClient::send: error while setting return.");
            }
        })
        .wait();
    return jsonReturn.serialize();
}

std::string RemoteClient::getPhysicalStreams() {
    auto restMethod = web::http::methods::GET;
    auto path = "streamCatalog/allPhysicalStream";
    auto message = "";

    web::json::value jsonReturn;
    web::http::client::http_client_config cfg;
    cfg.set_timeout(requestTimeout);
    NES_DEBUG("RemoteClient::send: " << this->coordinatorHost << " " << this->coordinatorRESTPort);
    web::http::client::http_client client(getHostName(), cfg);
    client.request(restMethod, path, message)
        .then([](const web::http::http_response& response) {
            return response.extract_json();
        })
        .then([&jsonReturn](const pplx::task<web::json::value>& task) {
            NES_INFO("RemoteClient::send: received response");
            try {
                jsonReturn = task.get();
            } catch (const web::http::http_exception& e) {
                NES_ERROR("RemoteClient::send: error while setting return: " << e.what());
                throw ClientException("RemoteClient::send: error while setting return.");
            }
        })
        .wait();
    return jsonReturn.serialize();
}

bool RemoteClient::addLogicalStream(const SchemaPtr schema, const std::string& streamName) {
    auto restMethod = web::http::methods::POST;
    auto path = "sourceCatalog/addLogicalStream-ex";

    auto serializableSchema = SchemaSerializationUtil::serializeSchema(schema, new SerializableSchema());
    SerializableNamedSchema request;
    request.set_streamname(streamName);
    request.set_allocated_schema(serializableSchema.get());
    std::string msg = request.SerializeAsString();
    request.release_schema();

    web::json::value jsonReturn;
    web::http::client::http_client_config cfg;
    cfg.set_timeout(requestTimeout);
    NES_DEBUG("RemoteClient::send: " << this->coordinatorHost << " " << this->coordinatorRESTPort);
    web::http::client::http_client client(getHostName(), cfg);
    client.request(restMethod, path, msg)
        .then([](const web::http::http_response& response) {
            return response.extract_json();
        })
        .then([&jsonReturn](const pplx::task<web::json::value>& task) {
            NES_INFO("RemoteClient::send: received response");
            try {
                jsonReturn = task.get();
            } catch (const web::http::http_exception& e) {
                NES_ERROR("RemoteClient::send: error while setting return: " << e.what());
                throw ClientException("RemoteClient::send: error while setting return.");
            }
        })
        .wait();
    return jsonReturn.at("Success").as_bool();
}

std::string RemoteClient::getLogicalStreams() {
    auto restMethod = web::http::methods::GET;
    auto path = "sourceCatalog/allLogicalStream";
    auto message = "";

    web::json::value jsonReturn;
    web::http::client::http_client_config cfg;
    cfg.set_timeout(requestTimeout);
    NES_DEBUG("RemoteClient::send: " << this->coordinatorHost << " " << this->coordinatorRESTPort);
    web::http::client::http_client client(getHostName(), cfg);
    client.request(restMethod, path, message)
        .then([](const web::http::http_response& response) {
            return response.extract_json();
        })
        .then([&jsonReturn](const pplx::task<web::json::value>& task) {
            NES_INFO("RemoteClient::send: received response");
            try {
                jsonReturn = task.get();
            } catch (const web::http::http_exception& e) {
                NES_ERROR("RemoteClient::send: error while setting return: " << e.what());
                throw ClientException("RemoteClient::send: error while setting return.");
            }
        })
        .wait();
    return jsonReturn.serialize();
}

std::string RemoteClient::getHostName() {
    return "http://" + coordinatorHost + ":" + std::to_string(coordinatorRESTPort) + "/v1/nes/";
}
}// namespace NES::Client
