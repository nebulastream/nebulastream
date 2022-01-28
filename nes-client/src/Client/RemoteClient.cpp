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

#include <Client/RemoteClient.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <API/Query.hpp>
#include <Client/ClientException.hpp>
#include <GRPC/Serialization/QueryPlanSerializationUtil.hpp>
#include <SerializableQueryPlan.pb.h>
#include <Util/Logger.hpp>
#include <cpprest/http_client.h>

namespace NES::Client {

RemoteClient::RemoteClient(const std::string& coordinatorHost, uint16_t coordinatorRESTPort, std::chrono::seconds requestTimeout)
    : coordinatorHost(coordinatorHost), coordinatorRESTPort(coordinatorRESTPort), requestTimeout(requestTimeout) {
    NES::setupLogging("nesClientStarter.log", NES::getDebugLevelFromString("LOG_DEBUG"));

    if (coordinatorHost.empty()) {
        throw ClientException("host name for coordinator is empty");
    }
}

int64_t RemoteClient::submitQuery(const Query& query, const QueryConfig config) {
    auto queryPlan = query.getQueryPlan();
    SubmitQueryRequest request;
    auto* serializedQueryPlan = request.mutable_queryplan();
    QueryPlanSerializationUtil::serializeQueryPlan(queryPlan, serializedQueryPlan, true);

    auto& context = *request.mutable_context();

    auto placement = google::protobuf::Any();
    placement.set_value(toString(config.getPlacementType()));
    context["placement"] = placement;

    auto linageType = google::protobuf::Any();
    linageType.set_value(toString(config.getLineageType()));
    context["linage"] = linageType;

    auto faultToleranceType = google::protobuf::Any();
    faultToleranceType.set_value(toString(config.getFaultToleranceType()));
    context["faultTolerance"] = faultToleranceType;

    std::string msg = request.SerializeAsString();

    web::json::value jsonResult;
    web::http::client::http_client_config cfg;
    cfg.set_timeout(requestTimeout);
    web::http::client::http_client client(getHostName(), cfg);
    client.request(web::http::methods::POST, "query/execute-query-ex", msg)
        .then([](const web::http::http_response& response) {
            return response.extract_json();
        })
        .then([&jsonResult](const pplx::task<web::json::value>& task) {
            NES_INFO("CPPClient::submitQuery: received response");
            try {
                jsonResult = task.get();
            } catch (const web::http::http_exception& e) {
                NES_ERROR("CPPClient::submitQuery: error while setting return: " << e.what());
                throw ClientException("CPPClient::submitQuery: error while setting return.");
            }
        })
        .wait();
    int64_t queryId = -1;
    try {
        queryId = jsonResult.at("queryId").as_integer();
    } catch (const web::json::json_exception& e) {
        NES_ERROR("CPPClient::submitQuery: error while parsing queryId: " << e.what());
        throw ClientException("CPPClient::submitQuery: error while parsing queryId:");
    }
    return queryId;
}

std::string RemoteClient::getHostName() {
    return "http://" + coordinatorHost + ":" + std::to_string(coordinatorRESTPort) + "/v1/nes/";
}
}// namespace NES::Client
