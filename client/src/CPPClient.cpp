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

#include <API/Query.hpp>
#include <GRPC/Serialization/QueryPlanSerializationUtil.hpp>
#include <SerializableQueryPlan.pb.h>
#include <Util/Logger.hpp>
#include <client/include/CPPClient.hpp>

#include <cpprest/http_client.h>

namespace NES {

CPPClient::CPPClient(const std::string& coordinatorHost, const std::string& coordinatorRESTPort)
    : coordinatorHost(coordinatorHost), coordinatorRESTPort(coordinatorRESTPort) {
    NES::setupLogging("nesClientStarter.log", NES::getDebugLevelFromString("LOG_DEBUG"));
}

int64_t CPPClient::submitQuery(const QueryPlanPtr& queryPlan, const std::string& placement) {
    SubmitQueryRequest request;
    auto serializedQueryPlan = request.mutable_queryplan();
    QueryPlanSerializationUtil::serializeQueryPlan(queryPlan, serializedQueryPlan, true);

    auto& context = *request.mutable_context();
    auto placement_buffer = google::protobuf::Any();
    placement_buffer.set_value(placement);
    context["placement"] = placement_buffer;

    std::string msg = request.SerializeAsString();

    web::json::value json_return;
    web::http::client::http_client client("http://" + coordinatorHost + ":" + coordinatorRESTPort + "/v1/nes/");
    client.request(web::http::methods::POST, "query/execute-query-ex", msg)
        .then([](const web::http::http_response& response) {
            return response.extract_json();
        })
        .then([&json_return](const pplx::task<web::json::value>& task) {
            NES_INFO("CPPClient::submitQuery: recieved response");
            try {
                json_return = task.get();
            } catch (const web::http::http_exception& e) {
                NES_ERROR("CPPClient::submitQuery: error while setting return: " << e.what());
            }
        })
        .wait();
    int64_t queryId = -1;
    try {
        queryId = json_return.at("queryId").as_integer();
    } catch (const web::json::json_exception& e) {
        NES_ERROR("CPPClient::submitQuery: error while parsing queryId: " << e.what());
    }
    return queryId;
}
}// namespace NES
