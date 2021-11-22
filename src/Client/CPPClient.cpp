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

#include <Client/CPPClient.hpp>
#include <GRPC/Serialization/QueryPlanSerializationUtil.hpp>
#include <SerializableQueryPlan.pb.h>

namespace NES {

CPPClient::CPPClient() { NES::setupLogging("nesClientStarter.log", NES::getDebugLevelFromString("LOG_DEBUG")); }

web::json::value CPPClient::deployQuery(const QueryPlanPtr& queryPlan,
                                        const std::string& coordinatorHost,
                                        const std::string& coordinatorRESTPort) {
    SubmitQueryRequest request;
    auto serializedQueryPlan = request.mutable_queryplan();
    QueryPlanSerializationUtil::serializeQueryPlan(queryPlan, serializedQueryPlan);
    std::string msg = request.SerializeAsString();

    web::json::value json_return;
    web::http::client::http_client client("http://" + coordinatorHost + ":" + coordinatorRESTPort + "/v1/nes/");
    client.request(web::http::methods::POST, "query/execute-query-ex", msg)
        .then([](const web::http::http_response& response) {
            NES_INFO("get first then");
            return response.extract_json();
        })
        .then([&json_return](const pplx::task<web::json::value>& task) {
            try {
                NES_INFO("post execute-query-ex: set return");
                json_return = task.get();
            } catch (const web::http::http_exception& e) {
                NES_ERROR("post execute-query-ex: error while setting return" << e.what());
            }
        })
        .wait();

    return json_return;
}
}// namespace NES