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

#ifndef NES_INCLUDE_CLIENT_CPPCLIENT_HPP_
#define NES_INCLUDE_CLIENT_CPPCLIENT_HPP_

#include <GRPC/Serialization/QueryPlanSerializationUtil.hpp>
#include <SerializableQueryPlan.pb.h>
#include <Util/Logger.hpp>
#include <cpprest/http_client.h>

/* TODO: No using in HPP files */
using namespace web;

namespace NES {

/**
 * @brief
 */
class CPPClient {
  public:
    /*
     * @brief Deploy a query to the coordinator
     * @param query plan to deploy
     * @param coordinator host e.g. 127.0.0.1
     * @param coordinator port e.g. 8081
     */
    static void deployQuery(const QueryPlanPtr& queryPlan,
                            const std::string& coordinatorHost = "127.0.0.1",
                            const std::string& coordinatorPort = "8081") {
        SubmitQueryRequest request = SubmitQueryRequest();
        auto queryPlanSerialized = request.mutable_queryplan();

        QueryPlanSerializationUtil::serializeQueryPlan(queryPlan, queryPlanSerialized);
        NES_TRACE("WorkerRPCClient:registerQuery -> " << request.DebugString());
        NES_INFO("QueryPlanSerializationUtil::serializeQueryPlan(queryPlan, queryPlanSerialized);");
        request.set_allocated_queryplan(queryPlanSerialized);
        NES_INFO("protobufMessage.set_allocated_queryplan(queryPlanSerialized);");
        std::string msg = request.SerializeAsString();
        NES_INFO("std::string msg = protobufMessage.SerializeAsString();");

        web::json::value json_return;
        web::http::client::http_client client("http://" + coordinatorHost + ":" + coordinatorPort + "/v1/nes/");
        NES_INFO("web::http::client::http_client client");
        client.request(web::http::methods::POST, "query/execute-query-ex", msg)
            .then([](const web::http::http_response& response) {
                auto i = response.extract_json();
                NES_INFO("response.extract_json ");
                return i;
            })
            .then([&json_return](const pplx::task<web::json::value>& task) {
                try {
                    json_return = task.get();
                    NES_INFO("json_return " << json_return);
                } catch (const web::http::http_exception& e) {
                    NES_ERROR("error " << e.what());
                }
            })
            .wait();

        NES_DEBUG("deployQuery: status =" << json_return);
    }
};
}// namespace NES

#endif//NES_INCLUDE_CLIENT_CPPCLIENT_HPP_
