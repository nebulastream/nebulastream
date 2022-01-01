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

namespace NES {

class Query;
class QueryPlan;
using QueryPlanPtr = std::shared_ptr<QueryPlan>;

/**
 * @brief CPP client to deploy queries over the REST API
 */
class CPPClient {
  public:
    /* @brief constructor of the client
     * @param coordinator host e.g. 127.0.0.1
     * @param coordinator REST port e.g. 8081
     */
    CPPClient(const std::string& coordinatorHost = "127.0.0.1", const std::string& coordinatorRESTPort = "8081");

    /*
     * @brief Deploy a query to the coordinator
     * @param query plan to deploy
     * @param operator placement e.g. "BottomUp" or "TopDown"
     * @return query id of deployed query. -1 if deployment failed
     */
    int64_t submitQuery(const Query& query, const std::string& placement = "BottomUp");

  private:
    const std::string coordinatorHost;
    const std::string coordinatorRESTPort;
};
}// namespace NES

#endif//NES_INCLUDE_CLIENT_CPPCLIENT_HPP_