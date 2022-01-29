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

#ifndef NES_MIGRATEQUERYREQUEST_HPP
#define NES_MIGRATEQUERYREQUEST_HPP
#include <WorkQueues/RequestTypes/Request.hpp>
#include <Topology/TopologyNodeId.hpp>
#include <Phases/MigrationType.hpp>
#include <memory>
#include <string>

namespace NES::Experimental {

    class MigrateQueryRequest;
    typedef std::shared_ptr<MigrateQueryRequest> MigrateQueryRequestPtr;

    /**
     * @breif this request is used to migrate a query sub plan
     */
    class MigrateQueryRequest : public Request{

    public:

        static MigrateQueryRequestPtr create(QueryId queryId,TopologyNodeId nodeId, MigrationType::Value migrationType);

         std::string toString() override;

        /**
         * @brief gets the Migration Type for this Query Migration Request
         * @return MigrationType
         */
        MigrationType::Value getMigrationType();

        /**
         * @brief gets the topology node on which the query can be found
         * @return topology node id
         */
        TopologyNodeId getTopologyNode();

    private:
        explicit MigrateQueryRequest(QueryId queryId,TopologyNodeId nodeId, MigrationType::Value migrationType);

        TopologyNodeId nodeId;
        MigrationType::Value migrationType;

    };
} // namespace NES
#endif //NES_MIGRATEQUERYREQUEST_HPP
