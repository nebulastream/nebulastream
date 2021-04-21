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
//
// Created by balint on 20.04.21.
//

#ifndef NES_MIGRATEWITHBUFFERREQUEST_HPP
#define NES_MIGRATEWITHBUFFERREQUEST_HPP

#include "NESRequest.hpp"
#include <Topology/TopologyNodeId.hpp>
#include <memory>
namespace NES {

class MigrateQueryRequest;
typedef std::shared_ptr<MigrateQueryRequest> MigrateQueryRequestPtr;

class MigrateQueryRequest : public NESRequest{

  public:

    static MigrateQueryRequestPtr create(QueryId queryId,TopologyNodeId nodeId, bool withBuffer);

    bool isWithBuffer();

    std::string toString() override;

    TopologyNodeId getTopologyNode();

  private:
    explicit MigrateQueryRequest(QueryId queryId,TopologyNodeId nodeId, bool withBuffer);

    bool withBuffer;
    TopologyNodeId nodeId;

};
} // namespace NES

#endif//NES_MIGRATEWITHBUFFERREQUEST_HPP