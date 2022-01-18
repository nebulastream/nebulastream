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

#ifndef NES_INCLUDE_GRPC_COORDINATOR_RPC_CLIENT_HPP_
#define NES_INCLUDE_GRPC_COORDINATOR_RPC_CLIENT_HPP_

#include <Catalogs/AbstractPhysicalStreamConfig.hpp>
#include <CoordinatorRPCService.grpc.pb.h>
#include <grpcpp/grpcpp.h>
#include <optional>
#include <string>

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;

namespace NES {

class StaticNesMetrics;
using StaticNesMetricsPtr = std::shared_ptr<StaticNesMetrics>;

class PhysicalStreamConfig;
using PhysicalSourcePtr = std::shared_ptr<PhysicalStreamConfig>;

class CoordinatorRPCClient {
  public:
    explicit CoordinatorRPCClient(const std::string& address);

    /**
     * @brief this methods registers a physical stream via the coordinator to a logical stream
     * @param configuration of the stream
     * @return bool indicating success
     */
    bool registerPhysicalStream(const AbstractPhysicalStreamConfigPtr& conf);

    /**
     * @brief this method registers logical streams via the coordinator
     * @param name of new logical stream
     * @param path to the file containing the schema
     * @return bool indicating the success of the log stream
     * @note the logical stream is not saved in the worker as it is maintained on the coordinator and all logical streams can be retrieved from the physical stream map locally, if we later need the data we can add a map
     */
    bool registerLogicalStream(const std::string& streamName, const std::string& filePath);

    /**
     * @brief this method removes the logical stream in the coordinator
     * @param logical stream to be deleted
     * @return bool indicating success of the removal
     */
    bool unregisterLogicalStream(const std::string& streamName);

    /**
     * @brief this method removes a physical stream from a logical stream in the coordinator
     * @param logical stream to be deleted
     * @return bool indicating success of the removal
     */
    bool unregisterPhysicalStream(const std::string& logicalStreamName, const std::string& physicalStreamName);

    /**
     * @brief method to add a new parent to an existing node
     * @param newParentId
     * @return bool indicating success
     */
    bool addParent(uint64_t parentId);

    /*
    * @brief method to replace old with new parent
    * @param oldParentId
    * @param newParentId
    * @return bool indicating success
    */
    bool replaceParent(uint64_t oldParentId, uint64_t newParentId);

    /**
     * @brief method to remove a parent from a node
     * @param newParentId
     * @return bool indicating success
     */
    bool removeParent(uint64_t parentId);

    /**
     * @brief method to register a node after the connection is established
     * @param ipAddress: where this node is listening
     * @param grpcPort: the grpc port of the node
     * @param dataPort: the data port of the node
     * @param numberOfSlots: processing slots capacity
     * @param type: of this node, e.g., sensor or worker
     * @param nodeProperties
     * @return bool indicating success
     */
    bool registerNode(const std::string& ipAddress,
                      int64_t grpcPort,
                      int64_t dataPort,
                      int16_t numberOfSlots,
                      NodeType type,
                      std::optional<StaticNesMetricsPtr> staticNesMetrics);

    /**
   * @brief method to unregister a node after the connection is established
   * @return bool indicating success
   */
    bool unregisterNode();

    /**
     * @brief method to get own id form server
     * @return own id as listed in the graph
     */
    uint64_t getId() const;

    /**
     * @brief method to let the Coordinator know of the failure of a query
     * @param queryId: Query Id of failed Query
     * @param subQueryId: subQuery Id of failed Query
     * @param workerId: workerId where the Query failed
     * @param operatorId: operator Id of failed Query
     * @param errorMsg: more information about failure of the Query
     * @return bool indicating success
     */
    bool notifyQueryFailure(uint64_t queryId, uint64_t subQueryId, uint64_t workerId, uint64_t operatorId, std::string errorMsg);

  private:
    uint64_t workerId;
    std::string address;
    std::shared_ptr<::grpc::Channel> rpcChannel;
    std::unique_ptr<CoordinatorRPCService::Stub> coordinatorStub;
};
using CoordinatorRPCClientPtr = std::shared_ptr<CoordinatorRPCClient>;

}// namespace NES
#endif// NES_INCLUDE_GRPC_COORDINATOR_RPC_CLIENT_HPP_
