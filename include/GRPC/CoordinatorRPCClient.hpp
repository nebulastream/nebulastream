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

#ifndef NES_INCLUDE_GRPC_COORDINATORRPCCLIENT_HPP_
#define NES_INCLUDE_GRPC_COORDINATORRPCCLIENT_HPP_

#include <CoordinatorRPCService.grpc.pb.h>
#include <CoordinatorRPCService.pb.h>

#include <Catalogs/AbstractPhysicalStreamConfig.hpp>
#include <grpcpp/grpcpp.h>
#include <string>
using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;

namespace NES {

class NodeStats;

class RegisterPhysicalStreamResponse{
  public:
    RegisterPhysicalStreamResponse(bool successful, std::string physicalStreamName);

    bool wasPhysicalStreamRegistrationSuccessful();
    /**
     * @brief returns if the physical stream was successfully registered true, otherwise false
     */

    std::string getPhysicalStreamName();
    /**
     * @brief returns the name of the physical stream
     */

  private:
    bool successful;
    std::string physicalStreamName;
};

class PhysicalStreamConfig;
typedef std::shared_ptr<PhysicalStreamConfig> PhysicalStreamConfigPtr;

class CoordinatorRPCClient {
  public:
    CoordinatorRPCClient(std::string address);

    ~CoordinatorRPCClient();
    /**
     * @brief this methods registers a physical stream via the coordinator to multiple logical streams
     * @param configuration of the stream
     * @return bool indicating success and a new name in case there was a name conflict
     */
    RegisterPhysicalStreamResponse registerPhysicalStream(AbstractPhysicalStreamConfigPtr conf);

    /**
     * @brief this method registers logical streams via the coordinator
     * @param name of new logical stream
     * @param path to the file containing the schema
     * @return bool indicating the success of the log stream
     * @note the logical stream is not saved in the worker as it is maintained on the coordinator and all logical streams can be retrieved from the physical stream map locally, if we later need the data we can add a map
     */
    bool registerLogicalStream(std::string streamName, std::string filePath);

    /**
     * @brief this method removes the logical stream in the coordinator
     * @param logical stream to be deleted
     * @return bool indicating success of the removal
     */
    bool unregisterLogicalStream(std::string streamName);

    /**
     * @brief this method removes a physical stream from all logical streams in the coordinator
     * @param physical stream to be deleted
     * @return bool indicating success of the removal
     */
    bool unregisterPhysicalStream(std::string physicalStreamName);

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
    bool registerNode(std::string ipAddress,
                      int64_t grpcPort,
                      int64_t dataPort,
                      int16_t numberOfSlots,
                      NodeType type,
                      NodeStats nodeStats);

    /**
   * @brief method to unregister a node after the connection is established
   * @return bool indicating success
   */
    bool unregisterNode();

    /**
     * @brief method to get own id form server
     * @return own id as listed in the graph
     */
    uint64_t getId();

  private:
    uint64_t workerId;

    std::string address;
    std::shared_ptr<::grpc::Channel> rpcChannel;
    std::unique_ptr<CoordinatorRPCService::Stub> coordinatorStub;
};
typedef std::shared_ptr<CoordinatorRPCClient> CoordinatorRPCClientPtr;

}// namespace NES
#endif//
