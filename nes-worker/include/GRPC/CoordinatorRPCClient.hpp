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

#ifndef NES_WORKER_INCLUDE_GRPC_COORDINATORRPCCLIENT_HPP_
#define NES_WORKER_INCLUDE_GRPC_COORDINATORRPCCLIENT_HPP_

#include <CoordinatorRPCService.grpc.pb.h>
#include <Identifiers/Identifiers.hpp>
#include <Network/NetworkForwardRefs.hpp>
#include <Operators/LogicalOperators/Network/NesPartition.hpp>
#include <Runtime/QueryTerminationType.hpp>
#include <Util/TimeMeasurement.hpp>
#include <grpcpp/grpcpp.h>
#include <optional>
#include <string>

namespace NES {
class TopologyLinkInformation;
}
using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;

namespace NES {
class PhysicalSourceType;
using PhysicalSourceTypePtr = std::shared_ptr<PhysicalSourceType>;

namespace Monitoring {
class RegistrationMetrics;
}// namespace Monitoring

namespace Spatial::DataTypes::Experimental {
class GeoLocation;
class Waypoint;
}// namespace Spatial::DataTypes::Experimental

namespace Spatial::Mobility::Experimental {
struct ReconnectPoint;
}

/**
 * @brief This class provides utility to interact with NES coordinator over RPC interface.
 */
class CoordinatorRPCClient {

  public:
    /**
     * @brief Factory method, to create the coordinator RPCClient. Internally a GRPC stub is created.
     * @param address
     * @param retryAttempts: number of attempts for connecting
     * @param backOffTimeMs: backoff time to wait after a failed connection attempt
     */
    static std::shared_ptr<CoordinatorRPCClient> create(const std::string& address,
                                                        uint32_t rpcRetryAttempts = 10,
                                                        std::chrono::milliseconds rpcBackoff = std::chrono::milliseconds(50));

    /**
     * @brief Constructor accepting a GRPC Stub.
     * @param coordinatorStub RPCStub handles grpc requests
     * @param address
     * @param retryAttempts: number of attempts for connecting
     * @param backOffTimeMs: backoff time to wait after a failed connection attempt
     */
    explicit CoordinatorRPCClient(std::unique_ptr<CoordinatorRPCService::StubInterface>&& coordinatorStub,
                                  const std::string& address,
                                  uint32_t rpcRetryAttempts = 10,
                                  std::chrono::milliseconds rpcBackoff = std::chrono::milliseconds(50));
    /**
     * @brief this methods registers physical sources provided by the node at the coordinator
     * @param physicalSourceTypes list of physical sources to register
     * @return bool indicating success
     */
    bool registerPhysicalSources(const std::vector<PhysicalSourceTypePtr>& physicalSourceTypes);

    /**
     * @brief this method registers logical source via the coordinator
     * @param logicalSourceName of new logical source name
     * @param filePath to the file containing the schema
     * @return bool indicating the success of the log source
     * @note the logical source is not saved in the worker as it is maintained on the coordinator and all logical source can be
     * retrieved from the physical source map locally, if we later need the data we can add a map
     */
    bool registerLogicalSource(const std::string& logicalSourceName, const std::string& filePath);

 bool requestQueryOffload(WorkerId originWorkerId, SharedQueryId sharedQueryId, DecomposedQueryId decomposedQueryId, WorkerId workerId);

 bool requestSubQueryRemoval(WorkerId originWorkerId, SharedQueryId sharedQueryId, DecomposedQueryId decomposedQueryId);
    /**
     * @brief this method removes the logical source in the coordinator
     * @param logicalSourceName name of the logical source to be deleted
     * @return bool indicating success of the removal
     */
    bool unregisterLogicalSource(const std::string& logicalSourceName);

    /**
     * @brief this method removes a physical source from a logical source in the coordinator
     * @param physicalSourceTypes vector physical sources
     * @return bool indicating success of the removal
     */
    bool unregisterPhysicalSource(const std::vector<PhysicalSourceTypePtr>& physicalSourceTypes);

    /**
     * @brief method to add a new parent to an existing node
     * @param newParentId
     * @return bool indicating success
     */
    bool addParent(WorkerId parentId);

    /**
     * @brief method to replace old with new parent
     * @param oldParentId id of the old parent
     * @param newParentId id of the new parent
     * @return bool indicating success
     */
    bool replaceParent(WorkerId oldParentId, WorkerId newParentId);

    /**
     * @brief method to remove a parent from a node
     * @param parentId: id of the parent to be removed
     * @return bool indicating success
     */
    bool removeParent(WorkerId parentId);

    /**
     * @brief method to register a node after the connection is established
     * @param registrationRequest: request containing necessary input for worker registration
     * @return bool indicating success
     */
    bool registerWorker(const RegisterWorkerRequest& registrationRequest);

    /**
     * @brief method to check if the coordinator is alive
     * @param healthServiceName name of the service
     * @return bool indicating success
     */
    bool checkCoordinatorHealth(std::string healthServiceName) const;

    /**
   * @brief method to unregister a node after the connection is established
   * @return bool indicating success
   */
    bool unregisterNode();

    /**
     * @brief method to get own id form server
     * @return own id as listed in the graph
     */
    WorkerId getId() const;

    /**
     * @brief method to let the Coordinator know of the failure of a query
     * @param sharedQueryId: Query Id of failed Query
     * @param subQueryId: subQuery Id of failed Query
     * @param workerId: workerId where the Query failed
     * @param operatorId: operator Id of failed Query
     * @param errorMsg: more information about failure of the Query
     * @return bool indicating success
     */
    bool notifyQueryFailure(SharedQueryId sharedQueryId,
                            DecomposedQueryId decomposedQueryId,
                            WorkerId workerId,
                            OperatorId operatorId,
                            std::string errorMsg);

    /**
      * @brief method to propagate new epoch timestamp to coordinator
      * @param timestamp: max timestamp of current epoch
      * @param queryId: identifies what query sends punctuation
      * @return bool indicating success
      */
    bool notifyEpochTermination(uint64_t timestamp, uint64_t queryId);

    /**
     * @brief method to send new checkpoint to coordinator
     * @param nesPartitionId: the partition where tuple belongs
     * @param binaryData: storage containing tupels as binary
     * @return bool indicating success
     */
    bool sendCheckpoint(uint64_t nesPartitionId, std::vector<char> binaryData);

    /**
        * @brief method to trim checkpoints from coordinator
        * @param nesPartition: the partition where tuple belongs
        * @param timestamp: indicates which checkpoints to trim
        * @return bool indicating success
        */
    bool trimCheckpoint(uint64_t nesPartitionId, uint64_t timestamp);

    /**
     * Experimental
     * @brief Method to get all field nodes (field nodes = non-mobile nodes with a specified geographical location) within a certain range around a geographical point
     * @param geoLocation: center of the query area
     * @param radius: radius in km to define query area
     * @return list of node IDs and their corresponding fixed coordinates as Location objects
     */
    std::vector<std::pair<WorkerId, NES::Spatial::DataTypes::Experimental::GeoLocation>>
    getNodeIdsInRange(const NES::Spatial::DataTypes::Experimental::GeoLocation& geoLocation, double radius);

    /**
     * @brief method to let the Coordinator know of errors and exceptions
     * @param workerId
     * @param errorMsg
     * @return bool indicating success
     */
    bool sendErrors(WorkerId workerId, std::string errorMsg);

    /**
     * Checks and mark the query for soft stop
     * @param sharedQueryId : the query id for which soft stop to be performed
     * @return true if coordinator marks the query for soft stop else false
     */
    bool checkAndMarkForSoftStop(SharedQueryId sharedQueryId, DecomposedQueryId decomposedQueryId, OperatorId sourceId);

    /**
     * Notify coordinator that for a subquery plan the soft stop is triggered or not
     * @param sharedQueryId: the query id to which the subquery plan belongs to
     * @param decomposedQueryId: the query sub plan id
     * @param sourceId: the source id
     * @return true if coordinator successfully recorded the information else false
     */
    bool notifySourceStopTriggered(SharedQueryId sharedQueryId,
                                   DecomposedQueryId decomposedQueryId,
                                   OperatorId sourceId,
                                   Runtime::QueryTerminationType queryTermination);

    /**
     * Notify coordinator that for a subquery plan the soft stop is completed or not
     * @param sharedQueryId: the query id to which the subquery plan belongs to
     * @param decomposedQueryId: the query sub plan id
     * @return true if coordinator successfully recorded the information else false
     */
    bool notifySoftStopCompleted(SharedQueryId sharedQueryId, DecomposedQueryId decomposedQueryId);

    /**
     * @brief this method is used by a mobile worker to inform the coordinator that location or time of the next expected reconnect
     * have changed or that the worker expects a reconnect to a different parent than the previously scheduled one
     * @param removePredictions old predictions which should be removed
     * @param addPredictions new predictions to be added
     * @return true if the information was successfully received at coordinator side
     */
    bool sendReconnectPrediction(const std::vector<NES::Spatial::Mobility::Experimental::ReconnectPoint>& addPredictions,
                                 const std::vector<NES::Spatial::Mobility::Experimental::ReconnectPoint>& removePredictions);

    /**
     * @brief this method can be called by a mobile worker to tell the coordinator, that the mobile devices position has changed
     * @param locationUpdate a tuple containing the mobile devices location and a timestamp indicating when the device was located
     * at the transmitted position
     * @return true if the information has benn succesfully processed
     */
    bool sendLocationUpdate(const NES::Spatial::DataTypes::Experimental::Waypoint& locationUpdate);

    /**
     * @brief the function queries for the ids of the parents of a node with a given id
     * @param workerId: the id of the worker whose parents are requested
     * @return a vector containing the ids of all of the nodes parents
     */
    std::vector<WorkerId> getParents(WorkerId workerId);

    /**
     * @brief modify the topology by removing and adding links and then rerun an incremental placement for queries that were
     * sending data over one of the removed links
     * @param removedTopologyLinks a list of topology links to remove
     * @param addedTopologyLinks a list or topology links to add
     * @return true on success
     */
    bool relocateTopologyNode(const std::vector<TopologyLinkInformation>& removedTopologyLinks,
                              const std::vector<TopologyLinkInformation>& addedTopologyLinks);

  private:
    WorkerId workerId = INVALID_WORKER_NODE_ID;
    std::string address;
    std::shared_ptr<::grpc::Channel> rpcChannel;
    std::unique_ptr<CoordinatorRPCService::StubInterface> coordinatorStub;
    uint32_t rpcRetryAttempts;
    std::chrono::milliseconds rpcBackoff;
};
using CoordinatorRPCClientPtr = std::shared_ptr<CoordinatorRPCClient>;

}// namespace NES
#endif// NES_WORKER_INCLUDE_GRPC_COORDINATORRPCCLIENT_HPP_
