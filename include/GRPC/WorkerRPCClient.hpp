#ifndef NES_INCLUDE_GRPC_WORKERRPCCLIENT_HPP_
#define NES_INCLUDE_GRPC_WORKERRPCCLIENT_HPP_

#include <WorkerRPCService.grpc.pb.h>
#include <WorkerRPCService.pb.h>

#include <Catalogs/PhysicalStreamConfig.hpp>
#include <Topology/NESTopologyEntry.hpp>
#include <grpcpp/grpcpp.h>
#include <string>

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;

namespace NES {

class OperatorNode;
typedef std::shared_ptr<OperatorNode> OperatorNodePtr;

class WorkerRPCClient {

  public:
    WorkerRPCClient();

    /**
    * @brief deploy registers and starts a query
    * @param queryId
    * @param new query plan as eto
    * @return true if succeeded, else false
    */
    bool deployQuery(std::string address, std::string executableTransferObject);

    /**
     * @brief undeploy stops and undeploy a query
     * @param queryId to undeploy
     * @return true if succeeded, else false
     */
    bool undeployQuery(std::string address, uint64_t queryId);

    /**
    * @brief gregisters a query
    * @param queryId
    * @param query plan to register as eto
    * @return true if succeeded, else false
    */
    bool registerQuery(std::string address, uint64_t queryId, OperatorNodePtr queryOperators);

    /**
     * @brief ungregisters a query
     * @param queryIdto unregister query
     * @return true if succeeded, else false
     */
    bool unregisterQuery(std::string address, uint64_t queryId);

    /**
     * @brief method to start a already deployed query
     * @note if query is not deploy, false is returned
     * @param queryId to start
     * @return bool indicating success
     */
    bool startQuery(std::string address, uint64_t queryId);

    /**
     * @brief method to stop a query
     * @param queryId to stop
     * @return bool indicating success
     */
    bool stopQuery(std::string address, uint64_t queryId);

  private:
};
typedef std::shared_ptr<WorkerRPCClient> WorkerRPCClientPtr;

}// namespace NES

#endif//NES_INCLUDE_GRPC_WORKERRPCCLIENT_HPP_
