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

#include <Configurations/Worker/PhysicalSourceTypes/PhysicalSourceType.hpp>
#include <CoordinatorRPCService.pb.h>
#include <GRPC/CoordinatorRPCClient.hpp>
#include <Health.grpc.pb.h>
#include <Util/Logger/Logger.hpp>
#include <Util/Mobility/GeoLocation.hpp>
#include <Util/Mobility/ReconnectPoint.hpp>
#include <Util/Mobility/Waypoint.hpp>
#include <Util/TopologyLinkInformation.hpp>
#include <filesystem>
#include <fstream>
#include <string>

namespace NES {
namespace detail {

/**
 * @brief Listener to process an Rpc Execution
 * @tparam ReturnType the return type of the rpc
 * @tparam RequestType type of the object sent as request
 * @tparam ReplyType type of the object expected as reply
 */
template<typename ReturnType, typename RequestType, typename ReplyType>
class RpcExecutionListener {
  public:
    virtual ~RpcExecutionListener() = default;

    /**
     * @brief Executes the rpc call calling the right grpc method
     * @return grpc invocation status code
     */
    virtual grpc::Status rpcCall(const RequestType&, ReplyType*) = 0;

    /**
     * @brief called upon the successful completion of the rpc with a reply
     * @return value to be returned on success
     */
    virtual ReturnType onSuccess(const ReplyType&) = 0;

    /**
     * @brief called upon an error during the rpc
     * @return true if a retry must be attempted
     */
    virtual bool onPartialFailure(const grpc::Status&) = 0;

    /**
     * @brief called upon the unsuccessful completion of the rpc with a reply
     * @return value to be returned on fail
     */
    virtual ReturnType onFailure() = 0;
};

/**
 * @brief This methods perform an rpc by sending a request and waiting for a reply.
 * It may retry in the case of a failure.
 * @tparam ReturnType the return type of the rpc
 * @tparam RequestType type of the object sent as request
 * @tparam ReplyType type of the object expected as reply
 * @param request the request to send
 * @param retryAttempts the number of retry attempts
 * @param backOffTime waiting time between retrial
 * @param listener the object that manages the rpc invocation
 * @return a return object in the case of success or failure
 */
template<typename ReturnType, typename RequestType, typename ReplyType>
[[nodiscard]] ReturnType processRpc(const RequestType& request,
                                    uint32_t retryAttempts,
                                    std::chrono::milliseconds backOffTime,
                                    RpcExecutionListener<ReturnType, RequestType, ReplyType>& listener) {

    for (uint32_t i = 0; i <= retryAttempts; ++i, backOffTime *= 2) {
        ReplyType reply;
        Status status = listener.rpcCall(request, &reply);

        if (status.ok()) {
            return listener.onSuccess(reply);
        } else if (listener.onPartialFailure(status)) {
            usleep(std::chrono::duration_cast<std::chrono::microseconds>(backOffTime).count());
        } else {
            break;
        }
    }
    return listener.onFailure();
}

/**
 * @brief This methods perform an rpc by sending a request and waiting for a reply.
 * It may retry in the case of a failure.
 * @tparam ReturnType the return type of the rpc
 * @tparam RequestType type of the object sent as request
 * @tparam ReplyType type of the object expected as reply
 * @param request the request to send
 * @param retryAttempts the number of retry attempts
 * @param backOffTime waiting time between retrial
 * @param func the call to the selected rpc
 * @return a return object in the case of success or failure
 */
template<typename ReturnType, typename RequestType, typename ReplyType>
[[nodiscard]] ReturnType processGenericRpc(const RequestType& request,
                                           uint32_t retryAttempts,
                                           std::chrono::milliseconds backOffTime,
                                           std::function<grpc::Status(ClientContext*, const RequestType&, ReplyType*)>&& func) {
    class GenericRpcListener : public detail::RpcExecutionListener<bool, RequestType, ReplyType> {
      public:
        explicit GenericRpcListener(std::function<grpc::Status(ClientContext*, const RequestType&, ReplyType*)>&& func)
            : func(std::move(func)) {}
        virtual ~GenericRpcListener() = default;

        grpc::Status rpcCall(const RequestType& request, ReplyType* reply) override {
            ClientContext context;

            return func(&context, request, reply);
        }
        bool onSuccess(const ReplyType& reply) override {
            NES_DEBUG("CoordinatorRPCClient::: status ok return success={}", reply.success());
            return reply.success();
        }
        bool onPartialFailure(const Status& status) override {
            NES_DEBUG(" CoordinatorRPCClient:: error={}: {}", status.error_code(), status.error_message());
            return false;
        }
        bool onFailure() override { return false; }

      private:
        std::function<grpc::Status(ClientContext*, const RequestType&, ReplyType*)> func;
    };

    auto listener = GenericRpcListener(std::move(func));
    return detail::processRpc(request, retryAttempts, backOffTime, listener);
}

}// namespace detail

std::shared_ptr<CoordinatorRPCClient>
CoordinatorRPCClient::create(const std::string& address, uint32_t rpcRetryAttempts, std::chrono::milliseconds rpcBackoff) {

    NES_DEBUG("CoordinatorRPCClient(): creating channels to address ={}", address);
    auto rpcChannel = grpc::CreateChannel(address, grpc::InsecureChannelCredentials());

    if (rpcChannel) {
        NES_DEBUG("CoordinatorRPCClient::connecting: channel successfully created");
    } else {
        NES_THROW_RUNTIME_ERROR("CoordinatorRPCClient::connecting error while creating channel");
    }

    return std::make_shared<CoordinatorRPCClient>(CoordinatorRPCService::NewStub(rpcChannel),
                                                  address,
                                                  rpcRetryAttempts,
                                                  rpcBackoff);
}

CoordinatorRPCClient::CoordinatorRPCClient(std::unique_ptr<CoordinatorRPCService::StubInterface>&& coordinatorStub,
                                           const std::string& address,
                                           uint32_t rpcRetryAttempts,
                                           std::chrono::milliseconds rpcBackoff)
    : address(address), coordinatorStub(std::move(coordinatorStub)), rpcRetryAttempts(rpcRetryAttempts), rpcBackoff(rpcBackoff) {}

bool CoordinatorRPCClient::registerPhysicalSources(const std::vector<PhysicalSourceTypePtr>& physicalSourceTypes) {
    NES_DEBUG("CoordinatorRPCClient::registerPhysicalSources: got {}"
              " physical sources to register for worker with id {}",
              physicalSourceTypes.size(),
              workerId);

    RegisterPhysicalSourcesRequest request;
    request.set_workerid(workerId.getRawValue());

    for (const auto& physicalSourceType : physicalSourceTypes) {
        PhysicalSourceDefinition* physicalSourceDefinition = request.add_physicalsourcetypes();
        physicalSourceDefinition->set_sourcetype(physicalSourceType->getSourceTypeAsString());
        physicalSourceDefinition->set_physicalsourcename(physicalSourceType->getPhysicalSourceName());
        physicalSourceDefinition->set_logicalsourcename(physicalSourceType->getLogicalSourceName());
    }

    class PhysicalSourceRegistrationRequestListener
        : public detail::RpcExecutionListener<bool, RegisterPhysicalSourcesRequest, RegisterPhysicalSourcesReply> {
      public:
        explicit PhysicalSourceRegistrationRequestListener(CoordinatorRPCService::StubInterface& coordinatorStub)
            : coordinatorStub(coordinatorStub) {}
        virtual ~PhysicalSourceRegistrationRequestListener() = default;

        grpc::Status rpcCall(const RegisterPhysicalSourcesRequest& request, RegisterPhysicalSourcesReply* reply) override {
            ClientContext context;
            return coordinatorStub.RegisterPhysicalSource(&context, request, reply);
        }

        bool onSuccess(const RegisterPhysicalSourcesReply& reply) override {
            if (!reply.success()) {
                for (const auto& result : reply.results()) {
                    if (result.success()) {
                        continue;
                    }
                    NES_ERROR("Could not register physical source {} at the coordinator! Reason: {}",
                              result.physicalsourcename(),
                              result.reason());
                }
            }
            return reply.success();
        }

        bool onPartialFailure(const Status& status) override {
            NES_WARNING(" CoordinatorRPCClient::registerPhysicalSources error={}: {}",
                        status.error_code(),
                        status.error_message());
            // Currently the Server always returns Ok. Thus any other error might be solved by a retry.
            return true;
        }

        bool onFailure() override {
            NES_ERROR("CoordinatorRPCClient::registerPhysicalSources Failed");
            return false;
        }
        CoordinatorRPCService::StubInterface& coordinatorStub;
    };

    auto listener = PhysicalSourceRegistrationRequestListener(*coordinatorStub);
    return detail::processRpc(request, rpcRetryAttempts, rpcBackoff, listener);
}

bool CoordinatorRPCClient::registerLogicalSource(const std::string& logicalSourceName, const std::string& filePath) {
    NES_DEBUG("CoordinatorRPCClient: registerLogicalSource {} with path{}", logicalSourceName, filePath);

    // Check if file can be found on system and read.
    std::filesystem::path path{filePath.c_str()};
    if (!std::filesystem::exists(path) || !std::filesystem::is_regular_file(path)) {
        NES_ERROR("CoordinatorRPCClient: file does not exits");
        throw Exceptions::RuntimeException("files does not exist");
    }

    /* Read file from file system. */
    std::ifstream ifs(path.string().c_str());
    std::string fileContent((std::istreambuf_iterator<char>(ifs)), (std::istreambuf_iterator<char>()));

    RegisterLogicalSourceRequest request;
    request.set_workerid(workerId.getRawValue());
    request.set_logicalsourcename(logicalSourceName);
    request.set_sourceschema(fileContent);
    NES_DEBUG("CoordinatorRPCClient::RegisterLogicalSourceRequest request={}", request.DebugString());

    return detail::processGenericRpc<bool, RegisterLogicalSourceRequest, RegisterLogicalSourceReply>(
        request,
        rpcRetryAttempts,
        rpcBackoff,
        [this](ClientContext* context, const RegisterLogicalSourceRequest& innerRequest, RegisterLogicalSourceReply* reply) {
            return coordinatorStub->RegisterLogicalSource(context, innerRequest, reply);
        });
}

bool CoordinatorRPCClient::unregisterPhysicalSource(const std::vector<PhysicalSourceTypePtr>& physicalSourceTypes) {

    for (const auto& physicalSourceType : physicalSourceTypes) {

        NES_DEBUG("CoordinatorRPCClient: unregisterPhysicalSource physical source {} from logical source",
                  physicalSourceType->toString());

        UnregisterPhysicalSourceRequest request;
        request.set_workerid(workerId.getRawValue());
        const std::string& physicalSourceName = physicalSourceType->getPhysicalSourceName();
        request.set_physicalsourcename(physicalSourceName);
        const std::string& logicalSourceName = physicalSourceType->getLogicalSourceName();
        request.set_logicalsourcename(logicalSourceName);
        NES_DEBUG("CoordinatorRPCClient::UnregisterPhysicalSourceRequest request={}", request.DebugString());

        bool success = detail::processGenericRpc<bool, UnregisterPhysicalSourceRequest, UnregisterPhysicalSourceReply>(
            request,
            rpcRetryAttempts,
            rpcBackoff,
            [this](ClientContext* context,
                   const UnregisterPhysicalSourceRequest& innerRequest,
                   UnregisterPhysicalSourceReply* reply) {
                return coordinatorStub->UnregisterPhysicalSource(context, innerRequest, reply);
            });

        if (!success) {
            NES_ERROR("Unable to unregister physical source {} for the logical source {}.", physicalSourceName, logicalSourceName)
            return success;
        }
    }
    return true;
}

bool CoordinatorRPCClient::unregisterLogicalSource(const std::string& logicalSourceName) {
    NES_DEBUG("CoordinatorRPCClient: unregisterLogicalSource source{}", logicalSourceName);

    UnregisterLogicalSourceRequest request;
    request.set_workerid(workerId.getRawValue());
    request.set_logicalsourcename(logicalSourceName);
    NES_DEBUG("CoordinatorRPCClient::UnregisterLogicalSourceRequest request={}", request.DebugString());

    return detail::processGenericRpc<bool, UnregisterLogicalSourceRequest, UnregisterLogicalSourceReply>(
        request,
        rpcRetryAttempts,
        rpcBackoff,
        [this](ClientContext* context, const UnregisterLogicalSourceRequest& innerRequest, UnregisterLogicalSourceReply* reply) {
            return coordinatorStub->UnregisterLogicalSource(context, innerRequest, reply);
        });
}

bool CoordinatorRPCClient::addParent(WorkerId parentId) {
    NES_DEBUG("CoordinatorRPCClient: addParent parentId={} workerId={}", parentId, workerId);

    AddParentRequest request;
    request.set_parentid(parentId.getRawValue());
    request.set_childid(workerId.getRawValue());
    NES_DEBUG("CoordinatorRPCClient::AddParentRequest request={}", request.DebugString());

    return detail::processGenericRpc<bool, AddParentRequest, AddParentReply>(
        request,
        rpcRetryAttempts,
        rpcBackoff,
        [this](ClientContext* context, const AddParentRequest& innerRequest, AddParentReply* reply) {
            return coordinatorStub->AddParent(context, innerRequest, reply);
        });
}

bool CoordinatorRPCClient::replaceParent(WorkerId oldParentId, WorkerId newParentId) {
    NES_DEBUG("CoordinatorRPCClient: replaceParent oldParentId={} newParentId={} workerId={}",
              oldParentId,
              newParentId,
              workerId);

    ReplaceParentRequest request;
    request.set_childid(workerId.getRawValue());
    request.set_oldparent(oldParentId.getRawValue());
    request.set_newparent(newParentId.getRawValue());
    NES_DEBUG("CoordinatorRPCClient::replaceParent request={}", request.DebugString());

    class ReplaceParentListener : public detail::RpcExecutionListener<bool, ReplaceParentRequest, ReplaceParentReply> {
      public:
        std::unique_ptr<CoordinatorRPCService::StubInterface>& coordinatorStub;

        explicit ReplaceParentListener(std::unique_ptr<CoordinatorRPCService::StubInterface>& coordinatorStub)
            : coordinatorStub(coordinatorStub) {}

        virtual ~ReplaceParentListener() = default;

        Status rpcCall(const ReplaceParentRequest& request, ReplaceParentReply* reply) override {
            ClientContext context;

            return coordinatorStub->ReplaceParent(&context, request, reply);
        }
        bool onSuccess(const ReplaceParentReply& reply) override {
            NES_DEBUG("CoordinatorRPCClient::removeAsParent: status ok return success={}", reply.success());
            return reply.success();
        }
        bool onPartialFailure(const Status& status) override {
            NES_DEBUG(" CoordinatorRPCClient::removeAsParent error={}: {}", status.error_code(), status.error_message());
            return false;
        }
        bool onFailure() override { return false; }
    };

    auto listener = ReplaceParentListener{coordinatorStub};

    return detail::processRpc(request, rpcRetryAttempts, rpcBackoff, listener);
}

WorkerId CoordinatorRPCClient::getId() const { return workerId; }

bool CoordinatorRPCClient::removeParent(WorkerId parentId) {
    NES_DEBUG("CoordinatorRPCClient: removeAsParent parentId{} workerId={}", parentId, workerId);

    RemoveParentRequest request;
    request.set_parentid(parentId.getRawValue());
    request.set_childid(workerId.getRawValue());
    NES_DEBUG("CoordinatorRPCClient::RemoveParentRequest request={}", request.DebugString());

    class RemoveParentListener : public detail::RpcExecutionListener<bool, RemoveParentRequest, RemoveParentReply> {
      public:
        std::unique_ptr<CoordinatorRPCService::StubInterface>& coordinatorStub;

        explicit RemoveParentListener(std::unique_ptr<CoordinatorRPCService::StubInterface>& coordinatorStub)
            : coordinatorStub(coordinatorStub) {}

        virtual ~RemoveParentListener() = default;

        Status rpcCall(const RemoveParentRequest& request, RemoveParentReply* reply) override {
            ClientContext context;

            return coordinatorStub->RemoveParent(&context, request, reply);
        }
        bool onSuccess(const RemoveParentReply& reply) override {
            NES_DEBUG("CoordinatorRPCClient::removeAsParent: status ok return success={}", reply.success());
            return reply.success();
        }
        bool onPartialFailure(const Status& status) override {
            NES_DEBUG(" CoordinatorRPCClient::removeAsParent error={}: {}", status.error_code(), status.error_message());
            return false;
        }
        bool onFailure() override { return false; }
    };

    auto listener = RemoveParentListener{coordinatorStub};

    return detail::processRpc(request, rpcRetryAttempts, rpcBackoff, listener);
}

bool CoordinatorRPCClient::unregisterNode() {
    NES_DEBUG("CoordinatorRPCClient::unregisterNode workerId={}", workerId);

    UnregisterWorkerRequest request;
    request.set_workerid(workerId.getRawValue());
    NES_DEBUG("CoordinatorRPCClient::unregisterNode request={}", request.DebugString());

    class UnRegisterNodeListener : public detail::RpcExecutionListener<bool, UnregisterWorkerRequest, UnregisterWorkerReply> {
      public:
        std::unique_ptr<CoordinatorRPCService::StubInterface>& coordinatorStub;

        explicit UnRegisterNodeListener(std::unique_ptr<CoordinatorRPCService::StubInterface>& coordinatorStub)
            : coordinatorStub(coordinatorStub) {}

        virtual ~UnRegisterNodeListener() = default;

        Status rpcCall(const UnregisterWorkerRequest& request, UnregisterWorkerReply* reply) override {
            ClientContext context;

            return coordinatorStub->UnregisterWorker(&context, request, reply);
        }
        bool onSuccess(const UnregisterWorkerReply& reply) override {
            NES_DEBUG("CoordinatorRPCClient::unregisterNode: status ok return success={}", reply.success());
            return reply.success();
        }
        bool onPartialFailure(const Status& status) override {
            NES_DEBUG(" CoordinatorRPCClient::unregisterNode error={}: {}", status.error_code(), status.error_message());
            return false;
        }
        bool onFailure() override { return false; }
    };

    auto listener = UnRegisterNodeListener{coordinatorStub};

    return detail::processRpc(request, rpcRetryAttempts, rpcBackoff, listener);
}

bool CoordinatorRPCClient::registerWorker(const RegisterWorkerRequest& registrationRequest) {

    NES_DEBUG("CoordinatorRPCClient::RegisterNodeRequest request={}", registrationRequest.DebugString());

    class RegisterWorkerListener : public detail::RpcExecutionListener<bool, RegisterWorkerRequest, RegisterWorkerReply> {
      public:
        WorkerId& workerId;
        std::unique_ptr<CoordinatorRPCService::StubInterface>& coordinatorStub;

        explicit RegisterWorkerListener(WorkerId& workerId,
                                        std::unique_ptr<CoordinatorRPCService::StubInterface>& coordinatorStub)
            : workerId(workerId), coordinatorStub(coordinatorStub) {}

        virtual ~RegisterWorkerListener() = default;

        Status rpcCall(const RegisterWorkerRequest& request, RegisterWorkerReply* reply) override {
            ClientContext context;

            return coordinatorStub->RegisterWorker(&context, request, reply);
        }
        bool onSuccess(const RegisterWorkerReply& reply) override {
            workerId = WorkerId(reply.workerid());
            return true;
        }
        bool onPartialFailure(const Status& status) override {
            NES_ERROR(" CoordinatorRPCClient::registerWorker error={}:{}", status.error_code(), status.error_message());
            switch (status.error_code()) {
                case grpc::UNIMPLEMENTED:
                case grpc::UNAVAILABLE: {
                    return true;// partial failure ok to continue
                }
                default: {
                    return false;// partial failure not ok to continue
                }
            }
        }
        bool onFailure() override { return false; }
    };

    auto listener = RegisterWorkerListener{workerId, coordinatorStub};
    return detail::processRpc(registrationRequest, rpcRetryAttempts, rpcBackoff, listener);
}

bool CoordinatorRPCClient::notifyQueryFailure(SharedQueryId sharedQueryId,
                                              DecomposedQueryId decomposedQueryId,
                                              WorkerId pWorkerId,
                                              OperatorId operatorId,
                                              std::string errorMsg) {

    // create & fill the protobuf
    QueryFailureNotification request;
    request.set_queryid(sharedQueryId.getRawValue());
    request.set_decomposedqueryid(decomposedQueryId.getRawValue());
    request.set_workerid(pWorkerId.getRawValue());
    request.set_operatorid(operatorId.getRawValue());
    request.set_errormsg(errorMsg);

    class NotifyQueryFailureListener
        : public detail::RpcExecutionListener<bool, QueryFailureNotification, QueryFailureNotificationReply> {
      public:
        std::unique_ptr<CoordinatorRPCService::StubInterface>& coordinatorStub;

        explicit NotifyQueryFailureListener(std::unique_ptr<CoordinatorRPCService::StubInterface>& coordinatorStub)
            : coordinatorStub(coordinatorStub) {}

        virtual ~NotifyQueryFailureListener() = default;

        Status rpcCall(const QueryFailureNotification& request, QueryFailureNotificationReply* reply) override {
            ClientContext context;

            return coordinatorStub->NotifyQueryFailure(&context, request, reply);
        }
        bool onSuccess(const QueryFailureNotificationReply&) override {
            NES_DEBUG("WorkerRPCClient::NotifyQueryFailure: status ok");
            return true;
        }
        bool onPartialFailure(const Status&) override { return true; }
        bool onFailure() override { return false; }
    };

    auto listener = NotifyQueryFailureListener{coordinatorStub};

    return detail::processRpc(request, rpcRetryAttempts, rpcBackoff, listener);
}

std::vector<std::pair<WorkerId, Spatial::DataTypes::Experimental::GeoLocation>>
CoordinatorRPCClient::getNodeIdsInRange(const Spatial::DataTypes::Experimental::GeoLocation& geoLocation, double radius) {
    if (!geoLocation.isValid()) {
        return {};
    }
    GetNodesInRangeRequest request;
    NES::Spatial::Protobuf::GeoLocation* pCoordinates = request.mutable_geolocation();
    pCoordinates->set_lat(geoLocation.getLatitude());
    pCoordinates->set_lng(geoLocation.getLongitude());
    request.set_radius(radius);
    GetNodesInRangeReply reply;
    ClientContext context;

    Status status = coordinatorStub->GetNodesInRange(&context, request, &reply);

    std::vector<std::pair<WorkerId, Spatial::DataTypes::Experimental::GeoLocation>> nodesInRange;
    for (const auto& workerLocation : *reply.mutable_nodes()) {
        nodesInRange.emplace_back(workerLocation.id(), workerLocation.geolocation());
    }
    return nodesInRange;
}

bool CoordinatorRPCClient::checkCoordinatorHealth(std::string healthServiceName) const {
    std::shared_ptr<::grpc::Channel> chan = grpc::CreateChannel(address, grpc::InsecureChannelCredentials());
    std::unique_ptr<grpc::health::v1::Health::StubInterface> workerStub = grpc::health::v1::Health::NewStub(chan);

    grpc::health::v1::HealthCheckRequest request;
    request.set_service(healthServiceName);
    grpc::health::v1::HealthCheckResponse response;
    ClientContext context;
    Status status = workerStub->Check(&context, request, &response);

    if (status.ok()) {
        NES_TRACE("CoordinatorRPCClient::checkHealth: status ok return success={}", response.status());
        return response.status();
    } else {
        NES_ERROR("CoordinatorRPCClient::checkHealth error={}:{}", status.error_code(), status.error_message());
        return response.status();
    }
}

bool CoordinatorRPCClient::notifyEpochTermination(uint64_t timestamp, uint64_t querySubPlanId) {
    EpochBarrierPropagationNotification request;
    request.set_timestamp(timestamp);
    request.set_queryid(querySubPlanId);
    EpochBarrierPropagationReply reply;
    ClientContext context;
    Status status = coordinatorStub->NotifyEpochTermination(&context, request, &reply);
    if (status.ok()) {
        NES_DEBUG("WorkerRPCClient::PropagatePunctuation: status ok");
        return true;
    }
    return false;
}

bool CoordinatorRPCClient::sendErrors(WorkerId pWorkerId, std::string errorMsg) {

    // create & fill the protobuf
    SendErrorsMessage request;
    request.set_workerid(pWorkerId.getRawValue());
    request.set_errormsg(errorMsg);

    ErrorReply reply;

    ClientContext context;

    Status status = coordinatorStub->SendErrors(&context, request, &reply);

    if (status.ok()) {
        NES_DEBUG("WorkerRPCClient::SendErrors: status ok");
        return true;
    }
    return false;
}

bool CoordinatorRPCClient::checkAndMarkForSoftStop(SharedQueryId sharedQueryId,
                                                   DecomposedQueryId decomposedQueryId,
                                                   OperatorId sourceId) {

    //Build request
    RequestSoftStopMessage requestSoftStopMessage;
    requestSoftStopMessage.set_queryid(sharedQueryId.getRawValue());
    requestSoftStopMessage.set_decomposedqueryid(decomposedQueryId.getRawValue());
    requestSoftStopMessage.set_sourceid(sourceId.getRawValue());

    //Build response
    StopRequestReply stopRequestReply;

    ClientContext context;
    coordinatorStub->RequestSoftStop(&context, requestSoftStopMessage, &stopRequestReply);

    //return the response
    return stopRequestReply.success();
}

bool CoordinatorRPCClient::notifySourceStopTriggered(SharedQueryId sharedQueryId,
                                                     DecomposedQueryId decomposedQueryId,
                                                     OperatorId sourceId,
                                                     Runtime::QueryTerminationType queryTermination) {
    if (queryTermination == Runtime::QueryTerminationType::Reconfiguration) {
        //todo #5133: implement separate call to inform coordinator of termination by reconfig
        return true;
    }
    NES_ASSERT2_FMT(queryTermination == Runtime::QueryTerminationType::Graceful, "Wrong termination requested");

    //Build request
    SoftStopTriggeredMessage softStopTriggeredMessage;
    softStopTriggeredMessage.set_queryid(sharedQueryId.getRawValue());
    softStopTriggeredMessage.set_decomposedqueryid(decomposedQueryId.getRawValue());
    softStopTriggeredMessage.set_sourceid(sourceId.getRawValue());

    //Build response
    SoftStopTriggeredReply softStopTriggeredReply;

    ClientContext context;
    coordinatorStub->notifySourceStopTriggered(&context, softStopTriggeredMessage, &softStopTriggeredReply);

    //return the response
    return softStopTriggeredReply.success();
}

bool CoordinatorRPCClient::notifySoftStopCompleted(SharedQueryId sharedQueryId, DecomposedQueryId decomposedQueryId) {
    //Build request
    SoftStopCompletionMessage softStopCompletionMessage;
    softStopCompletionMessage.set_queryid(sharedQueryId.getRawValue());
    softStopCompletionMessage.set_decomposedqueryid(decomposedQueryId.getRawValue());

    //Build response
    SoftStopCompletionReply softStopCompletionReply;

    ClientContext context;
    coordinatorStub->NotifySoftStopCompleted(&context, softStopCompletionMessage, &softStopCompletionReply);

    //return the response
    return softStopCompletionReply.success();
}

bool CoordinatorRPCClient::sendReconnectPrediction(
    const std::vector<Spatial::Mobility::Experimental::ReconnectPoint>& addPredictions,
    const std::vector<Spatial::Mobility::Experimental::ReconnectPoint>& removePredictions) {
    ClientContext context;
    SendScheduledReconnectRequest request;
    SendScheduledReconnectReply reply;

    request.set_deviceid(workerId.getRawValue());
    auto pointsToAdd = request.mutable_addreconnects();
    for (const auto addedPrediction : addPredictions) {
        auto addedPoint = pointsToAdd->Add();
        addedPoint->set_id(addedPrediction.newParentId.getRawValue());
        addedPoint->set_time(addedPrediction.expectedTime);
        auto pointLocation = addedPoint->mutable_geolocation();
        pointLocation->set_lat(addedPrediction.pointGeoLocation.getLatitude());
        pointLocation->set_lat(addedPrediction.pointGeoLocation.getLongitude());
    }
    for (const auto removedPrediction : removePredictions) {
        auto addedPoint = pointsToAdd->Add();
        addedPoint->set_id(removedPrediction.newParentId.getRawValue());
        addedPoint->set_time(removedPrediction.expectedTime);
        auto pointLocation = addedPoint->mutable_geolocation();
        pointLocation->set_lat(removedPrediction.pointGeoLocation.getLatitude());
        pointLocation->set_lat(removedPrediction.pointGeoLocation.getLongitude());
    }

    coordinatorStub->SendScheduledReconnect(&context, request, &reply);
    return reply.success();
}

bool CoordinatorRPCClient::sendLocationUpdate(const Spatial::DataTypes::Experimental::Waypoint& locationUpdate) {
    ClientContext context;
    LocationUpdateRequest request;
    LocationUpdateReply reply;

    request.set_workerid(workerId.getRawValue());

    NES::Spatial::Protobuf::Waypoint* waypoint = request.mutable_waypoint();
    NES::Spatial::Protobuf::GeoLocation* geoLocation = waypoint->mutable_geolocation();
    geoLocation->set_lat(locationUpdate.getLocation().getLatitude());
    geoLocation->set_lng(locationUpdate.getLocation().getLongitude());

    if (locationUpdate.getTimestamp()) {
        waypoint->set_timestamp(locationUpdate.getTimestamp().value());
    }
    coordinatorStub->SendLocationUpdate(&context, request, &reply);
    return reply.success();
}

std::vector<WorkerId> CoordinatorRPCClient::getParents(WorkerId pWorkerId) {
    ClientContext context;
    GetParentsRequest request;
    GetParentsReply reply;

    request.set_nodeid(pWorkerId.getRawValue());

    coordinatorStub->GetParents(&context, request, &reply);
    std::vector<WorkerId> parentWorkerIds;
    for (auto parentId : reply.parentids()) {
        parentWorkerIds.push_back(WorkerId(parentId));
    }
    return parentWorkerIds;
}

bool CoordinatorRPCClient::relocateTopologyNode(const std::vector<TopologyLinkInformation>& removedTopologyLinks,
                                                const std::vector<TopologyLinkInformation>& addedTopologyLinks) {
    ClientContext context;
    NodeRelocationRequest request;
    NodeRelocationReply reply;

    for (const auto& removedLink : removedTopologyLinks) {
        auto removed = request.add_removedlinks();
        removed->set_upstream(removedLink.upstreamTopologyNode.getRawValue());
        removed->set_downstream(removedLink.downstreamTopologyNode.getRawValue());
    }
    for (const auto& addedLink : addedTopologyLinks) {
        auto added = request.add_addedlinks();
        added->set_upstream(addedLink.upstreamTopologyNode.getRawValue());
        added->set_downstream(addedLink.downstreamTopologyNode.getRawValue());
    }
    coordinatorStub->RelocateTopologyNode(&context, request, &reply);
    return reply.success();
}

bool CoordinatorRPCClient::notifyCheckpoint(SharedQueryId sharedQueryId, std::vector<std::tuple<std::tuple<uint64_t, uint64_t>, uint64_t>> watermarkList) {
    ClientContext context;
    CheckPointList checkpointList;
    CheckPointRespone reply;
    checkpointList.set_sharedqueryid(sharedQueryId.getRawValue());
    checkpointList.set_checkpointwatermark(minWatermark);
    coordinatorStub->NotifyCheckPoint(&context, checkpointList, &reply);
    return reply.success();

}
}// namespace NES
