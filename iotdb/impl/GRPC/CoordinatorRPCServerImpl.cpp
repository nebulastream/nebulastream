#include <GRPC/CoordinatorRPCServerImpl.hpp>

Status CoordinatorRPCServerImpl::RegisterNode(ServerContext* context, const RegisterNodeRequest* request,
                                              RegisterNodeReply* reply) {
    return Status::OK;
}

Status CoordinatorRPCServerImpl::UnregisterNode(ServerContext* context, const UnregisterNodeRequest* request,
                                                UnregisterNodeReply* reply) {
    return Status::OK;
}

Status CoordinatorRPCServerImpl::RegisterPhysicalStream(ServerContext* context,
                                                        const RegisterPhysicalStreamRequest* request,
                                                        RegisterPhysicalStreamReply* reply) {
    return Status::OK;
}

Status CoordinatorRPCServerImpl::UnregisterPhysicalStream(ServerContext* context,
                                                          const UnregisterPhysicalStreamRequest* request,
                                                          UnregisterPhysicalStreamReply* reply) {
    return Status::OK;
}

Status CoordinatorRPCServerImpl::RegisterLogicalStream(ServerContext* context,
                                                       const RegisterLogicalStreamRequest* request,
                                                       RegisterLogicalStreamReply* reply) {
    return Status::OK;
}

Status CoordinatorRPCServerImpl::UnregisterLogicalStream(ServerContext* context,
                                                         const UnregisterLogicalStreamRequest* request,
                                                         UnregisterLogicalStreamReply* reply) {
    return Status::OK;
}

Status CoordinatorRPCServerImpl::AddParent(ServerContext* context, const AddParentRequest* request,
                                           AddParentReply* reply) {
    return Status::OK;
}

Status CoordinatorRPCServerImpl::RemoveParent(ServerContext* context, const RemoveParentRequest* request,
                                              RemoveParentReply* reply) {
    return Status::OK;
}