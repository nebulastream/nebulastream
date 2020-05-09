#ifndef NES_INCLUDE_GRPC_COORDINATORRPCCLIENTIMPL_HPP_
#define NES_INCLUDE_GRPC_COORDINATORRPCCLIENTIMPL_HPP_

#include <GRPC/Coordinator.grpc.pb.h>
#include <grpcpp/grpcpp.h>

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;

class CoordinatorRPCClientImpl {
  public:

    CoordinatorRPCClientImpl(std::shared_ptr<Channel> channel);

  private:
    std::unique_ptr<CoordinatorService::Stub> stub_;

};

#endif //



