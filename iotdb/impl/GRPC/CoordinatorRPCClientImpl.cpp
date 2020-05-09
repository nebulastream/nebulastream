#include <GRPC/CoordinatorRPCClientImpl.hpp>

CoordinatorRPCClientImpl::CoordinatorRPCClientImpl(std::shared_ptr<Channel> channel)
: stub_(CoordinatorService::NewStub(channel))
{
    //do something
}