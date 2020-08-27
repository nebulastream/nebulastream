#include <GRPC/GRPCLibraryInitializer.hpp>
#include <grpcpp/grpcpp.h>
namespace NES {
GrpcLibraryInizializer::GrpcLibraryInizializer() {
    grpc_init();
}

GrpcLibraryInizializer::~GrpcLibraryInizializer() {
    grpc_shutdown();
}
}// namespace NES

static NES::GrpcLibraryInizializer singleton;
