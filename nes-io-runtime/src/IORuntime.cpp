#include <nes-io-runtime-bindings/lib.h>
#include <IORuntime.hpp>
#include <IORuntimeBindings.hpp>
#include <Thread.hpp>

namespace NES
{

IORuntime::IORuntime()
{
    auto context = std::make_shared<ThreadInitializationContext>(0, NES::Thread::getThisWorkerNodeId());
    runtimeIdx = init_io_runtime(2, context);
    context->runtimeIdx = runtimeIdx;
}

IORuntime::~IORuntime()
{
    shutdown_runtime(runtimeIdx);
}

} // namespace NES
