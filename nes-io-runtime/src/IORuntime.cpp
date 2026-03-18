#include <IORuntime.hpp>
#include <IORuntimeBindings.hpp>
#include <nes-io-runtime-bindings/lib.h>
#include <Thread.hpp>

namespace NES
{

IORuntime& IORuntime::get()
{
    static IORuntime instance;
    return instance;
}

IORuntime::IORuntime()
{
    auto context = std::make_shared<ThreadInitializationContext>(ThreadInitializationContext{.runtimeIdx = 0, .workerId = NES::Thread::getThisWorkerNodeId()});
    runtimeIdx = init_io_runtime(2, context);
    context->runtimeIdx = runtimeIdx;
}

IORuntime::~IORuntime()
{
    shutdown_runtime(runtimeIdx);
}

} // namespace NES
