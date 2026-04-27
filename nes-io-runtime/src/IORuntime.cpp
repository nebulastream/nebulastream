#include <nes-io-runtime-bindings/lib.h>
#include <IORuntime.hpp>
#include <IORuntimeBindings.hpp>
#include <Thread.hpp>

namespace NES
{

IORuntime::IORuntime() : handle(init_io_runtime(2, std::make_shared<ThreadInitializationContext>(NES::Thread::getThisWorkerNodeId())))
{
}

IORuntime::~IORuntime() = default;

} // namespace NES

/// C ABI hook used by Rust FFI bridges (sink/source bindings) to discover the
/// current thread's `IORuntimeHandle` without taking it as a parameter.
/// Returns the address of the Rust opaque handle, or 0 if no IORuntime is
/// associated with the calling thread (i.e., not a NES worker thread).
extern "C" std::size_t nes_current_io_runtime_address() noexcept
{
    if (auto* rt = NES::IORuntime::tryInstance())
    {
        return rt->rustHandleAddress();
    }
    return 0;
}
