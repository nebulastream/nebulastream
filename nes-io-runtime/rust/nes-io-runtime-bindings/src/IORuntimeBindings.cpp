#include <cstddef>
#include <string>

#include <nes-io-runtime-bindings/lib.h>
#include <rust/cxx.h>
#include <Thread.hpp>

void init_thread(std::shared_ptr<ThreadInitializationContext> context)
{
    PRECONDITION(context != nullptr, "context must not be null");
    NES::Thread::initializeThread(context->host, fmt::format("IO-{}", context->counter++));
    for (const auto& hook : context->threadInitHooks)
    {
        hook();
    }
}
