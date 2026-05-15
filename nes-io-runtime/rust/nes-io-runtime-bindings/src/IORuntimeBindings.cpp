#include <cstddef>
#include <string>

#include <nes-io-runtime-bindings/lib.h>
#include <rust/cxx.h>
#include <Thread.hpp>

void init_thread(std::shared_ptr<ThreadInitializationContext> context)
{
    PRECONDITION(context != nullptr, "context must not be null");
    for (const auto& hook : context->threadInitHooks)
    {
        hook();
    }
}
