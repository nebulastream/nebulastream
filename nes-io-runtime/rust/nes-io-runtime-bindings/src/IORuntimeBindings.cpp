#include <cstddef>
#include <string>
#include <nes-io-runtime-bindings/lib.h>
#include <rust/cxx.h>
#include <Thread.hpp>

void init_thread(std::shared_ptr<ThreadInitializationContext> context)
{
    if (context)
    {
        NES::Thread::initializeThread(context->workerId, fmt::format("io-rt-{}", context->thread_id.fetch_add(1)));
    }
}
