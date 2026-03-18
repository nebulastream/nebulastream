#include <cstddef>
#include <string>
#include <rust/cxx.h>
#include <Thread.hpp>
#include <nes-io-runtime-bindings/lib.h>


void init_thread(std::shared_ptr<ThreadInitializationContext> context)
{
    if (context)
    {
        NES::Thread::initializeThread(context->workerId, "nes-io-rt-" + std::to_string(context->runtimeIdx));
    }
}
