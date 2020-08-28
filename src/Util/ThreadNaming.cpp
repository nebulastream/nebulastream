#include <Util/Logger.hpp>
#include <Util/ThreadNaming.hpp>
#include <cstring>
#include <stdarg.h>
#include <stdio.h>
#include <unistd.h>
#ifdef _POSIX_THREADS
#define HAS_POSIX_THREAD
#include <pthread.h>
#else
#error "Unsupported architecture"
#endif

namespace NES {
void setThreadName(const char* threadNameFmt, ...) {
    char buffer[128];
    va_list args;
    va_start(args, threadNameFmt);
    vsprintf(buffer, threadNameFmt, args);
    NES_ASSERT(std::strlen(buffer) <= 16, "Invalid name length");
#ifdef HAS_POSIX_THREAD
    pthread_setname_np(pthread_self(), buffer);
#endif
    va_end(args);
}
}// namespace NES