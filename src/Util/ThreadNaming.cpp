#include <Util/Logger.hpp>
#include <Util/ThreadNaming.hpp>
#include <algorithm>
#include <cstring>
#include <stdarg.h>
#include <stdio.h>
#include <unistd.h>
#ifdef _POSIX_THREADS
#define HAS_POSIX_THREAD
#include <pthread.h>
#include <Util/Logger.hpp>
#include <string>
#else
#error "Unsupported architecture"
#endif

namespace NES {
void setThreadName(const char* threadNameFmt, ...) {
    char buffer[128];
    char resized_buffer[16];
    va_list args;
    va_start(args, threadNameFmt);
    vsprintf(buffer, threadNameFmt, args);
    auto sz = std::min<size_t>(15, std::strlen(buffer));
    std::strncpy(resized_buffer, buffer, sz);
    std::string thName(resized_buffer);
    MDC::put("threadName", thName);

    resized_buffer[sz] = 0;
#ifdef HAS_POSIX_THREAD
    pthread_setname_np(pthread_self(), resized_buffer);
#endif
    va_end(args);
}
}// namespace NES