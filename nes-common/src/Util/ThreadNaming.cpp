/*
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

#include <Util/Logger.hpp>
#include <Util/ThreadNaming.hpp>
#include <algorithm>
#include <cstdarg>
#include <cstdio>
#include <cstring>
#include <unistd.h>
#ifdef _POSIX_THREADS
#define HAS_POSIX_THREAD
#include <Util/Logger.hpp>
#include <pthread.h>
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
    // We need to copy the null terminator from buffer if any and if strlen(...) < 15 in order to not
    // display uninitialized memory. Therefore, increase strlen by one.
    auto sz = std::min<uint64_t>(15, std::strlen(buffer) + 1);
    std::strncpy(resized_buffer, buffer, sz);
    resized_buffer[sz] = 0;
    std::string thName(resized_buffer);
    //this will add the thread name in the log
    MDC::put("threadName", thName);

#ifdef HAS_POSIX_THREAD
#ifdef __linux__
    pthread_setname_np(pthread_self(), resized_buffer);
#elif defined(__APPLE__)
    pthread_setname_np(resized_buffer);
#else
#error "Unsupported OS"
#endif
#else
#error "Unsupported Thread Library"
#endif
    va_end(args);
}
}// namespace NES