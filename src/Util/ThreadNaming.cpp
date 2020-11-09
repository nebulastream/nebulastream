/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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
#include <cstring>
#include <stdarg.h>
#include <stdio.h>
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
    auto sz = std::min<size_t>(15, std::strlen(buffer));
    std::strncpy(resized_buffer, buffer, sz);
    std::string thName(resized_buffer);
    //this will add the thread name in the log
    MDC::put("threadName", thName);

    resized_buffer[sz] = 0;
#ifdef HAS_POSIX_THREAD
    pthread_setname_np(pthread_self(), resized_buffer);
#endif
    va_end(args);
}
}// namespace NES