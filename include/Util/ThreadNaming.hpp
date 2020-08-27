#ifndef NES_INCLUDE_UTIL_THREADNAMING_HPP_
#define NES_INCLUDE_UTIL_THREADNAMING_HPP_

#include <stdarg.h>
#include <stdio.h>
#include <thread>
#include <unistd.h>
#ifdef _POSIX_THREADS
#define HAS_POSIX_THREAD
#include <pthread.h>
#else
#error "Unsupported architecture"
#endif

void setThreadName(const char* threadNameFmt, ...) {
    char buffer[256];
    va_list args;
    va_start(args, threadNameFmt);
    vsprintf(buffer, threadNameFmt, args);
#ifdef HAS_POSIX_THREAD
    pthread_setname_np(pthread_self(), buffer);
#endif
    va_end(args);
}

#endif//NES_INCLUDE_UTIL_THREADNAMING_HPP_
