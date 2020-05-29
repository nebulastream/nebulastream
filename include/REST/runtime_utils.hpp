#pragma once

#include <execinfo.h>
#include <unistd.h>

namespace NES {
class RuntimeUtils {
  public:
    static void printStackTrace() {
        //Size of the array storing the stack trace in case of the error
        const int MAX_CALLSTACK = 100;
        void* callstack[MAX_CALLSTACK];
        int frames;

        // get void*'s for all entries on the stack...
        frames = backtrace(callstack, MAX_CALLSTACK);

        // print out all the frames to stderr...
        backtrace_symbols_fd(callstack, frames, STDERR_FILENO);
    }
};
}// namespace NES