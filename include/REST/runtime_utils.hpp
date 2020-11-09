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