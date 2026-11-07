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

#pragma once

#include <cstdint>

namespace NES
{

/// Where an in-process UDF runs its Python, for each worker thread.
enum class UdfInterpreter : uint8_t
{
    /// The one interpreter of the process, whose GIL all threads share.
    SHARED,
    /// A subinterpreter of the thread, with its own GIL. Extension modules that do not support subinterpreters, like numpy and
    /// pyarrow, fail to load.
    SUBINTERPRETER,
    /// A copy of the bridge and its interpreter in a link namespace of the thread (dlmopen), with its own GIL and copies of every
    /// module. glibc has room for about a dozen such namespaces.
    DLMOPEN,
};

}
