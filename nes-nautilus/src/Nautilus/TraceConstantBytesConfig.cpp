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

#include <Nautilus/TraceConstantBytesConfig.hpp>

namespace NES
{

namespace
{
/// Deliberately a plain global, not thread_local: it is written once at worker startup and only
/// read afterwards, and a thread_local would be `__emutls`-resolved if this ever got pulled into
/// the JIT module. Defined out of line (not a header static) so no inlined body can capture it.
bool traceConstantBytes = true;
}

bool traceConstantBytesEnabled()
{
    return traceConstantBytes;
}

void setTraceConstantBytes(const bool enabled)
{
    traceConstantBytes = enabled;
}

}
