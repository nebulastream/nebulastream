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
#include <Util/SourceMode.hpp>

namespace NES {

SourceMode::Value SourceMode::getFromString(const std::string sourceMode) {
    if (sourceMode == "emptyBuffer") {
        return SourceMode::EMPTY_BUFFER;
    } else if (sourceMode == "wrapBuffer") {
        return SourceMode::WRAP_BUFFER;
    } else if (sourceMode == "copyBuffer") {
        return SourceMode::COPY_BUFFER;
    } else if (sourceMode == "copyBufferSimdRte") {
        return SourceMode::COPY_BUFFER_SIMD_RTE;
    } else if (sourceMode == "cacheCopy") {
        return SourceMode::CACHE_COPY;
    } else if (sourceMode == "copyBufferSimdApex") {
        return SourceMode::COPY_BUFFER_SIMD_APEX;
    } else {
        NES_THROW_RUNTIME_ERROR("mode not supported " << sourceMode);
    }
}

std::string SourceMode::toString(const Value sourceMode) {

    switch (sourceMode) {
        case EMPTY_BUFFER: return "emptyBuffer";
        case WRAP_BUFFER: return "wrapBuffer";
        case COPY_BUFFER: return "copyBuffer";
        case COPY_BUFFER_SIMD_RTE: return "copyBufferSimdRte";
        case CACHE_COPY: return "cacheCopy";
        case COPY_BUFFER_SIMD_APEX: return "copyBufferSimdApex";
    }
}

}// namespace NES
