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

#include <Nautilus/Interface/FunctionCall.hpp>
#include <Nautilus/Tracing/TraceContext.hpp>
#include <regex>

namespace NES::Nautilus {

void traceFunctionCall(Nautilus::Tracing::ValueRef& resultRef, const std::vector<Nautilus::Tracing::InputVariant>& arguments) {
    if (auto ctx = Nautilus::Tracing::TraceContext::getIfActive()) {
        //auto operation = Nautilus::Tracing::TraceOperation(Nautilus::Tracing::CALL, resultRef, arguments);
        ctx->traceFunctionCall(resultRef, arguments);
    }
}

void traceVoidFunctionCall(const std::vector<Nautilus::Tracing::InputVariant>& arguments) {
    if (auto ctx = Nautilus::Tracing::TraceContext::getIfActive()) {
        // auto operation = Nautilus::Tracing::TraceOperation(Nautilus::Tracing::CALL, arguments);
        ctx->traceFunctionCall(arguments);
    }
}

}// namespace NES::Nautilus