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
#include <Nautilus/Interface/DataTypes/Any.hpp>
#include <Nautilus/Interface/DataTypes/Boolean.hpp>
#include <Nautilus/Tracing/TraceContext.hpp>
#include <Nautilus/Tracing/TraceUtil.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES::Nautilus::Tracing::TraceUtil {

[[maybe_unused, nodiscard]] bool inTracer() {
    if (auto traceContext = TraceContext::get()) {
        return true;
    }
    return false;
}

[[maybe_unused, nodiscard]] bool inInterpreter() { return !inTracer(); }

void traceStoreOperation(const Tracing::ValueRef& memRef, const Tracing::ValueRef& valueRef) {
    if (auto ctx = Nautilus::Tracing::TraceContext::getIfActive()) {
        return ctx->traceStore(memRef, valueRef);
    }
}

bool traceBoolOperation(const AnyPtr& value, const Nautilus::Tracing::ValueRef& sourceRef) {
    if (value->isType<Boolean>()) {
        if (auto ctx = Nautilus::Tracing::TraceContext::getIfActive()) {
            return ctx->traceCMP(sourceRef);
        } else {
            auto boolValue = cast<Boolean>(value);
            return boolValue->getValue();
        }
    }
    NES_THROW_RUNTIME_ERROR("Can't evaluate bool on non Boolean value: " << value->toString());
}

void traceAssignmentOperation(const Nautilus::Tracing::ValueRef& targetRef, const Nautilus::Tracing::ValueRef& sourceRef) {
    if (auto ctx = Tracing::TraceContext::getIfActive()) {
        ctx->traceAssignmentOperation(targetRef, sourceRef);
    }
};

void traceBinaryOperation(const Nautilus::Tracing::OpCode& op,
                          const Nautilus::Tracing::ValueRef& resultRef,
                          const Nautilus::Tracing::ValueRef& leftRef,
                          const Nautilus::Tracing::ValueRef& rightRef) {
    if (auto ctx = Tracing::TraceContext::getIfActive()) {
        ctx->traceBinaryOperation(op, leftRef, rightRef, resultRef);
    }
}

void traceUnaryOperation(const Nautilus::Tracing::OpCode& op,
                         const Nautilus::Tracing::ValueRef& resultRef,
                         const Nautilus::Tracing::ValueRef& inputRef) {
    if (auto ctx = Tracing::TraceContext::getIfActive()) {
        ctx->traceUnaryOperation(op, inputRef, resultRef);
    }
}

void traceConstOperation(const AnyPtr& constValue, const Nautilus::Tracing::ValueRef& valueReference) {
    if (auto ctx = Tracing::TraceContext::getIfActive()) {
        ctx->traceConstOperation(valueReference, constValue);
    }
};

}// namespace NES::Nautilus::Tracing::TraceUtil