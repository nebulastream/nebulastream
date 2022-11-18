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

#include <Nautilus/Tracing/Trace/BlockRef.hpp>
#include <Nautilus/Tracing/Trace/TraceOperation.hpp>
#include <Util/magicenum/magic_enum.hpp>

namespace NES::Nautilus::Tracing {

TraceOperation::TraceOperation(OpCode op) : op(op), result(), input(){};

TraceOperation::TraceOperation(OpCode op, const std::vector<InputVariant>& input) : op(op), result(None()), input(input){};

TraceOperation::TraceOperation(OpCode op, ValueRef result, const std::vector<InputVariant>& input)
    : op(op), result(result), input(input){};

TraceOperation::TraceOperation(const TraceOperation&& other) {
    this->op = other.op;
    this->result = other.result;
    this->input = std::move(other.input);
    this->operationRef = std::move(other.operationRef);
}

TraceOperation& TraceOperation::operator=(const TraceOperation& other) {
    this->op = other.op;
    this->result = other.result;
    this->input = other.input;
    this->operationRef = other.operationRef;
    return *this;
}

TraceOperation& TraceOperation::operator=(const TraceOperation&& other) {
    this->op = other.op;
    this->result = other.result;
    this->input = std::move(other.input);
    this->operationRef = std::move(other.operationRef);
    return *this;
}

std::ostream& operator<<(std::ostream& os, const TraceOperation& operation) {
    os << magic_enum::enum_name(operation.op) << "\t";
    if (auto ref = std::get_if<ValueRef>(&operation.result)) {
        os << *ref << "\t";
    } else if (auto ref = std::get_if<BlockRef>(&operation.result)) {
        os << *ref << "\t";
    }
    for (const InputVariant& input : operation.input) {
        if (auto ref = std::get_if<ValueRef>(&input)) {
            os << *ref << "\t";
        } else if (auto ref = std::get_if<BlockRef>(&input)) {
            os << *ref << "\t";
        } else if (auto ref = std::get_if<ConstantValue>(&input)) {
            os << *ref << "\t";
        } else if (auto ref = std::get_if<FunctionCallTarget>(&input)) {
            os << ref->mangledName << "\t";
        }
    }
    return os;
}

}// namespace NES::Nautilus::Tracing