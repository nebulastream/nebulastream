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

#include <Experimental/Interpreter/Operators/ExecutableOperator.hpp>
#include <Experimental/Interpreter/Operators/Operator.hpp>
namespace NES::Nautilus {

void Operator::setup(RuntimeExecutionContext& executionCtx) const {
    if (hasChildren()) {
        child->setup(executionCtx);
    }
}

void Operator::open(RuntimeExecutionContext& executionCtx, RecordBuffer& rb) const {
    if (hasChildren()) {
        child->open(executionCtx, rb);
    }
}

void Operator::close(RuntimeExecutionContext& executionCtx, RecordBuffer& rb) const {
    if (hasChildren()) {
        child->close(executionCtx, rb);
    }
}

bool Operator::hasChildren() const { return child != nullptr; }

void Operator::setChild(ExecuteOperatorPtr child) { this->child = std::move(child); }

Operator::~Operator() {}

}// namespace NES::Nautilus