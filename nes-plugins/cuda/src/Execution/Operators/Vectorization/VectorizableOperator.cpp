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

#include <Exceptions/RuntimeException.hpp>
#include <Execution/Operators/Vectorization/VectorizableOperator.hpp>

namespace NES::Runtime::Execution::Operators {

void VectorizableOperator::execute(ExecutionContext&, Record&) const {
    throw Exceptions::RuntimeException(
        "VectorizableOperator: Non-vectorized record input is not allowed in vectorizable operator");
}

}// namespace NES::Runtime::Execution::Operators
