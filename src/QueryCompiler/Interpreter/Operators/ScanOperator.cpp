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

#include <QueryCompiler/Interpreter/Operators/OperatorContext.hpp>
#include <QueryCompiler/Interpreter/Operators/ExecutableOperator.hpp>

#include <QueryCompiler/Interpreter/Operators/ScanOperator.hpp>
#include <QueryCompiler/Interpreter/Record.hpp>
#include <QueryCompiler/Interpreter/RecordBuffer.hpp>
namespace NES::QueryCompilation {

ScanOperator::ScanOperator(ExecutableOperatorPtr next) : Operator(next) {}

void ScanOperator::execute(RecordBuffer& buffer, ExecutionContextPtr ctx) {
    for (uint64_t i = 0; i < buffer.getNumberOfTuples(); i++) {
        auto record = buffer[i];
        next->execute(record, ctx);
    }
}
}// namespace NES::QueryCompilation
