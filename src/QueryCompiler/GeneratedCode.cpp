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

#include <Common/DataTypes/DataTypeFactory.hpp>
#include <QueryCompiler/GeneratedCode.hpp>

namespace NES {

GeneratedCode::GeneratedCode()
    : variableDeclarations(),
      variableInitStmts(),
      forLoopStmt(),
      currentCodeInsertionPoint(),
      cleanupStmts(),
      returnStmt(),
      varDeclarationRecordIndex(),
      varDeclarationReturnValue(),
      structDeclaratonInputTuple(StructDeclaration::create("InputTuple", "")),
      structDeclarationResultTuple(StructDeclaration::create("ResultTuple", "")),
      varDeclarationInputBuffer(VariableDeclaration::create(DataTypeFactory::createInt32(), "input_buffers")),
      varDeclarationResultBuffer(VariableDeclaration::create(DataTypeFactory::createInt32(), "output_buffer")),
      varDeclarationExecutionContext(VariableDeclaration::create(DataTypeFactory::createInt32(), "output_buffer")),
      varDeclarationWorkerContext(VariableDeclaration::create(DataTypeFactory::createInt32(), "worker_context")),
      tupleBufferGetNumberOfTupleCall(FunctionCallStatement("getNumberOfTuples")),
      varDeclarationInputTuples(VariableDeclaration::create(DataTypeFactory::createInt32(), "inputTuples")),
      varDeclarationNumberOfResultTuples(VariableDeclaration::create(
          DataTypeFactory::createInt64(), "numberOfResultTuples", DataTypeFactory::createBasicValue(DataTypeFactory::createInt64(), "0"))),
      typeDeclarations() {
}

}// namespace NES
