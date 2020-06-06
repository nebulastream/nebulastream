
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
      varDeclarationInputBuffer(VariableDeclaration::create(createDataType(INT32), "input_buffers")),
      varDeclarationWindowManager(VariableDeclaration::create(createDataType(INT32), "window")),
      varDeclarationResultBuffer(VariableDeclaration::create(createDataType(INT32), "output_buffer")),
      varDeclarationExecutionContext(VariableDeclaration::create(createDataType(INT32), "output_buffer")),
      varDeclarationState(VariableDeclaration::create(createDataType(INT32), "state")),
      tupleBufferGetNumberOfTupleCall(FunctionCallStatement("getNumberOfTuples")),
      varDeclarationInputTuples(VariableDeclaration::create(createDataType(INT32), "inputTuples")),
      varDeclarationNumberOfResultTuples(VariableDeclaration::create(
          createDataType(UINT64), "numberOfResultTuples", createBasicTypeValue(INT64, "0"))),
      typeDeclarations() {
}

}// namespace NES
