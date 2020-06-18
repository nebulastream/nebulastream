
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
      varDeclarationWindowManager(VariableDeclaration::create(DataTypeFactory::createInt32(), "window")),
      varDeclarationResultBuffer(VariableDeclaration::create(DataTypeFactory::createInt32(), "output_buffer")),
      varDeclarationExecutionContext(VariableDeclaration::create(DataTypeFactory::createInt32(), "output_buffer")),
      varDeclarationState(VariableDeclaration::create(DataTypeFactory::createInt32(), "state")),
      tupleBufferGetNumberOfTupleCall(FunctionCallStatement("getNumberOfTuples")),
      varDeclarationInputTuples(VariableDeclaration::create(DataTypeFactory::createInt32(), "inputTuples")),
      varDeclarationNumberOfResultTuples(VariableDeclaration::create(
          DataTypeFactory::createInt64(), "numberOfResultTuples", DataTypeFactory::createBasicValue(DataTypeFactory::createInt64(), "0"))),
      typeDeclarations() {
}

}// namespace NES
