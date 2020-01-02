#pragma once

#include "Operators/Operator.hpp"
#include <memory>

#include <API/Schema.hpp>
#include <API/Window/WindowDefinition.hpp>
#include <QueryCompiler/C_CodeGen/BinaryOperatorStatement.hpp>
#include <QueryCompiler/C_CodeGen/CodeCompiler.hpp>
#include <QueryCompiler/C_CodeGen/Declaration.hpp>
#include <QueryCompiler/C_CodeGen/FileBuilder.hpp>
#include <QueryCompiler/C_CodeGen/FunctionBuilder.hpp>
#include <QueryCompiler/C_CodeGen/Statement.hpp>
#include <QueryCompiler/C_CodeGen/UnaryOperatorStatement.hpp>
#include <QueryCompiler/CodeGen.hpp>
#include <Util/ErrorHandling.hpp>
#include <SourceSink/DataSink.hpp>
#include <API/Types/DataTypes.hpp>

namespace iotdb {

class AttributeReference;
typedef std::shared_ptr<AttributeReference> AttributeReferencePtr;

class CodeGenerator;
typedef std::shared_ptr<CodeGenerator> CodeGeneratorPtr;

class PipelineStage;
typedef std::shared_ptr<PipelineStage> PipelineStagePtr;

// class Operator;
typedef std::shared_ptr<Operator> OperatorPtr;

class CompilerArgs;
class CodeGenArgs;

class CompilerArgs {
};

class CodeGenArgs {
};

class CodeGenerator {
 public:
  CodeGenerator(const CodeGenArgs &args);
  // virtual bool addOperator(OperatorPtr) = 0;
  virtual bool generateCode(const DataSourcePtr &source, const PipelineContextPtr &context, std::ostream &out) = 0;
  virtual bool generateCode(const PredicatePtr &pred, const PipelineContextPtr &context, std::ostream &out) = 0;
  virtual bool generateCode(const AttributeFieldPtr field,
                            const PredicatePtr &pred,
                            const iotdb::PipelineContextPtr &context,
                            std::ostream &out) = 0;
  virtual bool generateCode(const DataSinkPtr &sink, const PipelineContextPtr &context, std::ostream &out) = 0;
  virtual bool generateCode(const WindowDefinitionPtr &window, const PipelineContextPtr &context, std::ostream &out) = 0;
  virtual PipelineStagePtr compile(const CompilerArgs &) = 0;
  virtual ~CodeGenerator();

  const Schema &getInputSchema() const { return input_schema_; };
  const Schema &getResultSchema() const { return result_schema_; };

 protected:
  CodeGenArgs args_;
  Schema input_schema_;
  Schema result_schema_;
};

/** \brief factory method for creating a code generator */
CodeGeneratorPtr createCodeGenerator();
/** \brief factory method for creating a pipeline context */
const PipelineContextPtr createPipelineContext();

class GeneratedCode {
 public:
  GeneratedCode();

  std::vector<VariableDeclaration> variable_decls;
  std::vector<StatementPtr> variable_init_stmts;
  std::shared_ptr<FOR> for_loop_stmt;
  /* points to the current scope (compound statement)
   * to insert the code of the next operation,
   * important when multiple levels of nesting occur
   * due to loops (for(){ <cursor> }) or
   * if statements (if(..){ <cursor>}) */
  CompoundStatementPtr current_code_insertion_point;
  std::vector<StatementPtr> cleanup_stmts;
  StatementPtr return_stmt;
  std::shared_ptr<VariableDeclaration> var_decl_id;
  std::shared_ptr<VariableDeclaration> var_decl_return;
  StructDeclaration struct_decl_tuple_buffer;
  StructDeclaration struct_decl_state;
  StructDeclaration struct_decl_input_tuple;
  StructDeclaration struct_decl_result_tuple;
  VariableDeclaration var_decl_tuple_buffers;
  VariableDeclaration var_declare_window;
  VariableDeclaration var_decl_tuple_buffer_output;
  VariableDeclaration var_decl_state;
  VariableDeclaration decl_field_num_tuples_struct_tuple_buf;
  VariableDeclaration decl_field_data_ptr_struct_tuple_buf;
  VariableDeclaration var_decl_input_tuple;
  VariableDeclaration var_num_for_loop;
  std::vector<StructDeclaration> type_decls;
  std::vector<DeclarationPtr> override_fields;

};

typedef std::shared_ptr<GeneratedCode> GeneratedCodePtr;

const StructDeclaration getStructDeclarationTupleBuffer();
//const StructDeclaration getStructDeclarationWindowState();
const StructDeclaration getStructDeclarationTupleBuffer();
//const StructDeclaration getStructDeclarationWindowState();
const StructDeclaration getStructDeclarationFromSchema(const std::string struct_name, const Schema &schema);
} // namespace iotdb

