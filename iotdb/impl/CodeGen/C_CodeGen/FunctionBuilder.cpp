
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

#include <CodeGen/C_CodeGen/CodeCompiler.hpp>
#include <CodeGen/CodeExpression.hpp>
#include <CodeGen/PipelineStage.hpp>
#include <Core/DataTypes.hpp>
#include <Util/ErrorHandling.hpp>

namespace iotdb {

enum StatementType {
  RETURN_STMT,
  IF_STMT,
  IF_ELSE_STMT,
  FOR_LOOP_STMT,
  FUNC_CALL_STMT,
  VAR_REF_STMT,
  CONSTANT_VALUE_EXPR_STMT,
  BINARY_OP_STMT,
  UNARY_OP_STMT
};

class Statement {
public:
  virtual StatementType getStamentType() const = 0;
  virtual const CodeExpressionPtr getCode() const = 0;
  virtual ~Statement();
};

Statement::~Statement() {}

class ExpressionStatment;
typedef std::shared_ptr<ExpressionStatment> ExpressionStatmentPtr;

class BinaryOperatorStatement;

class ExpressionStatment : public Statement {
public:
  virtual StatementType getStamentType() const = 0;
  virtual const CodeExpressionPtr getCode() const = 0;
  /** \brief virtual copy constructor */
  virtual const ExpressionStatmentPtr copy() const = 0;

//  UnaryOperatorStatement operator sizeof(const ExpressionStatment &ref){
//  return UnaryOperatorStatement(ref, SIZE_OF_TYPE_OP);
//  }



  //UnaryOperatorStatement operator ++(const ExpressionStatment &ref){
  //return UnaryOperatorStatement(ref, POSTFIX_INCREMENT_OP);
  //}
  //UnaryOperatorStatement operator --(const ExpressionStatment &ref){
  //return UnaryOperatorStatement(ref, POSTFIX_DECREMENT_OP);
  //}

  virtual ~ExpressionStatment();
};

ExpressionStatment::~ExpressionStatment() {}

typedef std::shared_ptr<Statement> StatementPtr;
typedef std::string Code;

class Declaration;
typedef std::shared_ptr<Declaration> DeclarationPtr;

class Declaration {
public:
  virtual const DataTypePtr getType() const = 0;
  virtual const std::string getIdentifierName() const = 0;
  virtual const Code getTypeDefinitionCode() const = 0;
  virtual const Code getCode() const = 0;
  virtual const DeclarationPtr copy() const = 0;
  virtual ~Declaration();
};

Declaration::~Declaration() {}

class StructDeclaration;
const DataTypePtr createUserDefinedType(const StructDeclaration &decl);

class StructDeclaration : public Declaration {
public:
  static StructDeclaration create(const std::string &type_name, const std::string &variable_name) {
    return StructDeclaration(type_name, variable_name);
  }

  virtual const DataTypePtr getType() const override { return createUserDefinedType(*this); }

  virtual const std::string getIdentifierName() const override { return variable_name_; }

  const Code getTypeDefinitionCode() const override {
    std::stringstream expr;
    expr << "struct " << type_name_ << "{" << std::endl;
    for (auto &decl : decls_) {
      expr << decl->getCode() << ";" << std::endl;
    }
    expr << "}";
    return expr.str();
  }

  const Code getCode() const override {
    std::stringstream expr;
    expr << "struct " << type_name_ << "{" << std::endl;
    for (auto &decl : decls_) {
      expr << decl->getCode() << ";" << std::endl;
    }
    expr << "}";
    expr << variable_name_;
    return expr.str();
  }

  const uint32_t getTypeSizeInBytes() const {
    IOTDB_FATAL_ERROR("Called unimplemented function!");
    return 0;
  }

  const std::string getTypeName() const { return type_name_; }

  const DeclarationPtr copy() const override { return std::make_shared<StructDeclaration>(*this); }

  DeclarationPtr getField(const std::string field_name) const {
    for (auto &decl : decls_) {
      if (decl->getIdentifierName() == field_name) {
        return decl;
      }
    }
    return DeclarationPtr();
  }

  //      VariableDeclarationPtr getReferenceToField(const std::string field_name) const{
  //        for(auto& decl : decls_){
  //            if(decl->getIdentifierName()==field_name){
  //                return std::make_shared<VariableDelaration>;
  //            }
  //        }
  //      }

  StructDeclaration &addField(const Declaration &decl) {
    DeclarationPtr decl_p = decl.copy();
    if (decl_p)
      decls_.push_back(decl_p);
    return *this;
  }
  ~StructDeclaration();

private:
  StructDeclaration(const std::string &type_name, const std::string &variable_name)
      : type_name_(type_name), variable_name_(variable_name), decls_() {}
  std::string type_name_;
  std::string variable_name_;
  std::vector<DeclarationPtr> decls_;
};

StructDeclaration::~StructDeclaration() {}

class VariableDeclaration;
typedef std::shared_ptr<VariableDeclaration> VariableDeclarationPtr;

class VariableDeclaration : public Declaration {
public:
  static VariableDeclaration create(DataTypePtr type, const std::string &identifier, ValueTypePtr value = nullptr);

  virtual const DataTypePtr getType() const override { return type_; }
  virtual const std::string getIdentifierName() const override { return identifier_; }

  const Code getTypeDefinitionCode() const override {
    CodeExpressionPtr code = type_->getTypeDefinitionCode();
    if (code)
      return code->code_;
    else
      return Code();
  }

  const Code getCode() const override {
    std::stringstream str;
    str << type_->getCode()->code_ << " " << identifier_;
    if (init_value_) {
      str << " = " << init_value_->getCodeExpression()->code_;
    }
    return str.str();
  }

  const CodeExpressionPtr getIdentifier() const { return CodeExpressionPtr(new CodeExpression(identifier_)); }

  const DataTypePtr getDataType() const { return type_; }

  const DeclarationPtr copy() const override { return std::make_shared<VariableDeclaration>(*this); }
  ~VariableDeclaration();

private:
  VariableDeclaration(DataTypePtr type, const std::string &identifier, ValueTypePtr value = nullptr);
  DataTypePtr type_;
  std::string identifier_;
  ValueTypePtr init_value_;
};

VariableDeclaration::~VariableDeclaration() {}

VariableDeclaration::VariableDeclaration(DataTypePtr type, const std::string &identifier, ValueTypePtr value)
    : type_(type), identifier_(identifier), init_value_(value) {}

VariableDeclaration VariableDeclaration::create(DataTypePtr type, const std::string &identifier, ValueTypePtr value) {
  if (!type)
    IOTDB_FATAL_ERROR("DataTypePtr type is nullptr!");
  return VariableDeclaration(type, identifier, value);
}

class FunctionDeclaration : public Declaration {
private:
  Code function_code;

public:
  FunctionDeclaration(Code code) : function_code(code) {}

  virtual const DataTypePtr getType() const override { return DataTypePtr(); }
  virtual const std::string getIdentifierName() const override { return ""; }

  const Code getTypeDefinitionCode() const override { return Code(); }

  const Code getCode() const override { return function_code; }
  const DeclarationPtr copy() const override { return std::make_shared<FunctionDeclaration>(*this); }
};

class StructBuilder {
public:
  static StructBuilder create(const std::string &struct_name);
  StructBuilder &addField(AttributeFieldPtr attr);
  StructDeclaration build();
};

class StatementBuilder {
public:
  static StatementBuilder create(const std::string &struct_name);
};

class FunctionBuilder {
private:
  std::string name;
  DataTypePtr returnType;
  std::vector<VariableDeclaration> parameters;
  std::vector<VariableDeclaration> variable_declarations;
  std::vector<StatementPtr> statements;
  FunctionBuilder(const std::string &function_name);

public:
  static FunctionBuilder create(const std::string &function_name);

  FunctionBuilder &returns(DataTypePtr returnType_);
  FunctionBuilder &addParameter(VariableDeclaration var_decl);
  FunctionBuilder &addStatement(StatementPtr statement);
  FunctionBuilder &addVariableDeclaration(VariableDeclaration var_decl);
  FunctionDeclaration build();
};

FunctionBuilder::FunctionBuilder(const std::string &function_name) : name(function_name) {}

FunctionBuilder FunctionBuilder::create(const std::string &function_name) { return FunctionBuilder(function_name); }

FunctionDeclaration FunctionBuilder::build() {
  std::stringstream function;
  if (!returnType) {
    function << "void";
  } else {
    function << returnType->getCode()->code_;
  }
  function << " " << name << "(";
  for (uint64_t i = 0; i < parameters.size(); ++i) {
    function << parameters[i].getCode();
    if (i + 1 < parameters.size())
      function << ", ";
  }
  function << "){";

  for (uint64_t i = 0; i < variable_declarations.size(); ++i) {
    function << variable_declarations[i].getCode() << ";";
  }

  for (uint64_t i = 0; i < statements.size(); ++i) {
    function << statements[i]->getCode()->code_ << ";";
  }
  function << "}";

  return FunctionDeclaration(function.str());
}

FunctionBuilder &FunctionBuilder::returns(DataTypePtr type) {
  returnType = type;
  return *this;
}

FunctionBuilder &FunctionBuilder::addParameter(VariableDeclaration var_decl) {
  parameters.push_back(var_decl);
  return *this;
}
FunctionBuilder &FunctionBuilder::addStatement(StatementPtr statement) {
  if (statement)
    statements.push_back(statement);
  return *this;
}

FunctionBuilder &FunctionBuilder::addVariableDeclaration(VariableDeclaration vardecl) {
  variable_declarations.push_back(vardecl);
  return *this;
}

class CodeFile {
public:
  std::string code;
};

PipelineStagePtr compile(const CodeFile &file) {
  CCodeCompiler compiler;
  CompiledCCodePtr compiled_code = compiler.compile(file.code);
  return createPipelineStage(compiled_code);
}

class FileBuilder {
private:
  std::stringstream declations;

public:
  static FileBuilder create(const std::string &file_name);
  FileBuilder &addDeclaration(const Declaration &);
  CodeFile build();
};

FileBuilder FileBuilder::create(const std::string &file_name) {
  FileBuilder builder;
  builder.declations << "#include <cstdint>" << std::endl;
  return builder;
}
FileBuilder &FileBuilder::addDeclaration(const Declaration &decl) {
  declations << decl.getCode() << ";";
  return *this;
}
CodeFile FileBuilder::build() {
  CodeFile file;
  file.code = declations.str();
  return file;
}

class ConstantExprStatement : public ExpressionStatment {
public:
  ValueTypePtr val_;

  virtual StatementType getStamentType() const { return CONSTANT_VALUE_EXPR_STMT; }

  virtual const CodeExpressionPtr getCode() const { return val_->getCodeExpression(); }

  virtual const ExpressionStatmentPtr copy() const { return std::make_shared<ConstantExprStatement>(*this); }

  ConstantExprStatement(const ValueTypePtr &val) : val_(val) {}

  ConstantExprStatement(const BasicType &type, const std::string &value) : val_(createBasicTypeValue(type, value)) {}

  virtual ~ConstantExprStatement();
};

ConstantExprStatement::~ConstantExprStatement() {}

class TypeCastExprStatement : public ExpressionStatment {
public:
  virtual StatementType getStamentType() const { return CONSTANT_VALUE_EXPR_STMT; }

  virtual const CodeExpressionPtr getCode() const {
    CodeExpressionPtr code;
    code = combine(std::make_shared<CodeExpression>("("), type_->getCode());
    code = combine(code, std::make_shared<CodeExpression>(")"));
    code = combine(code, expr_->getCode());
    return code;
  }

  virtual const ExpressionStatmentPtr copy() const { return std::make_shared<TypeCastExprStatement>(*this); }

  TypeCastExprStatement(const ExpressionStatment &expr, const DataTypePtr &type) : expr_(expr.copy()), type_(type) {}

  virtual ~TypeCastExprStatement();

private:
  ExpressionStatmentPtr expr_;
  DataTypePtr type_;
};

TypeCastExprStatement::~TypeCastExprStatement() {}

class VarRefStatement : public ExpressionStatment {
public:
  const VariableDeclaration &var_decl_;

  virtual StatementType getStamentType() const { return VAR_REF_STMT; }

  virtual const CodeExpressionPtr getCode() const { return var_decl_.getIdentifier(); }

  virtual const ExpressionStatmentPtr copy() const { return std::make_shared<VarRefStatement>(*this); }

  VarRefStatement(const VariableDeclaration &var_decl) : var_decl_(var_decl) {}

  virtual ~VarRefStatement();
};

VarRefStatement::~VarRefStatement() {}

typedef VarRefStatement VarRef;

enum UnaryOperatorType {
  ADDRESS_OF_OP,
  DEREFERENCE_POINTER_OP,
  PREFIX_INCREMENT_OP,
  PREFIX_DECREMENT_OP,
  POSTFIX_INCREMENT_OP,
  POSTFIX_DECREMENT_OP,
  BITWISE_COMPLEMENT_OP,
  LOGICAL_NOT_OP,
  SIZE_OF_TYPE_OP
};

const std::string toString(const UnaryOperatorType &type) {
  const char *const names[] = {"ADDRESS_OF_OP",         "DEREFERENCE_POINTER_OP", "PREFIX_INCREMENT_OP",
                               "PREFIX_DECREMENT_OP",   "POSTFIX_INCREMENT_OP",   "POSTFIX_DECREMENT_OP",
                               "BITWISE_COMPLEMENT_OP", "LOGICAL_NOT_OP",         "SIZE_OF_TYPE_OP"};
  return std::string(names[type]);
}

const CodeExpressionPtr toCodeExpression(const UnaryOperatorType &type) {
  const char *const names[] = {"&", "*", "++", "--", "++", "--", "~", "!", "sizeof"};
  return std::make_shared<CodeExpression>(names[type]);
}

enum BinaryOperatorType {
  EQUAL_OP,
  UNEQUAL_OP,
  LESS_THEN_OP,
  LESS_THEN_EQUAL_OP,
  GREATER_THEN_OP,
  GREATER_THEN_EQUAL_OP,
  PLUS_OP,
  MINUS_OP,
  MULTIPLY_OP,
  DIVISION_OP,
  MODULO_OP,
  LOGICAL_AND_OP,
  LOGICAL_OR_OP,
  BITWISE_AND_OP,
  BITWISE_OR_OP,
  BITWISE_XOR_OP,
  BITWISE_LEFT_SHIFT_OP,
  BITWISE_RIGHT_SHIFT_OP,
  ASSIGNMENT_OP,
  ARRAY_REFERENCE_OP,
  MEMBER_SELECT_POINTER_OP,
  MEMBER_SELECT_REFERENCE_OP
};

const std::string toString(const BinaryOperatorType &type) {
  const char *const names[] = {"EQUAL_OP",
                               "UNEQUAL_OP",
                               "LESS_THEN_OP",
                               "LESS_THEN_EQUAL_OP",
                               "GREATER_THEN_OP",
                               "GREATER_THEN_EQUAL_OP",
                               "PLUS_OP",
                               "MINUS_OP",
                               "MULTIPLY_OP",
                               "DIVISION_OP",
                               "MODULO_OP",
                               "LOGICAL_AND_OP",
                               "LOGICAL_OR_OP",
                               "BITWISE_AND_OP",
                               "BITWISE_OR_OP",
                               "BITWISE_XOR_OP",
                               "BITWISE_LEFT_SHIFT_OP",
                               "BITWISE_RIGHT_SHIFT_OP",
                               "ASSIGNMENT_OP",
                               "ARRAY_REFERENCE_OP",
                               "MEMBER_SELECT_POINTER_OP",
                               "MEMBER_SELECT_REFERENCE_OP"};
  return std::string(names[type]);
}

const CodeExpressionPtr toCodeExpression(const BinaryOperatorType &type) {
  const char *const names[] = {
      "==", "!=", "<", "<=", ">", ">=", "+",  "-", "*",  "/",  "%",
      "&&", "||", "&", "|",  "^", "<<", ">>", "=", "[]", "->", ".",
  };
  return std::make_shared<CodeExpression>(names[type]);
}

enum BracketMode { NO_BRACKETS, BRACKETS };

class UnaryOperatorStatement : public ExpressionStatment {
public:
  UnaryOperatorStatement(const ExpressionStatment &expr, const UnaryOperatorType &op,
                         BracketMode bracket_mode = NO_BRACKETS)
      : expr_(expr.copy()), op_(op), bracket_mode_(bracket_mode) {}

  virtual StatementType getStamentType() const { return UNARY_OP_STMT; }
  virtual const CodeExpressionPtr getCode() const {
    CodeExpressionPtr code;
    if (POSTFIX_INCREMENT_OP == op_ || POSTFIX_DECREMENT_OP == op_) {
      /* postfix operators */
      code = combine(expr_->getCode(), toCodeExpression(op_));
    } else if (SIZE_OF_TYPE_OP == op_) {
      code = combine(toCodeExpression(op_), std::make_shared<CodeExpression>("("));
      code = combine(code, expr_->getCode());
      code = combine(code, std::make_shared<CodeExpression>(")"));
    } else {
      /* prefix operators */
      code = combine(toCodeExpression(op_), expr_->getCode());
    }
    std::string ret;
    if (bracket_mode_ == BRACKETS) {
      ret = std::string("(") + code->code_ + std::string(")");
    } else {
      ret = code->code_;
    }
    return std::make_shared<CodeExpression>(ret);
  }

  virtual const ExpressionStatmentPtr copy() const { return std::make_shared<UnaryOperatorStatement>(*this); }

  virtual ~UnaryOperatorStatement();

private:
  ExpressionStatmentPtr expr_;
  UnaryOperatorType op_;
  BracketMode bracket_mode_;
};

UnaryOperatorStatement::~UnaryOperatorStatement() {}

class BinaryOperatorStatement : public ExpressionStatment {
public:
  BinaryOperatorStatement(const ExpressionStatment &lhs, const BinaryOperatorType &op, const ExpressionStatment &rhs,
                          BracketMode bracket_mode = NO_BRACKETS)
      : lhs_(lhs.copy()), rhs_(rhs.copy()), op_(op), bracket_mode_(bracket_mode) {}

  BinaryOperatorStatement addRight(const BinaryOperatorType &op, const VarRefStatement &rhs,
                                   BracketMode bracket_mode = NO_BRACKETS) {
    return BinaryOperatorStatement(*this, op, rhs, bracket_mode);
    //        if(lhs_){
    //            return BinaryOperatorStatement(*lhs_, op, rhs);
    //          }else if(bin_op_){
    //            return BinaryOperatorStatement(*bin_op_, op, rhs);
    //          }else{
    //            IOTDB_FATAL_ERROR("In BinaryOperatorStatement: lhs_ and bin_op_ null!");
    //          }
  }

  StatementPtr assignToVariable(const VarRefStatement &lhs) { return StatementPtr(); }

  virtual StatementType getStamentType() const { return BINARY_OP_STMT; }
  virtual const CodeExpressionPtr getCode() const {
    CodeExpressionPtr code;
    if (ARRAY_REFERENCE_OP == op_) {
      code = combine(lhs_->getCode(), std::make_shared<CodeExpression>("["));
      code = combine(code, rhs_->getCode());
      code = combine(code, std::make_shared<CodeExpression>("]"));
    } else {
      code = combine(lhs_->getCode(), toCodeExpression(op_));
      code = combine(code, rhs_->getCode());
    }

    std::string ret;
    if (bracket_mode_ == BRACKETS) {
      ret = std::string("(") + code->code_ + std::string(")");
    } else {
      ret = code->code_;
    }
    return std::make_shared<CodeExpression>(ret);
  }

  virtual const ExpressionStatmentPtr copy() const { return std::make_shared<BinaryOperatorStatement>(*this); }

  BinaryOperatorStatement operator [](const ExpressionStatment &ref){
    return BinaryOperatorStatement(*this, ARRAY_REFERENCE_OP, ref);
  }


  virtual ~BinaryOperatorStatement();

private:
  ExpressionStatmentPtr lhs_;
  ExpressionStatmentPtr rhs_;
  BinaryOperatorType op_;
  BracketMode bracket_mode_;
};

BinaryOperatorStatement::~BinaryOperatorStatement() {}

/** \brief small utility operator overloads to make code generation simpler and */

UnaryOperatorStatement operator &(const ExpressionStatment &ref){
return UnaryOperatorStatement(ref, ADDRESS_OF_OP);
}
UnaryOperatorStatement operator *(const ExpressionStatment &ref){
return UnaryOperatorStatement(ref, DEREFERENCE_POINTER_OP);
}
UnaryOperatorStatement operator ++(const ExpressionStatment &ref){
return UnaryOperatorStatement(ref, PREFIX_INCREMENT_OP);
}
UnaryOperatorStatement operator --(const ExpressionStatment &ref){
return UnaryOperatorStatement(ref, PREFIX_DECREMENT_OP);
}
UnaryOperatorStatement operator ~(const ExpressionStatment &ref){
return UnaryOperatorStatement(ref, BITWISE_COMPLEMENT_OP);
}
UnaryOperatorStatement operator !(const ExpressionStatment &ref){
return UnaryOperatorStatement(ref, LOGICAL_NOT_OP);
}

BinaryOperatorStatement assign(const ExpressionStatment &lhs, const ExpressionStatment &rhs){
 return BinaryOperatorStatement(lhs, ASSIGNMENT_OP, rhs);
}

UnaryOperatorStatement sizeOf(const ExpressionStatment &ref){
 return UnaryOperatorStatement(ref, SIZE_OF_TYPE_OP);
}

//BinaryOperatorStatement ExpressionStatment::operator =(const ExpressionStatment &ref){
//return BinaryOperatorStatement(*this, ASSIGNMENT_OP, ref);
//}

BinaryOperatorStatement operator ==(const ExpressionStatment &lhs, const ExpressionStatment &rhs){
return BinaryOperatorStatement(lhs, EQUAL_OP, rhs);
}
BinaryOperatorStatement operator !=(const ExpressionStatment &lhs, const ExpressionStatment &rhs){
return BinaryOperatorStatement(lhs, UNEQUAL_OP, rhs);
}
BinaryOperatorStatement operator <(const ExpressionStatment &lhs, const ExpressionStatment &rhs){
return BinaryOperatorStatement(lhs, LESS_THEN_OP, rhs);
}
BinaryOperatorStatement operator <=(const ExpressionStatment &lhs, const ExpressionStatment &rhs){
return BinaryOperatorStatement(lhs, LESS_THEN_EQUAL_OP, rhs);
}
BinaryOperatorStatement operator >(const ExpressionStatment &lhs, const ExpressionStatment &rhs){
return BinaryOperatorStatement(lhs, GREATER_THEN_OP, rhs);
}
BinaryOperatorStatement operator >=(const ExpressionStatment &lhs, const ExpressionStatment &rhs){
return BinaryOperatorStatement(lhs, GREATER_THEN_EQUAL_OP, rhs);
}
BinaryOperatorStatement operator +(const ExpressionStatment &lhs, const ExpressionStatment &rhs){
return BinaryOperatorStatement(lhs, PLUS_OP, rhs);
}
BinaryOperatorStatement operator -(const ExpressionStatment &lhs, const ExpressionStatment &rhs){
return BinaryOperatorStatement(lhs, MINUS_OP, rhs);
}
BinaryOperatorStatement operator *(const ExpressionStatment &lhs, const ExpressionStatment &rhs){
return BinaryOperatorStatement(lhs, MULTIPLY_OP, rhs);
}
BinaryOperatorStatement operator /(const ExpressionStatment &lhs, const ExpressionStatment &rhs){
return BinaryOperatorStatement(lhs, DIVISION_OP, rhs);
}
BinaryOperatorStatement operator %(const ExpressionStatment &lhs, const ExpressionStatment &rhs){
return BinaryOperatorStatement(lhs, MODULO_OP, rhs);
}
BinaryOperatorStatement operator &&(const ExpressionStatment &lhs, const ExpressionStatment &rhs){
return BinaryOperatorStatement(lhs, LOGICAL_AND_OP, rhs);
}
BinaryOperatorStatement operator ||(const ExpressionStatment &lhs, const ExpressionStatment &rhs){
return BinaryOperatorStatement(lhs, LOGICAL_OR_OP, rhs);
}
BinaryOperatorStatement operator &(const ExpressionStatment &lhs, const ExpressionStatment &rhs){
return BinaryOperatorStatement(lhs, BITWISE_AND_OP, rhs);
}
BinaryOperatorStatement operator |(const ExpressionStatment &lhs, const ExpressionStatment &rhs){
return BinaryOperatorStatement(lhs, BITWISE_OR_OP, rhs);
}
BinaryOperatorStatement operator ^(const ExpressionStatment &lhs, const ExpressionStatment &rhs){
return BinaryOperatorStatement(lhs, BITWISE_XOR_OP, rhs);
}
BinaryOperatorStatement operator <<(const ExpressionStatment &lhs, const ExpressionStatment &rhs){
return BinaryOperatorStatement(lhs, BITWISE_LEFT_SHIFT_OP, rhs);
}
BinaryOperatorStatement operator >>(const ExpressionStatment &lhs, const ExpressionStatment &rhs){
return BinaryOperatorStatement(lhs, BITWISE_RIGHT_SHIFT_OP, rhs);
}


//BinaryOperatorStatement operator [](const ExpressionStatment &lhs, const ExpressionStatment &rhs){
//return BinaryOperatorStatement(lhs, ARRAY_REFERENCE_OP, rhs);
//}
//BinaryOperatorStatement operator ->(const ExpressionStatment &lhs, const ExpressionStatment &rhs){
//return BinaryOperatorStatement(lhs, MEMBER_SELECT_POINTER_OP, rhs);
//}







class ReturnStatement : public Statement {
public:
  ReturnStatement(VarRefStatement var_ref) : var_ref_(var_ref) {}

  VarRefStatement var_ref_;

  virtual StatementType getStamentType() const {}
  virtual const CodeExpressionPtr getCode() const {
    std::stringstream stmt;
    stmt << "return " << var_ref_.getCode()->code_ << ";";
    return std::make_shared<CodeExpression>(stmt.str());
  }
  virtual ~ReturnStatement() {}
};

class IfStatement : public Statement {
public:
  IfStatement(const Statement &cond_expr, const Statement &cond_true_stmt)
      : cond_expr_(cond_expr), cond_true_stmt_(cond_true_stmt) {}

  virtual StatementType getStamentType() const { return IF_STMT; }
  virtual const CodeExpressionPtr getCode() const {
    std::stringstream code;
    code << "if(" << cond_expr_.getCode()->code_ << "){" << std::endl;
    code << cond_true_stmt_.getCode()->code_ << std::endl;
    code << "}" << std::endl;
    return std::make_shared<CodeExpression>(code.str());
  }
  virtual ~IfStatement();

private:
  const Statement &cond_expr_;
  const Statement &cond_true_stmt_;
};

IfStatement::~IfStatement() {}

typedef IfStatement IF;

class IfElseStatement : public Statement {
public:
  IfElseStatement(const Statement &cond_true, const Statement &cond_false);

  virtual StatementType getStamentType() const { return IF_ELSE_STMT; }
  virtual const CodeExpressionPtr getCode() const {}
  virtual ~IfElseStatement();
};

class ForLoopStatement : public Statement {
public:
  ForLoopStatement(const VariableDeclaration &var_decl, const ExpressionStatment &condition,
                   const ExpressionStatment &advance,
                   const std::vector<StatementPtr> &loop_body = std::vector<StatementPtr>());

  virtual StatementType getStamentType() const { return StatementType::FOR_LOOP_STMT; }
  virtual const CodeExpressionPtr getCode() const {
    std::stringstream code;
    code << "for(" << var_decl_.getCode() << ";" << condition_->getCode()->code_ << ";" << advance_->getCode()->code_
         << "){" << std::endl;
    for (const auto &stmt : loop_body_) {
      code << stmt->getCode()->code_ << ";" << std::endl;
    }
    code << "}" << std::endl;
    return std::make_shared<CodeExpression>(code.str());
  }

  void addStatement(StatementPtr stmt) {
    if (stmt)
      loop_body_.push_back(stmt);
  }

  virtual ~ForLoopStatement();

private:
  VariableDeclaration var_decl_;
  ExpressionStatmentPtr condition_;
  ExpressionStatmentPtr advance_;
  std::vector<StatementPtr> loop_body_;
};

typedef ForLoopStatement FOR;

ForLoopStatement::ForLoopStatement(const VariableDeclaration &var_decl, const ExpressionStatment &condition,
                                   const ExpressionStatment &advance, const std::vector<StatementPtr> &loop_body)
    : var_decl_(var_decl), condition_(condition.copy()), advance_(advance.copy()), loop_body_(loop_body) {}

ForLoopStatement::~ForLoopStatement() {}

class FunctionCallStatement : public Statement {
  virtual StatementType getStamentType() const {}
  virtual const CodeExpressionPtr getCode() const {}
  virtual ~FunctionCallStatement();
};

class UserDefinedDataType : public DataType {
public:
  UserDefinedDataType(const StructDeclaration &decl) : decl_(decl) {}

  ValueTypePtr getDefaultInitValue() const { return ValueTypePtr(); }

  ValueTypePtr getNullValue() const { return ValueTypePtr(); }
  uint32_t getSizeBytes() const {
    /* assume a 64 bit architecture, each pointer is 8 bytes */
    return decl_.getTypeSizeInBytes();
  }
  const std::string toString() const { return std::string("STRUCT ") + decl_.getTypeName(); }
  const CodeExpressionPtr getTypeDefinitionCode() const { return std::make_shared<CodeExpression>(decl_.getCode()); }
  const CodeExpressionPtr getCode() const { return std::make_shared<CodeExpression>(decl_.getTypeName()); }

  ~UserDefinedDataType();

private:
  StructDeclaration decl_;
};
UserDefinedDataType::~UserDefinedDataType() {}

const DataTypePtr createUserDefinedType(const StructDeclaration &decl) {
  return std::make_shared<UserDefinedDataType>(decl);
}



int CodeGenTest() {

  VariableDeclaration var_decl = VariableDeclaration::create(createDataType(BasicType::UINT32), "global_int");

  VariableDeclaration var_decl_i =
      VariableDeclaration::create(createDataType(BasicType(INT32)), "i", createBasicTypeValue(BasicType(INT32), "0"));
  VariableDeclaration var_decl_j =
      VariableDeclaration::create(createDataType(BasicType(INT32)), "j", createBasicTypeValue(BasicType(INT32), "5"));
  VariableDeclaration var_decl_k =
      VariableDeclaration::create(createDataType(BasicType(INT32)), "k", createBasicTypeValue(BasicType(INT32), "7"));
  VariableDeclaration var_decl_l =
      VariableDeclaration::create(createDataType(BasicType(INT32)), "l", createBasicTypeValue(BasicType(INT32), "2"));

  {
    BinaryOperatorStatement bin_op(VarRefStatement(var_decl_i), PLUS_OP, VarRefStatement(var_decl_j));
    std::cout << bin_op.getCode()->code_ << std::endl;
    CodeExpressionPtr code = bin_op.addRight(PLUS_OP, VarRefStatement(var_decl_k)).getCode();

    std::cout << code->code_ << std::endl;
  }
  {
    CodeExpressionPtr code = BinaryOperatorStatement(VarRefStatement(var_decl_i), PLUS_OP, VarRefStatement(var_decl_j))
                                 .addRight(PLUS_OP, VarRefStatement(var_decl_k))
                                 .addRight(MULTIPLY_OP, VarRefStatement(var_decl_i), BRACKETS)
                                 .addRight(GREATER_THEN_OP, VarRefStatement(var_decl_l))
                                 .getCode();

    std::cout << code->code_ << std::endl;

    std::cout << "=========================" << std::endl;

    std::cout << BinaryOperatorStatement(VarRefStatement(var_decl_i), PLUS_OP, VarRefStatement(var_decl_j)).getCode()->code_ << std::endl;
    std::cout << (VarRefStatement(var_decl_i) + VarRefStatement(var_decl_j)).getCode()->code_ << std::endl;

    std::cout << "=========================" << std::endl;

    std::cout << UnaryOperatorStatement(VarRefStatement(var_decl_i),POSTFIX_INCREMENT_OP).getCode()->code_ << std::endl;
    std::cout << (++VarRefStatement(var_decl_i)).getCode()->code_ << std::endl;
    std::cout << (VarRefStatement(var_decl_i) >= VarRefStatement(var_decl_j))[VarRefStatement(var_decl_j)].getCode()->code_ << std::endl;

    std::cout << ((~VarRefStatement(var_decl_i) >= VarRefStatement(var_decl_j) << ConstantExprStatement(createBasicTypeValue(INT32,"0"))))[VarRefStatement(var_decl_j)].getCode()->code_ << std::endl;

    std::cout << (assign(VarRefStatement(var_decl_i),VarRefStatement(var_decl_i)+VarRefStatement(var_decl_i))).getCode()->code_ << std::endl;

    std::cout << (sizeOf(VarRefStatement(var_decl_i))).getCode()->code_ << std::endl;

    std::cout << assign(VarRef(var_decl_i), VarRef(var_decl_i)).getCode()->code_  << std::endl;

    std::cout << IF(VarRef(var_decl_i)<VarRef(var_decl_j),
                    assign(VarRef(var_decl_i), VarRef(var_decl_i)*VarRef(var_decl_k))).getCode()->code_  << std::endl;

    std::cout << "=========================" << std::endl;


    std::cout << IfStatement(
                     BinaryOperatorStatement(VarRefStatement(var_decl_i), GREATER_THEN_OP, VarRefStatement(var_decl_j)),
                     ReturnStatement(VarRefStatement(var_decl_i)))
                     .getCode()
                     ->code_
              << std::endl;

    std::cout << IfStatement(VarRefStatement(var_decl_j), VarRefStatement(var_decl_i)).getCode()->code_ << std::endl;
  }

  {
    std::cout << BinaryOperatorStatement(
                     VarRefStatement(var_decl_k), ASSIGNMENT_OP,
                     BinaryOperatorStatement(VarRefStatement(var_decl_j), GREATER_THEN_OP, VarRefStatement(var_decl_i)))
                     .getCode()
                     ->code_
              << std::endl;
  }

  {
    VariableDeclaration var_decl_num_tup = VariableDeclaration::create(createDataType(BasicType(INT32)), "num_tuples",
                                                                       createBasicTypeValue(BasicType(INT32), "0"));

    std::cout << toString(ADDRESS_OF_OP) << ": "
              << UnaryOperatorStatement(VarRefStatement(var_decl_num_tup), ADDRESS_OF_OP).getCode()->code_ << std::endl;
    std::cout << toString(DEREFERENCE_POINTER_OP) << ": "
              << UnaryOperatorStatement(VarRefStatement(var_decl_num_tup), DEREFERENCE_POINTER_OP).getCode()->code_
              << std::endl;
    std::cout << toString(PREFIX_INCREMENT_OP) << ": "
              << UnaryOperatorStatement(VarRefStatement(var_decl_num_tup), PREFIX_INCREMENT_OP).getCode()->code_
              << std::endl;
    std::cout << toString(PREFIX_DECREMENT_OP) << ": "
              << UnaryOperatorStatement(VarRefStatement(var_decl_num_tup), PREFIX_DECREMENT_OP).getCode()->code_
              << std::endl;
    std::cout << toString(POSTFIX_INCREMENT_OP) << ": "
              << UnaryOperatorStatement(VarRefStatement(var_decl_num_tup), POSTFIX_INCREMENT_OP).getCode()->code_
              << std::endl;
    std::cout << toString(POSTFIX_DECREMENT_OP) << ": "
              << UnaryOperatorStatement(VarRefStatement(var_decl_num_tup), POSTFIX_DECREMENT_OP).getCode()->code_
              << std::endl;
    std::cout << toString(BITWISE_COMPLEMENT_OP) << ": "
              << UnaryOperatorStatement(VarRefStatement(var_decl_num_tup), BITWISE_COMPLEMENT_OP).getCode()->code_
              << std::endl;
    std::cout << toString(LOGICAL_NOT_OP) << ": "
              << UnaryOperatorStatement(VarRefStatement(var_decl_num_tup), LOGICAL_NOT_OP).getCode()->code_
              << std::endl;
    std::cout << toString(SIZE_OF_TYPE_OP) << ": "
              << UnaryOperatorStatement(VarRefStatement(var_decl_num_tup), SIZE_OF_TYPE_OP).getCode()->code_
              << std::endl;
  }

  {

    VariableDeclaration var_decl_q =
        VariableDeclaration::create(createDataType(BasicType(INT32)), "q", createBasicTypeValue(BasicType(INT32), "0"));
    VariableDeclaration var_decl_num_tup = VariableDeclaration::create(createDataType(BasicType(INT32)), "num_tuples",
                                                                       createBasicTypeValue(BasicType(INT32), "0"));

    VariableDeclaration var_decl_sum = VariableDeclaration::create(createDataType(BasicType(INT32)), "sum",
                                                                   createBasicTypeValue(BasicType(INT32), "0"));

    ForLoopStatement loop_stmt(var_decl_q, BinaryOperatorStatement(VarRefStatement(var_decl_q), LESS_THEN_OP,
                                                                   VarRefStatement(var_decl_num_tup)),
                               UnaryOperatorStatement(VarRefStatement(var_decl_q), PREFIX_INCREMENT_OP));

    loop_stmt.addStatement(BinaryOperatorStatement(VarRefStatement(var_decl_sum), ASSIGNMENT_OP,
                                                   BinaryOperatorStatement(VarRefStatement(var_decl_sum), PLUS_OP,
                                                                           VarRefStatement(var_decl_q)))
                               .copy());

    std::cout << loop_stmt.getCode()->code_ << std::endl;

    std::cout << ForLoopStatement(var_decl_q, BinaryOperatorStatement(VarRefStatement(var_decl_q), LESS_THEN_OP,
                                                                      VarRefStatement(var_decl_num_tup)),
                                  UnaryOperatorStatement(VarRefStatement(var_decl_q), PREFIX_INCREMENT_OP))
                     .getCode()
                     ->code_
              << std::endl;

    std::cout << BinaryOperatorStatement(VarRefStatement(var_decl_k), ASSIGNMENT_OP,
                                         BinaryOperatorStatement(VarRefStatement(var_decl_j), GREATER_THEN_OP,
                                                                 ConstantExprStatement(INT32, "5")))
                     .getCode()
                     ->code_
              << std::endl;
  }
  /* pointers */
  {

    DataTypePtr val = createPointerDataType(BasicType(INT32));
    assert(val != nullptr);
    VariableDeclaration var_decl_i =
        VariableDeclaration::create(createDataType(BasicType(INT32)), "i", createBasicTypeValue(BasicType(INT32), "0"));
    VariableDeclaration var_decl_p = VariableDeclaration::create(val, "array");

    std::cout << var_decl_i.getCode() << std::endl;
    std::cout << var_decl_p.getCode() << std::endl;

    StructDeclaration struct_decl =
        StructDeclaration::create("TupleBuffer", "buffer")
            .addField(VariableDeclaration::create(createDataType(BasicType(UINT64)), "num_tuples",
                                                  createBasicTypeValue(BasicType(UINT64), "0")))
            .addField(var_decl_p);

    std::cout << VariableDeclaration::create(createUserDefinedType(struct_decl), "buffer").getCode() << std::endl;
    std::cout
        << VariableDeclaration::create(createPointerDataType(createUserDefinedType(struct_decl)), "buffer").getCode()
        << std::endl;
    std::cout << createPointerDataType(createUserDefinedType(struct_decl))->getCode()->code_ << std::endl;

    std::cout << VariableDeclaration::create(createPointerDataType(createUserDefinedType(struct_decl)), "buffer")
                     .getTypeDefinitionCode()
              << std::endl;
  }

  /** define structure of TupleBuffer
    struct TupleBuffer {
      void *data;
      uint64_t buffer_size;
      uint64_t tuple_size_bytes;
      uint64_t num_tuples;
    };
  */
  StructDeclaration struct_decl_tuple_buffer =
      StructDeclaration::create("TupleBuffer", "")
          .addField(VariableDeclaration::create(createPointerDataType(createDataType(BasicType(VOID_TYPE))), "data"))
          .addField(VariableDeclaration::create(createDataType(BasicType(UINT64)), "buffer_size"))
          .addField(VariableDeclaration::create(createDataType(BasicType(UINT64)), "tuple_size_bytes"))
          .addField(VariableDeclaration::create(createDataType(BasicType(UINT64)), "num_tuples"));

  /**
     define the WindowState struct
   struct WindowState {
    void *window_state;
    };
  */
  StructDeclaration struct_decl_state =
      StructDeclaration::create("WindowState", "")
          .addField(
              VariableDeclaration::create(createPointerDataType(createDataType(BasicType(VOID_TYPE))), "window_state"));

  VariableDeclaration var_decl_tuple_buffers = VariableDeclaration::create(
      createPointerDataType(createPointerDataType(createUserDefinedType(struct_decl_tuple_buffer))), "window_buffer");
  VariableDeclaration var_decl_tuple_buffer_output = VariableDeclaration::create(
      createPointerDataType(createUserDefinedType(struct_decl_tuple_buffer)), "output_tuple_buffer");
  VariableDeclaration var_decl_state =
      VariableDeclaration::create(createPointerDataType(createUserDefinedType(struct_decl_state)), "global_state");

  /* struct definition for input tuples */

  StructDeclaration struct_decl_tuple =
      StructDeclaration::create("Tuple", "")
          .addField(VariableDeclaration::create(createDataType(BasicType(UINT64)), "campaign_id"));
  //.addField(VariableDeclaration::create(createDataType(BasicType(UINT64)),"payload"));

  /* Tuple *tuples; */

  VariableDeclaration var_decl_tuple =
      VariableDeclaration::create(createPointerDataType(createUserDefinedType(struct_decl_tuple)), "tuples");

  /* struct definition for result tuples */
  StructDeclaration struct_decl_result_tuple =
      StructDeclaration::create("ResultTuple", "")
          .addField(VariableDeclaration::create(createDataType(BasicType(UINT64)), "sum"));

  VariableDeclaration var_decl_result_tuple = VariableDeclaration::create(
      createPointerDataType(createUserDefinedType(struct_decl_result_tuple)), "result_tuples");

  DeclarationPtr decl = struct_decl_tuple.getField("campaign_id");
  assert(decl != nullptr);
  VariableDeclaration decl_field_campaign_id = VariableDeclaration::create(decl->getType(), decl->getIdentifierName());

  DeclarationPtr decl_field_tup_buf_num_tuples = struct_decl_tuple_buffer.getField("num_tuples");
  assert(decl_field_tup_buf_num_tuples != nullptr);
  VariableDeclaration decl_field_num_tuples_struct_tuple_buf = VariableDeclaration::create(
      decl_field_tup_buf_num_tuples->getType(), decl_field_tup_buf_num_tuples->getIdentifierName());
  DeclarationPtr decl_field_tup_buf_data_ptr = struct_decl_tuple_buffer.getField("data");
  assert(decl_field_tup_buf_data_ptr != nullptr);
  VariableDeclaration decl_field_data_ptr_struct_tuple_buf = VariableDeclaration::create(
      decl_field_tup_buf_data_ptr->getType(), decl_field_tup_buf_data_ptr->getIdentifierName());

  DeclarationPtr decl_field_result_tuple_sum = struct_decl_result_tuple.getField("sum");
  assert(decl_field_result_tuple_sum != nullptr);
  VariableDeclaration var_decl_field_result_tuple_sum = VariableDeclaration::create(
      decl_field_result_tuple_sum->getType(), decl_field_result_tuple_sum->getIdentifierName());

  std::cout << BinaryOperatorStatement(VarRefStatement(var_decl_tuple), ARRAY_REFERENCE_OP,
                                       ConstantExprStatement(INT32, "0"))
                   .getCode()
                   ->code_
            << std::endl;
  std::cout << BinaryOperatorStatement(VarRefStatement(var_decl_tuple), ARRAY_REFERENCE_OP, VarRefStatement(var_decl_i))
                   .getCode()
                   ->code_
            << std::endl;
  std::cout << BinaryOperatorStatement(BinaryOperatorStatement(VarRefStatement(var_decl_tuple), ARRAY_REFERENCE_OP,
                                                               VarRefStatement(var_decl_i)),
                                       MEMBER_SELECT_REFERENCE_OP, VarRefStatement(decl_field_campaign_id))
                   .getCode()
                   ->code_
            << std::endl;

  /* generating the query function */

  /* variable declarations */

  /* TupleBuffer *tuple_buffer_1; */

  VariableDeclaration var_decl_tuple_buffer_1 = VariableDeclaration::create(
      createPointerDataType(createUserDefinedType(struct_decl_tuple_buffer)), "tuple_buffer_1");

  VariableDeclaration var_decl_id =
      VariableDeclaration::create(createDataType(BasicType(UINT64)), "id", createBasicTypeValue(BasicType(INT32), "0"));
  VariableDeclaration var_decl_num_tup = VariableDeclaration::create(createDataType(BasicType(INT32)), "num_tuples",
                                                                     createBasicTypeValue(BasicType(INT32), "0"));

  VariableDeclaration var_decl_sum =
      VariableDeclaration::create(createDataType(BasicType(INT32)), "sum", createBasicTypeValue(BasicType(INT32), "0"));

  BinaryOperatorStatement init_tuple_buffer_ptr(VarRefStatement(var_decl_tuple_buffer_1), ASSIGNMENT_OP,
                                                BinaryOperatorStatement(VarRefStatement(var_decl_tuple_buffers),
                                                                        ARRAY_REFERENCE_OP,
                                                                        ConstantExprStatement(INT32, "0")));
  BinaryOperatorStatement init_tuple_ptr(
      VarRefStatement(var_decl_tuple), ASSIGNMENT_OP,
      TypeCastExprStatement(BinaryOperatorStatement(VarRefStatement(var_decl_tuple_buffer_1), MEMBER_SELECT_POINTER_OP,
                                                    VarRefStatement(decl_field_data_ptr_struct_tuple_buf)),
                            createPointerDataType(createUserDefinedType(struct_decl_tuple))));

  BinaryOperatorStatement init_result_tuple_ptr(
      VarRefStatement(var_decl_result_tuple), ASSIGNMENT_OP,
      TypeCastExprStatement(BinaryOperatorStatement(VarRefStatement(var_decl_tuple_buffer_output),
                                                    MEMBER_SELECT_POINTER_OP,
                                                    VarRefStatement(decl_field_data_ptr_struct_tuple_buf)),
                            createPointerDataType(createUserDefinedType(struct_decl_result_tuple))));

  /* for (uint64_t id = 0; id < tuple_buffer_1->num_tuples; ++id) */
  ForLoopStatement loop_stmt(
      var_decl_id, BinaryOperatorStatement(
                       VarRefStatement(var_decl_id), LESS_THEN_OP,
                       BinaryOperatorStatement(VarRefStatement(var_decl_tuple_buffer_1), MEMBER_SELECT_POINTER_OP,
                                               VarRefStatement(decl_field_num_tuples_struct_tuple_buf))),
      UnaryOperatorStatement(VarRefStatement(var_decl_id), PREFIX_INCREMENT_OP));

  /* sum = sum + tuples[id].campaign_id; */
  loop_stmt.addStatement(
      BinaryOperatorStatement(
          VarRefStatement(var_decl_sum), ASSIGNMENT_OP,
          BinaryOperatorStatement(
              VarRefStatement(var_decl_sum), PLUS_OP,
              BinaryOperatorStatement(BinaryOperatorStatement(VarRefStatement(var_decl_tuple), ARRAY_REFERENCE_OP,
                                                              VarRefStatement(var_decl_id)),
                                      MEMBER_SELECT_REFERENCE_OP, VarRefStatement(decl_field_campaign_id))))
          .copy());

  /* typedef uint32_t (*SharedCLibPipelineQueryPtr)(TupleBuffer**, WindowState*, TupleBuffer*); */

  VariableDeclaration var_decl_return =
      VariableDeclaration::create(createDataType(BasicType(INT32)), "ret", createBasicTypeValue(BasicType(INT32), "0"));

  FunctionDeclaration main_function =
      FunctionBuilder::create("compiled_query")
          .returns(createDataType(BasicType(UINT32)))
          .addParameter(var_decl_tuple_buffers)
          .addParameter(var_decl_state)
          .addParameter(var_decl_tuple_buffer_output)
          .addVariableDeclaration(var_decl_return)
          .addVariableDeclaration(var_decl_tuple)
          .addVariableDeclaration(var_decl_result_tuple)
          .addVariableDeclaration(var_decl_tuple_buffer_1)
          .addVariableDeclaration(var_decl_sum)
          .addStatement(init_tuple_buffer_ptr.copy())
          .addStatement(init_tuple_ptr.copy())
          .addStatement(init_result_tuple_ptr.copy())
          .addStatement(StatementPtr(new ForLoopStatement(loop_stmt)))
          .addStatement( /*   result_tuples[0].sum = sum; */
              BinaryOperatorStatement(BinaryOperatorStatement(VarRefStatement(var_decl_result_tuple),
                                                              ARRAY_REFERENCE_OP, ConstantExprStatement(INT32, "0")),
                                      MEMBER_SELECT_REFERENCE_OP,
                                      BinaryOperatorStatement(VarRefStatement(var_decl_field_result_tuple_sum),
                                                              ASSIGNMENT_OP, VarRefStatement(var_decl_sum)))
                  .copy())
            /* return ret; */
          .addStatement(StatementPtr(new ReturnStatement(VarRefStatement(var_decl_return))))
          .build();

  CodeFile file = FileBuilder::create("query.cpp")
                      .addDeclaration(struct_decl_tuple_buffer)
                      .addDeclaration(struct_decl_state)
                      .addDeclaration(struct_decl_tuple)
                      .addDeclaration(struct_decl_result_tuple)
                      .addDeclaration(var_decl)
                      .addDeclaration(main_function)
                      .build();

  PipelineStagePtr stage = compile(file);

  uint64_t *my_array = (uint64_t *)malloc(100 * sizeof(uint64_t));

  for (unsigned int i = 0; i < 100; ++i) {
    my_array[i] = i;
  }

  TupleBuffer buf{my_array, 100 * sizeof(uint64_t), sizeof(uint64_t), 100};

  uint64_t *result_array = (uint64_t *)malloc(1 * sizeof(uint64_t));

  std::vector<TupleBuffer *> bufs;
  bufs.push_back(&buf);

  TupleBuffer result_buf{result_array, sizeof(uint64_t), sizeof(uint64_t), 0};

  if (!stage->execute(bufs, nullptr, &result_buf)) {
    std::cout << "Error!" << std::endl;
  }

  std::cout << "Sum (Generated Code): " << result_array[0] << std::endl;

  uint64_t my_sum = 0;
  for (uint64_t i = 0; i < 100; ++i) {
    my_sum += my_array[i];
  }
  std::cout << "my sum: " << my_sum << std::endl;

  free(my_array);
  free(result_array);

  // stage->execute();

  return 0;
}
}
