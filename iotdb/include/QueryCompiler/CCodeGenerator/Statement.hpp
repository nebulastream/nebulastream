#pragma once

#include <memory>
#include <string>

#include <API/Types/DataTypes.hpp>
#include <QueryCompiler/CCodeGenerator/Declaration.hpp>
#include <QueryCompiler/CodeExpression.hpp>
#include <QueryCompiler/DataTypes/ValueType.hpp>
#include <utility>

namespace NES {

enum StatementType {
    RETURN_STMT,
    IF_STMT,
    IF_ELSE_STMT,
    FOR_LOOP_STMT,
    FUNC_CALL_STMT,
    VAR_REF_STMT,
    VAR_DEC_STMT,
    CONSTANT_VALUE_EXPR_STMT,
    BINARY_OP_STMT,
    UNARY_OP_STMT,
    COMPOUND_STMT
};

enum BracketMode { NO_BRACKETS,
                   BRACKETS };

class Statement;
typedef std::shared_ptr<Statement> StatementPtr;

class Statement {
  public:
    virtual StatementType getStamentType() const = 0;
    virtual const CodeExpressionPtr getCode() const = 0;
    virtual const StatementPtr createCopy() const = 0;
    /** \brief virtual copy constructor */
    virtual ~Statement();
};

class ExpressionStatment;
typedef std::shared_ptr<ExpressionStatment> ExpressionStatmentPtr;

class BinaryOperatorStatement;

class ExpressionStatment : public Statement {
  public:
    virtual StatementType getStamentType() const = 0;
    virtual const CodeExpressionPtr getCode() const = 0;
    /** \brief virtual copy constructor */
    virtual const ExpressionStatmentPtr copy() const = 0;
    /** \brief virtual copy constructor of base class
     *  \todo create one unified version, having this twice is problematic
    */
    const StatementPtr createCopy() const override final;
    //  UnaryOperatorStatement operator sizeof(const ExpressionStatment &ref){
    //  return UnaryOperatorStatement(ref, SIZE_OF_TYPE_OP);
    //  }

    BinaryOperatorStatement assign(const ExpressionStatment& ref);
    BinaryOperatorStatement accessPtr(const ExpressionStatment& ref);
    BinaryOperatorStatement accessRef(const ExpressionStatment& ref);

    BinaryOperatorStatement operator[](const ExpressionStatment& ref);

    // UnaryOperatorStatement operator ++(const ExpressionStatment &ref){
    // return UnaryOperatorStatement(ref, POSTFIX_INCREMENT_OP);
    //}
    // UnaryOperatorStatement operator --(const ExpressionStatment &ref){
    // return UnaryOperatorStatement(ref, POSTFIX_DECREMENT_OP);
    //}

    virtual ~ExpressionStatment();
};

typedef std::string Code;

class ConstantExprStatement : public ExpressionStatment {
  public:
    ValueTypePtr val_;

    virtual StatementType getStamentType() const { return CONSTANT_VALUE_EXPR_STMT; }

    virtual const CodeExpressionPtr getCode() const { return val_->getCodeExpression(); }

    virtual const ExpressionStatmentPtr copy() const { return std::make_shared<ConstantExprStatement>(*this); }

    ConstantExprStatement(const ValueTypePtr& val) : val_(val) {}

    ConstantExprStatement(const BasicType& type, const std::string& value) : val_(createBasicTypeValue(type, value)) {}
    ConstantExprStatement(int32_t value) : ConstantExprStatement(INT32, std::to_string(value)) {}

    virtual ~ConstantExprStatement();
};

typedef ConstantExprStatement Constant;

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

    TypeCastExprStatement(const ExpressionStatment& expr, const DataTypePtr& type) : expr_(expr.copy()), type_(type) {}

    virtual ~TypeCastExprStatement();

  private:
    ExpressionStatmentPtr expr_;
    DataTypePtr type_;
};

typedef TypeCastExprStatement TypeCast;

class VarRefStatement : public ExpressionStatment {
  public:
    VariableDeclarationPtr varDeclaration;

    virtual StatementType getStamentType() const { return VAR_REF_STMT; }

    virtual const CodeExpressionPtr getCode() const { return varDeclaration->getIdentifier(); }

    virtual const ExpressionStatmentPtr copy() const { return std::make_shared<VarRefStatement>(*this); }

    VarRefStatement(const VariableDeclaration& var_decl)
        : varDeclaration(std::dynamic_pointer_cast<VariableDeclaration>(var_decl.copy())) {
    }

    VarRefStatement(VariableDeclarationPtr varDeclaration)
        : varDeclaration(std::move(varDeclaration)) {
    }

    virtual ~VarRefStatement();
};

typedef VarRefStatement VarRef;

class VarDeclStatement : public ExpressionStatment {
  public:
    std::shared_ptr<VariableDeclaration> var_decl_;

    virtual StatementType getStamentType() const { return VAR_DEC_STMT; }

    virtual const CodeExpressionPtr getCode() const {
        return std::make_shared<CodeExpression>(var_decl_->getCode());
    }

    virtual const ExpressionStatmentPtr copy() const { return std::make_shared<VarDeclStatement>(*this); }

    VarDeclStatement(const VariableDeclaration& var_decl)
        : var_decl_(std::dynamic_pointer_cast<VariableDeclaration>(var_decl.copy())) {
    }

    virtual ~VarDeclStatement();
};

typedef VarRefStatement VarRef;

class ReturnStatement : public Statement {
  public:
    ReturnStatement(VarRefStatement var_ref) : var_ref_(var_ref) {}

    VarRefStatement var_ref_;

    virtual StatementType getStamentType() const { return RETURN_STMT; }
    virtual const CodeExpressionPtr getCode() const {
        std::stringstream stmt;
        stmt << "return " << var_ref_.getCode()->code_ << ";";
        return std::make_shared<CodeExpression>(stmt.str());
    }
    const StatementPtr createCopy() const override {
        return std::make_shared<ReturnStatement>(*this);
    }
    virtual ~ReturnStatement() {}
};

/** \brief stores a sequence of statements and is a basic building block for any AST */
class CompoundStatement : public Statement {
  public:
    CompoundStatement();

    virtual StatementType getStamentType() const override;
    virtual const CodeExpressionPtr getCode() const override;
    const StatementPtr createCopy() const override {
        return std::make_shared<CompoundStatement>(*this);
    }

    void addStatement(StatementPtr stmt);

    virtual ~CompoundStatement() override;

  private:
    std::vector<StatementPtr> statements_;
};
typedef std::shared_ptr<CompoundStatement> CompoundStatementPtr;

class IfStatement : public Statement {
  public:
    IfStatement(const Statement& cond_expr) : cond_expr_(cond_expr.createCopy()),
                                              cond_true_stmt_(new CompoundStatement()) {
    }
    IfStatement(const Statement& cond_expr, const Statement& cond_true_stmt)
        : cond_expr_(cond_expr.createCopy()), cond_true_stmt_(new CompoundStatement()) {
        cond_true_stmt_->addStatement(cond_true_stmt.createCopy());
    }

    virtual StatementType getStamentType() const { return IF_STMT; }
    virtual const CodeExpressionPtr getCode() const {
        std::stringstream code;
        code << "if(" << cond_expr_->getCode()->code_ << "){" << std::endl;
        code << cond_true_stmt_->getCode()->code_ << std::endl;
        code << "}" << std::endl;
        return std::make_shared<CodeExpression>(code.str());
    }
    const StatementPtr createCopy() const override {
        return std::make_shared<IfStatement>(*this);
    }

    const CompoundStatementPtr getCompoundStatement() {
        return cond_true_stmt_;
    }

    virtual ~IfStatement();

  private:
    const StatementPtr cond_expr_;
    CompoundStatementPtr cond_true_stmt_;
};

typedef IfStatement IF;

class IfElseStatement : public Statement {
  public:
    IfElseStatement(const Statement& cond_true, const Statement& cond_false);

    virtual StatementType getStamentType() const { return IF_ELSE_STMT; }
    virtual const CodeExpressionPtr getCode() const {
        return std::make_shared<CodeExpression>("");
    }
    const StatementPtr createCopy() const override {
        return std::make_shared<IfElseStatement>(*this);
    }
    virtual ~IfElseStatement();
};

class ForLoopStatement : public Statement {
  public:
    ForLoopStatement(DeclarationPtr variableDeclarationPtr, ExpressionStatmentPtr condition,
                     ExpressionStatmentPtr advance,
                     const std::vector<StatementPtr>& loop_body = std::vector<StatementPtr>());

    virtual StatementType getStamentType() const;
    virtual const CodeExpressionPtr getCode() const;
    const StatementPtr createCopy() const override {
        return std::make_shared<ForLoopStatement>(*this);
    }

    void addStatement(StatementPtr stmt);
    const CompoundStatementPtr getCompoundStatement() {
        return body;
    }
    virtual ~ForLoopStatement();

  private:
    DeclarationPtr varDeclaration;
    ExpressionStatmentPtr condition;
    ExpressionStatmentPtr advance;
    CompoundStatementPtr body;
};

typedef ForLoopStatement FOR;

class FunctionCallStatement : public ExpressionStatment {
  public:
    virtual StatementType getStamentType() const { return FUNC_CALL_STMT; }

    virtual const CodeExpressionPtr getCode() const {

        u_int32_t i;

        CodeExpressionPtr code;
        code = combine(std::make_shared<CodeExpression>(functionname_), std::make_shared<CodeExpression>("("));
        for (i = 0; i < expr_.size(); i++) {
            if (i != 0)
                code = combine(code, std::make_shared<CodeExpression>(", "));
            code = combine(code, expr_.at(i)->getCode());
        }
        code = combine(code, std::make_shared<CodeExpression>(")"));
        return code;
    }

    virtual const ExpressionStatmentPtr copy() const { return std::make_shared<FunctionCallStatement>(*this); }

    virtual void addParameter(const ExpressionStatment& expr) { expr_.push_back(expr.copy()); }
    virtual void addParameter(ExpressionStatmentPtr expr) { expr_.push_back(expr); }

    FunctionCallStatement(const std::string functionname) : functionname_(functionname) {}

    virtual ~FunctionCallStatement();

  private:
    std::string functionname_;
    std::vector<ExpressionStatmentPtr> expr_;
};

class AnonymUserDefinedDataType : public DataType {
  public:
    AnonymUserDefinedDataType(const std::string name) : name(name) {}

    ValueTypePtr getDefaultInitValue() const { return ValueTypePtr(); }

    ValueTypePtr getNullValue() const { return ValueTypePtr(); }
    uint32_t getSizeBytes() const {
        /* assume a 64 bit architecture, each pointer is 8 bytes */
        return -1;
    }
    const std::string toString() const { return std::string("STRUCT ") + name; }
    const std::string convertRawToString(void* data) const override { return ""; }
    const CodeExpressionPtr getTypeDefinitionCode() const { return std::make_shared<CodeExpression>(name); }
    const CodeExpressionPtr getCode() const { return std::make_shared<CodeExpression>(name); }
    const CodeExpressionPtr getDeclCode(const std::string& identifier) const override {
        std::stringstream str;
        str << " " << identifier;
        return combine(getCode(), std::make_shared<CodeExpression>(str.str()));
    }
    bool isArrayDataType() const override { return false; }
    bool isCharDataType() const override { return false; }
    const DataTypePtr copy() const override {
        return std::make_shared<AnonymUserDefinedDataType>(*this);
    }
    bool isEqual(DataTypePtr ptr) const override {
        std::shared_ptr<AnonymUserDefinedDataType> temp = std::dynamic_pointer_cast<AnonymUserDefinedDataType>(ptr);
        if (temp)
            return isEqual(temp);
        return false;
    }

    // \todo: isEqual for structType
    const bool isEqual(std::shared_ptr<AnonymUserDefinedDataType> btr) {
        return true;
    }

    bool operator==(const DataType& _rhs) const override {
        try {
            auto rhs = dynamic_cast<const NES::AnonymUserDefinedDataType&>(_rhs);
            return name == rhs.name;
        } catch (...) {
            return false;
        }
    }

    ~AnonymUserDefinedDataType();

  private:
    const std::string name;
};

class UserDefinedDataType : public DataType {
  public:
    UserDefinedDataType(const StructDeclaration& decl) : decl_(decl) {}

    ValueTypePtr getDefaultInitValue() const { return ValueTypePtr(); }

    ValueTypePtr getNullValue() const { return ValueTypePtr(); }
    uint32_t getSizeBytes() const {
        /* assume a 64 bit architecture, each pointer is 8 bytes */
        return decl_.getTypeSizeInBytes();
    }
    const std::string toString() const { return std::string("STRUCT ") + decl_.getTypeName(); }
    const std::string convertRawToString(void* data) const override { return ""; }
    const CodeExpressionPtr getTypeDefinitionCode() const { return std::make_shared<CodeExpression>(decl_.getCode()); }
    const CodeExpressionPtr getCode() const { return std::make_shared<CodeExpression>(decl_.getTypeName()); }
    const CodeExpressionPtr getDeclCode(const std::string& identifier) const override { return getCode(); }
    bool isArrayDataType() const override { return false; }
    bool isCharDataType() const override { return false; }
    const DataTypePtr copy() const override {
        return std::make_shared<UserDefinedDataType>(*this);
    }
    bool isEqual(DataTypePtr ptr) const override {
        std::shared_ptr<UserDefinedDataType> temp = std::dynamic_pointer_cast<UserDefinedDataType>(ptr);
        if (temp)
            return isEqual(temp);
        return false;
    }

    // \todo: isEqual for structType
    const bool isEqual(std::shared_ptr<UserDefinedDataType> btr) {
        return true;
    }

    bool operator==(const DataType& _rhs) const override {
        assert(0);
    }

    ~UserDefinedDataType();

  private:
    StructDeclaration decl_;
};

struct AssignmentStatment {
    VariableDeclaration lhs_tuple_var;
    VariableDeclaration lhs_field_var;
    VariableDeclaration lhs_index_var;
    VariableDeclaration rhs_tuple_var;
    VariableDeclaration rhs_field_var;
    VariableDeclaration rhs_index_var;
};

const DataTypePtr createUserDefinedType(const StructDeclaration& decl);
const DataTypePtr createAnonymUserDefinedType(const std::string name);

}// namespace NES
