
#include <string>
#include <iostream>
#include <sstream>
#include <vector>


#include <Core/DataTypes.hpp>
#include <CodeGen/C_CodeGen/CodeCompiler.hpp>
#include <CodeGen/PipelineStage.hpp>
#include <CodeGen/CodeExpression.hpp>
#include <Util/ErrorHandling.hpp>

namespace iotdb {

    enum StatementType{RETURN_STMT,IF_STMT,IF_ELSE_STMT,FOR_LOOP_STMT,FUNC_CALL_STMT,VAR_REF_STMT, BINARY_OP_STMT};

    class Statement{
    public:
       virtual StatementType getStamentType() const = 0;
       virtual const CodeExpressionPtr getCode() const = 0;
       virtual ~Statement();
    };

    Statement::~Statement(){

    }

    typedef std::shared_ptr<Statement> StatementPtr;


    typedef std::string Code;

    class Declaration{
    public:
      virtual const Code getCode() const = 0;
      virtual ~Declaration(){}
    };

    class StructDeclaration : public Declaration{
    public:
      const Code getCode() const override{
         return Code();
      }
    };

    class VariableDeclaration : public Declaration{
    public:
        static VariableDeclaration create(DataTypePtr type, const std::string& identifier, ValueTypePtr value=nullptr);
        const Code getCode() const override{
          std::stringstream str;
          str << type_->getCode()->code_ << " " << identifier_;
          if(init_value_){
            str << " = " << init_value_->getCodeExpression()->code_;
          }
          return str.str();
        }

        const CodeExpressionPtr getIdentifier() const{
          return CodeExpressionPtr(new CodeExpression(identifier_));
        }

        const DataTypePtr getDataType() const{
          return type_;
        }
    private:
        VariableDeclaration(DataTypePtr type, const std::string& identifier, ValueTypePtr value=nullptr);
        DataTypePtr type_;
        std::string identifier_;
        ValueTypePtr init_value_;
    };

    VariableDeclaration::VariableDeclaration(DataTypePtr type, const std::string& identifier, ValueTypePtr value)
      : type_(type), identifier_(identifier), init_value_(value){}

    VariableDeclaration VariableDeclaration::create(DataTypePtr type, const std::string& identifier, ValueTypePtr value){
      return VariableDeclaration(type,identifier,value);
    }

    class FunctionDeclaration : public Declaration{
     private:
      Code function_code;
    public:
      FunctionDeclaration(Code code) : function_code(code){}

      const Code getCode() const override{
         return function_code;
      }
    };

    class StructBuilder{
        public:
        static StructBuilder create(const std::string& struct_name);
        StructBuilder& addField(AttributeFieldPtr attr);
        StructDeclaration build();
    };

    class StatementBuilder{
        public:
        static StatementBuilder create(const std::string& struct_name);


    };




    class FunctionBuilder {
    private:
      std::string name;
      DataTypePtr returnType;
      std::vector<VariableDeclaration> parameters;
      std::vector<VariableDeclaration> variable_declarations;
      std::vector<StatementPtr> statements;
      FunctionBuilder(const std::string& function_name);
    public:
      static FunctionBuilder create(const std::string& function_name);

      FunctionBuilder &returns(DataTypePtr returnType_);
      FunctionBuilder &addParameter(VariableDeclaration var_decl);
      FunctionBuilder &addStatement(StatementPtr statement);
      FunctionBuilder &addVariableDeclaration(VariableDeclaration var_decl);
      FunctionDeclaration build();
    };

    FunctionBuilder::FunctionBuilder(const std::string& function_name) : name(function_name){

    }

    FunctionBuilder FunctionBuilder::create(const std::string& function_name){
      return FunctionBuilder(function_name);
    }

    FunctionDeclaration FunctionBuilder::build(){
      std::stringstream function;
      if(!returnType){
       function << "void";
      } else {
       function << returnType->getCode()->code_;
      }
      function << " " << name << "(";
      for(uint64_t i=0;i<parameters.size();++i){
         function << parameters[i].getCode();
         if(i+1<parameters.size())
           function << ", ";
      }
      function << "){";

      for(uint64_t i=0;i<variable_declarations.size();++i){
         function << variable_declarations[i].getCode() << ";";
      }

      for(uint64_t i=0;i<statements.size();++i){
         function << statements[i]->getCode()->code_ << ";";
      }
      function << "}";

      return FunctionDeclaration(function.str());
    }

    FunctionBuilder &FunctionBuilder::returns(DataTypePtr type){
        returnType = type;
        return *this;
    }

    FunctionBuilder &FunctionBuilder::addParameter(VariableDeclaration var_decl){
      parameters.push_back(var_decl);
      return *this;
    }
    FunctionBuilder &FunctionBuilder::addStatement(StatementPtr statement){
      if(statement)
        statements.push_back(statement);
      return *this;
    }

    FunctionBuilder &FunctionBuilder::addVariableDeclaration(VariableDeclaration vardecl){
      variable_declarations.push_back(vardecl);
      return *this;
    }

    class CodeFile{
    public:
      std::string code;
    };

    PipelineStagePtr compile(const CodeFile& file){
      CCodeCompiler compiler;
      CompiledCCodePtr compiled_code = compiler.compile(file.code);
      return createPipelineStage(compiled_code);
    }

    class FileBuilder{
    private:
      std::stringstream declations;
    public:
      static FileBuilder create(const std::string& file_name);
      FileBuilder& addDeclaration(const Declaration&);
      CodeFile build();
    };

    FileBuilder FileBuilder::create(const std::string& file_name){
      FileBuilder builder;
      builder.declations << "#include <cstdint>" << std::endl;
      return builder;
    }
    FileBuilder& FileBuilder::addDeclaration(const Declaration& decl){
      declations << decl.getCode() << ";";
      return *this;
    }
    CodeFile FileBuilder::build(){
      CodeFile file;
      file.code = declations.str();
      return file;
    }

    class VarRefStatement : public Statement{
    public:
      const VariableDeclaration& var_decl_;

      virtual StatementType getStamentType() const{
        return VAR_REF_STMT;
      }

      virtual const CodeExpressionPtr getCode() const{
         return var_decl_.getIdentifier();
      }

     VarRefStatement(const VariableDeclaration& var_decl) : var_decl_(var_decl){

     }

     virtual ~VarRefStatement(){

     }
    };

    enum BinaryOperatorType{EQUAL_OP, UNEQUAL_OP, LESS_THEN_OP,LESS_THEN_EQUAL_OP,GREATER_THEN_OP,GREATER_THEN_EQUAL_OP, PLUS_OP, MINUS_OP, MULTIPLY_OP, DIVISION_OP, MODULO_OP, LOGICAL_AND_OP, LOGICAL_OR_OP, BITWISE_AND_OP, BITWISE_OR_OP, BITWISE_XOR_OP, BITWISE_LEFT_SHIFT_OP, BITWISE_RIGHT_SHIFT_OP};

    const std::string toString(const BinaryOperatorType& type){
      const char* const names[] = {"EQUAL_OP", "UNEQUAL_OP", "LESS_THEN_OP", "LESS_THEN_EQUAL_OP","GREATER_THEN_OP","GREATER_THEN_EQUAL_OP","PLUS_OP",
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
                                   "ASSIGNMENT_OP"};
      return std::string(names[type]);
    }

    const CodeExpressionPtr toCodeExpression(const BinaryOperatorType& type){
      const char* const names[] = {"==", "!=", "<", "<=",">",">=","+",
                                   "-",
                                   "*",
                                   "/",
                                   "%",
                                   "&&",
                                   "||",
                                   "&",
                                   "|",
                                   "^",
                                   "<<",
                                   ">>", "="};
      return std::make_shared<CodeExpression>(names[type]);
    }

    class BinaryOperatorStatement
      : public Statement{
    public:
      BinaryOperatorStatement(const VarRefStatement& lhs,
                              const BinaryOperatorType& op,
                              const VarRefStatement& rhs)
        : lhs_(std::make_shared<VarRefStatement>(lhs)), bin_op_(), op_(op), rhs_(rhs)
      {
      }
      BinaryOperatorStatement(const BinaryOperatorStatement& bin_op,
                              const BinaryOperatorType& op,
                              const VarRefStatement& rhs)
        : lhs_(), bin_op_(std::make_shared<BinaryOperatorStatement>(bin_op)), op_(op), rhs_(rhs)
      {
      }

      BinaryOperatorStatement addRight(const BinaryOperatorType& op, const VarRefStatement& rhs)
      {
        return BinaryOperatorStatement(*this, op, rhs);
//        if(lhs_){
//            return BinaryOperatorStatement(*lhs_, op, rhs);
//          }else if(bin_op_){
//            return BinaryOperatorStatement(*bin_op_, op, rhs);
//          }else{
//            IOTDB_FATAL_ERROR("In BinaryOperatorStatement: lhs_ and bin_op_ null!");
//          }
      }

      StatementPtr assignToVariable(const VarRefStatement& lhs){
          return StatementPtr();
      }

       virtual StatementType getStamentType() const{
        return BINARY_OP_STMT;
       }
       virtual const CodeExpressionPtr getCode() const{
          CodeExpressionPtr code;
          if(lhs_){
            code = combine(lhs_->getCode(),toCodeExpression(op_));
            }else if(bin_op_){
            code = combine(bin_op_->getCode(),toCodeExpression(op_));
            }else{
              IOTDB_FATAL_ERROR("In BinaryOperatorStatement: lhs_ and bin_op_ null!");
            }
          code = combine(code,rhs_.getCode());
          return std::make_shared<CodeExpression>(std::string("(")+code->code_+std::string(")"));
       }

       virtual ~BinaryOperatorStatement();
    private:
      const std::shared_ptr<VarRefStatement> lhs_;
      const std::shared_ptr<BinaryOperatorStatement> bin_op_;
      BinaryOperatorType op_;
      VarRefStatement rhs_;
    };

    BinaryOperatorStatement::~BinaryOperatorStatement(){

    }

    class ReturnStatement : public Statement{
public:
      ReturnStatement(VarRefStatement var_ref) : var_ref_(var_ref) {}

      VarRefStatement var_ref_;

      virtual StatementType getStamentType() const{

      }
      virtual const CodeExpressionPtr getCode() const{
         std::stringstream stmt;
         stmt << "return " << var_ref_.getCode()->code_;
         return std::make_shared<CodeExpression>(stmt.str());
      }
      virtual ~ReturnStatement(){}
    };

    class IfStatement
      : public Statement{
           virtual StatementType getStamentType() const{

           }
           virtual const CodeExpressionPtr getCode() const{

           }
           virtual ~IfStatement();
    };

    class IfElseStatement : public Statement{
      virtual StatementType getStamentType() const{

      }
      virtual const CodeExpressionPtr getCode() const{

      }
      virtual ~IfElseStatement();

    };

    class ForLoopStatement : public Statement{



      virtual StatementType getStamentType() const{
        return StatementType::FOR_LOOP_STMT;
      }
      virtual const CodeExpressionPtr getCode() const{

      }
      virtual ~ForLoopStatement();

    };

    class FunctionCallStatement : public Statement{
      virtual StatementType getStamentType() const{

      }
      virtual const CodeExpressionPtr getCode() const{

      }
      virtual ~FunctionCallStatement();
    };



    int CodeGenTest(){




        VariableDeclaration var_decl = VariableDeclaration::create(createDataType(BasicType::UINT32),"global_int");

        VariableDeclaration var_decl_i = VariableDeclaration::create(createDataType(BasicType(INT32)),"i",createBasicTypeValue(BasicType(INT32),"0"));
        VariableDeclaration var_decl_j = VariableDeclaration::create(createDataType(BasicType(INT32)),"j",createBasicTypeValue(BasicType(INT32),"5"));
        VariableDeclaration var_decl_k = VariableDeclaration::create(createDataType(BasicType(INT32)),"k",createBasicTypeValue(BasicType(INT32),"7"));
        VariableDeclaration var_decl_l = VariableDeclaration::create(createDataType(BasicType(INT32)),"l",createBasicTypeValue(BasicType(INT32),"2"));

        {
        BinaryOperatorStatement bin_op (VarRefStatement(var_decl_i),PLUS_OP,VarRefStatement(var_decl_j));
        std::cout << bin_op.getCode()->code_ << std::endl;
        CodeExpressionPtr code = bin_op.addRight(PLUS_OP,VarRefStatement(var_decl_k)).getCode();

        std::cout << code->code_ << std::endl;
        }
        {
        CodeExpressionPtr code =
          BinaryOperatorStatement(
              VarRefStatement(var_decl_i),PLUS_OP,VarRefStatement(var_decl_j))
            .addRight(PLUS_OP,VarRefStatement(var_decl_k))
            .addRight(MULTIPLY_OP,VarRefStatement(var_decl_i))
            .addRight(GREATER_THEN_OP,VarRefStatement(var_decl_l)).getCode();

        std::cout << code->code_ << std::endl;
        }

        FunctionDeclaration main_function = FunctionBuilder::create("compiled_query")
            .returns(createDataType(BasicType(INT32)))
            .addParameter(VariableDeclaration::create(
                            createDataType(BasicType(INT32)),
                            "thread_id"))
            .addVariableDeclaration(var_decl_i)
            .addStatement(StatementPtr(new ReturnStatement(VarRefStatement(var_decl_i))))
            .build();
        CodeFile file = FileBuilder::create("query.cpp")
            .addDeclaration(var_decl)
            .addDeclaration(main_function)
            .build();

        PipelineStagePtr stage = compile(file);
        //stage->execute();

      return 0;

    }

}
