
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

    enum StatementType{RETURN_STMT,IF_STMT,IF_ELSE_STMT,FOR_LOOP_STMT,FUNC_CALL_STMT,VAR_REF_STMT, CONSTANT_VALUE_EXPR_STMT, BINARY_OP_STMT, UNARY_OP_STMT};

    class Statement{
    public:
       virtual StatementType getStamentType() const = 0;
       virtual const CodeExpressionPtr getCode() const = 0;
       virtual ~Statement();
    };

    Statement::~Statement(){

    }

    class ExpressionStatment;
    typedef std::shared_ptr<ExpressionStatment> ExpressionStatmentPtr;

    class ExpressionStatment : public Statement{
    public:
      virtual StatementType getStamentType() const = 0;
      virtual const CodeExpressionPtr getCode() const = 0;
      /** \brief virtual copy constructor */
      virtual const ExpressionStatmentPtr copy() const = 0;
      virtual ~ExpressionStatment();
    };

    ExpressionStatment::~ExpressionStatment(){
    }

    typedef std::shared_ptr<Statement> StatementPtr;


    typedef std::string Code;

    class Declaration;
    typedef std::shared_ptr<Declaration> DeclarationPtr;

    class Declaration{
    public:
      virtual const Code getTypeDefinitionCode() const = 0;
      virtual const Code getCode() const = 0;
      virtual const DeclarationPtr copy() const = 0;
      virtual ~Declaration();
    };

    Declaration::~Declaration(){
    }

    class StructDeclaration : public Declaration{
    public:
      static StructDeclaration create(const std::string& type_name){
          return StructDeclaration(type_name);
      }

      const Code getTypeDefinitionCode() const override{
        std::stringstream expr;
        expr << "struct " << type_name_ << "{" << std::endl;
        for(auto& decl : decls_){
           expr << decl->getCode() << ";" << std::endl;
        }
        expr << "}";
        return expr.str();
      }

      const Code getCode() const override{
         std::stringstream expr;
         expr << "struct " << type_name_ << "{" << std::endl;
         for(auto& decl : decls_){
            expr << decl->getCode() << ";" << std::endl;
         }
         expr << "}";
         return expr.str();
      }

      const uint32_t getTypeSizeInBytes() const{
        IOTDB_FATAL_ERROR("Called unimplemented function!");
        return 0;
      }

      const std::string getTypeName() const{
        return type_name_;
      }

      const DeclarationPtr copy() const override{
        return std::make_shared<StructDeclaration>(*this);
      }



      StructDeclaration& addField(const Declaration& decl){
        DeclarationPtr decl_p = decl.copy();
        if(decl_p)
          decls_.push_back(decl_p);
        return *this;
      }
      ~StructDeclaration();
    private:
      StructDeclaration(const std::string& type_name) :
        type_name_(type_name), decls_(){}
      std::string type_name_;
      std::vector<DeclarationPtr> decls_;
    };

    StructDeclaration::~StructDeclaration(){

    }

    class VariableDeclaration : public Declaration{
    public:
        static VariableDeclaration create(DataTypePtr type, const std::string& identifier, ValueTypePtr value=nullptr);

        const Code getTypeDefinitionCode() const override{
          return Code();
          //return type_->getCode()->code_;
        }

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

        const DeclarationPtr copy() const override{
          return std::make_shared<VariableDeclaration>(*this);
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
      if(!type)
        IOTDB_FATAL_ERROR("DataTypePtr type is nullptr!");
      return VariableDeclaration(type,identifier,value);
    }

    class FunctionDeclaration : public Declaration{
     private:
      Code function_code;
    public:
      FunctionDeclaration(Code code) : function_code(code){}

      const Code getTypeDefinitionCode() const override{
        return Code();
      }

      const Code getCode() const override{
         return function_code;
      }
      const DeclarationPtr copy() const override{
        return std::make_shared<FunctionDeclaration>(*this);
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

    class ConstantExprStatement : public ExpressionStatment{
    public:
      ValueTypePtr val_;

      virtual StatementType getStamentType() const{
        return CONSTANT_VALUE_EXPR_STMT;
      }

      virtual const CodeExpressionPtr getCode() const{
         return val_->getCodeExpression();
      }

      virtual const ExpressionStatmentPtr copy() const{
        return std::make_shared<ConstantExprStatement>(*this);
      }

     ConstantExprStatement(const ValueTypePtr& val)
       : val_(val){

     }

     ConstantExprStatement(const BasicType & type, const std::string& value)
       : val_(createBasicTypeValue(type, value)){

     }

     virtual ~ConstantExprStatement();
    };

    ConstantExprStatement::~ConstantExprStatement(){}

    class VarRefStatement : public ExpressionStatment{
    public:
      const VariableDeclaration& var_decl_;

      virtual StatementType getStamentType() const{
        return VAR_REF_STMT;
      }

      virtual const CodeExpressionPtr getCode() const{
         return var_decl_.getIdentifier();
      }

      virtual const ExpressionStatmentPtr copy() const{
        return std::make_shared<VarRefStatement>(*this);
      }

     VarRefStatement(const VariableDeclaration& var_decl) : var_decl_(var_decl){

     }

     virtual ~VarRefStatement();
    };

    VarRefStatement::~VarRefStatement(){}

    enum UnaryOperatorType{ADDRESS_OF_OP,
                           DEREFERENCE_POINTER_OP,
                           ARRAY_REFERENCE_OP,
                           MEMBER_SELECT_POINTER_OP,
                           MEMBER_SELECT_REFERENCE_OP,
                           PREFIX_INCREMENT_OP,
                           PREFIX_DECREMENT_OP,
                           POSTFIX_INCREMENT_OP,
                           POSTFIX_DECREMENT_OP,
                           BITWISE_COMPLEMENT_OP,
                           LOGICAL_NOT_OP,
                           SIZE_OF_TYPE_OP
                           };


    const std::string toString(const UnaryOperatorType& type){
      const char* const names[] = {
    "ADDRESS_OF_OP",
    "DEREFERENCE_POINTER_OP",
    "ARRAY_REFERENCE_OP",
    "MEMBER_SELECT_POINTER_OP",
    "MEMBER_SELECT_REFERENCE_OP",
    "PREFIX_INCREMENT_OP",
    "PREFIX_DECREMENT_OP",
    "POSTFIX_INCREMENT_OP",
    "POSTFIX_DECREMENT_OP",
    "BITWISE_COMPLEMENT_OP",
    "LOGICAL_NOT_OP",
    "SIZE_OF_TYPE_OP"};
      return std::string(names[type]);
    }

    const CodeExpressionPtr toCodeExpression(const UnaryOperatorType& type){
      const char* const names[] = {
        "&",
        "*",
        "[]",
        "->",
        ".",
        "++",
        "--",
        "++",
        "--",
        "~",
        "!",
        "sizeof"
      };
      return std::make_shared<CodeExpression>(names[type]);
    }

    enum BinaryOperatorType{EQUAL_OP, UNEQUAL_OP, LESS_THEN_OP,LESS_THEN_EQUAL_OP,GREATER_THEN_OP,GREATER_THEN_EQUAL_OP, PLUS_OP, MINUS_OP, MULTIPLY_OP, DIVISION_OP, MODULO_OP, LOGICAL_AND_OP, LOGICAL_OR_OP, BITWISE_AND_OP, BITWISE_OR_OP, BITWISE_XOR_OP, BITWISE_LEFT_SHIFT_OP, BITWISE_RIGHT_SHIFT_OP, ASSIGNMENT_OP};

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


    enum BracketMode{NO_BRACKETS,BRACKETS};

    class UnaryOperatorStatement
      : public ExpressionStatment{
    public:



      UnaryOperatorStatement(const ExpressionStatment& expr,
                              const UnaryOperatorType& op,
                              BracketMode bracket_mode = NO_BRACKETS)
        : expr_(expr.copy()), op_(op), bracket_mode_(bracket_mode)
      {
      }

       virtual StatementType getStamentType() const{
        return UNARY_OP_STMT;
       }
       virtual const CodeExpressionPtr getCode() const{
          CodeExpressionPtr code;
          code = combine(expr_->getCode(),toCodeExpression(op_));
          std::string ret;
          if(bracket_mode_==BRACKETS){
           ret=std::string("(")+code->code_+std::string(")");
            }else{
              ret=code->code_;
            }
          return std::make_shared<CodeExpression>(ret);
       }

      virtual const ExpressionStatmentPtr copy() const{
        return std::make_shared<UnaryOperatorStatement>(*this);
      }

       virtual ~UnaryOperatorStatement();
    private:
      ExpressionStatmentPtr expr_;
      UnaryOperatorType op_;
      BracketMode bracket_mode_;
    };

    UnaryOperatorStatement::~UnaryOperatorStatement(){

    }

    class BinaryOperatorStatement
      : public ExpressionStatment{
    public:



      BinaryOperatorStatement(const ExpressionStatment& lhs,
                              const BinaryOperatorType& op,
                              const ExpressionStatment& rhs,
                              BracketMode bracket_mode = NO_BRACKETS)
        : lhs_(lhs.copy()), rhs_(rhs.copy()), op_(op), bracket_mode_(bracket_mode)
      {
      }

      BinaryOperatorStatement addRight(const BinaryOperatorType& op, const VarRefStatement& rhs, BracketMode bracket_mode = NO_BRACKETS)
      {
        return BinaryOperatorStatement(*this, op, rhs, bracket_mode);
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
          code = combine(lhs_->getCode(),toCodeExpression(op_));
          code = combine(code,rhs_->getCode());
          std::string ret;
          if(bracket_mode_==BRACKETS){
           ret=std::string("(")+code->code_+std::string(")");
            }else{
              ret=code->code_;
            }
          return std::make_shared<CodeExpression>(ret);
       }

      virtual const ExpressionStatmentPtr copy() const{
        return std::make_shared<BinaryOperatorStatement>(*this);
      }

       virtual ~BinaryOperatorStatement();
    private:
      ExpressionStatmentPtr lhs_;
      ExpressionStatmentPtr rhs_;
      BinaryOperatorType op_;
      BracketMode bracket_mode_;
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
         stmt << "return " << var_ref_.getCode()->code_ << ";";
         return std::make_shared<CodeExpression>(stmt.str());
      }
      virtual ~ReturnStatement(){}
    };

    class IfStatement
      : public Statement{
    public:
      IfStatement(const Statement& cond_expr, const Statement& cond_true_stmt)
        : cond_expr_(cond_expr), cond_true_stmt_(cond_true_stmt){

      }

           virtual StatementType getStamentType() const{
              return IF_STMT;
           }
           virtual const CodeExpressionPtr getCode() const{
                std::stringstream code;
                code << "if(" << cond_expr_.getCode()->code_ << "){" << std::endl;
                code << cond_true_stmt_.getCode()->code_ << std::endl;
                code << "}" << std::endl;
                return std::make_shared<CodeExpression>(code.str());
           }
           virtual ~IfStatement();
    private:
      const Statement& cond_expr_;
      const Statement& cond_true_stmt_;
    };

    IfStatement::~IfStatement(){

    }

    class IfElseStatement : public Statement{
    public:
      IfElseStatement(const Statement& cond_true, const Statement& cond_false);

      virtual StatementType getStamentType() const{
        return IF_ELSE_STMT;
      }
      virtual const CodeExpressionPtr getCode() const{

      }
      virtual ~IfElseStatement();

    };

    class ForLoopStatement : public Statement{
public:

      ForLoopStatement(const VariableDeclaration& var_decl,
                       const ExpressionStatment& condition,
                       const ExpressionStatment& advance,
                       const std::vector<StatementPtr>& loop_body = std::vector<StatementPtr>());

      virtual StatementType getStamentType() const{
        return StatementType::FOR_LOOP_STMT;
      }
      virtual const CodeExpressionPtr getCode() const{
        std::stringstream code;
        code << "for(" << var_decl_.getCode() << ";"
             << condition_->getCode()->code_ << ";"
             << advance_->getCode()->code_ << "){" << std::endl;
        for(const auto& stmt : loop_body_){
          code << stmt->getCode()->code_ << ";" << std::endl;
        }
        code << "}" << std::endl;
        return std::make_shared<CodeExpression>(code.str());
      }

      void addStatement(StatementPtr stmt){
        if(stmt)
          loop_body_.push_back(stmt);
      }

      virtual ~ForLoopStatement();
private:
      VariableDeclaration var_decl_;
      ExpressionStatmentPtr condition_;
      ExpressionStatmentPtr advance_;
      std::vector<StatementPtr> loop_body_;
    };

    ForLoopStatement::ForLoopStatement(const VariableDeclaration& var_decl,
                                       const ExpressionStatment& condition,
                                       const ExpressionStatment& advance,
                                       const std::vector<StatementPtr>& loop_body)
      : var_decl_(var_decl), condition_(condition.copy()), advance_(advance.copy()),
        loop_body_(loop_body){

    }

    ForLoopStatement::~ForLoopStatement(){

    }

    class FunctionCallStatement : public Statement{
      virtual StatementType getStamentType() const{

      }
      virtual const CodeExpressionPtr getCode() const{

      }
      virtual ~FunctionCallStatement();
    };

    class UserDefinedDataType : public DataType{
      public:
      UserDefinedDataType(const StructDeclaration& decl)
        : decl_(decl){
      }

      ValueTypePtr getDefaultInitValue() const{
           return ValueTypePtr();
         }

         ValueTypePtr getNullValue() const{
           return ValueTypePtr();
         }
         uint32_t getSizeBytes() const{
           /* assume a 64 bit architecture, each pointer is 8 bytes */
           return decl_.getTypeSizeInBytes();
         }
         const std::string toString() const{
           return std::string("STRUCT ")+decl_.getTypeName();
         }
         const CodeExpressionPtr getTypeDefinitionCode() const{
           return std::make_shared<CodeExpression>(decl_.getCode());
         }
         const CodeExpressionPtr getCode() const{
           return std::make_shared<CodeExpression>(decl_.getTypeName());
         }
      ~UserDefinedDataType();
    private:
      StructDeclaration decl_;
    };
    UserDefinedDataType::~UserDefinedDataType(){

    }

    const DataTypePtr createUserDefinedType(const StructDeclaration& decl);

    const DataTypePtr createUserDefinedType(const StructDeclaration& decl){
      return std::make_shared<UserDefinedDataType>(decl);
    }



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
            .addRight(MULTIPLY_OP,VarRefStatement(var_decl_i), BRACKETS)
            .addRight(GREATER_THEN_OP,VarRefStatement(var_decl_l)).getCode();

        std::cout << code->code_ << std::endl;

        std::cout << IfStatement(BinaryOperatorStatement(
                      VarRefStatement(var_decl_i),GREATER_THEN_OP,VarRefStatement(var_decl_j)),
                    ReturnStatement(VarRefStatement(var_decl_i))).getCode()->code_ << std::endl;


        std::cout << IfStatement(VarRefStatement(var_decl_j),
                    VarRefStatement(var_decl_i)).getCode()->code_ << std::endl;
        }

        {
          std::cout <<
          BinaryOperatorStatement(
                VarRefStatement(var_decl_k),
                ASSIGNMENT_OP,
                BinaryOperatorStatement(
                  VarRefStatement(var_decl_j),
                  GREATER_THEN_OP,
                  VarRefStatement(var_decl_i))).getCode()->code_ << std::endl;

        }

        {

          VariableDeclaration var_decl_q = VariableDeclaration::create(createDataType(BasicType(INT32)),"q",createBasicTypeValue(BasicType(INT32),"0"));
          VariableDeclaration var_decl_num_tup = VariableDeclaration::create(createDataType(BasicType(INT32)),"num_tuples",createBasicTypeValue(BasicType(INT32),"0"));

          VariableDeclaration var_decl_sum = VariableDeclaration::create(createDataType(BasicType(INT32)),"sum",createBasicTypeValue(BasicType(INT32),"0"));

          ForLoopStatement loop_stmt(var_decl_q,
                           BinaryOperatorStatement(
                             VarRefStatement(var_decl_q),
                             LESS_THEN_OP,
                             VarRefStatement(var_decl_num_tup)),
                           UnaryOperatorStatement(VarRefStatement(var_decl_q), PREFIX_INCREMENT_OP));

          loop_stmt.addStatement(BinaryOperatorStatement(
                                   VarRefStatement(var_decl_sum),
                                   ASSIGNMENT_OP,
                                   BinaryOperatorStatement(
                                     VarRefStatement(var_decl_sum),
                                     PLUS_OP,
                                     VarRefStatement(var_decl_q))).copy());

          std::cout << loop_stmt.getCode()->code_ << std::endl;


          std::cout <<
                       ForLoopStatement(var_decl_q,
                                        BinaryOperatorStatement(
                                          VarRefStatement(var_decl_q),
                                          LESS_THEN_OP,
                                          VarRefStatement(var_decl_num_tup)),
                                        UnaryOperatorStatement(VarRefStatement(var_decl_q), PREFIX_INCREMENT_OP)).getCode()->code_ << std::endl;

          std::cout << BinaryOperatorStatement(
                VarRefStatement(var_decl_k),
                ASSIGNMENT_OP,
                BinaryOperatorStatement(
                  VarRefStatement(var_decl_j),
                  GREATER_THEN_OP,
                  ConstantExprStatement(INT32,"5"))).getCode()->code_ << std::endl;

        }
        /* pointers */
        {

            DataTypePtr val = createPointerDataType(BasicType(INT32));
            assert(val!=nullptr);
            VariableDeclaration var_decl_i = VariableDeclaration::create(createDataType(BasicType(INT32)),"i",createBasicTypeValue(BasicType(INT32),"0"));
            VariableDeclaration var_decl_p = VariableDeclaration::create(val,"array");

            std::cout << var_decl_i.getCode() << std::endl;
            std::cout << var_decl_p.getCode() << std::endl;


            StructDeclaration struct_decl = StructDeclaration::create("TupleBuffer")
                .addField(var_decl_i)
                .addField(var_decl_p);



            std::cout << VariableDeclaration::create(createUserDefinedType(struct_decl), "buffer").getCode() << std::endl;
            std::cout << VariableDeclaration::create(createPointerDataType(createUserDefinedType(struct_decl)), "buffer").getCode() << std::endl;
            std::cout << createPointerDataType(createUserDefinedType(struct_decl))->getCode()->code_ << std::endl;
            std::cout << VariableDeclaration::create(createPointerDataType(createUserDefinedType(struct_decl)), "buffer").getTypeDefinitionCode() << std::endl;


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
