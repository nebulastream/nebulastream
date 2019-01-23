
#include <string>
#include <sstream>
#include <vector>


#include <Core/DataTypes.hpp>
#include <CodeGen/C_CodeGen/CodeCompiler.hpp>
#include <CodeGen/PipelineStage.hpp>
#include <CodeGen/CodeExpression.hpp>

namespace iotdb {

    class Statement{

    };


    typedef std::string Code;

    class Declaration{
    public:
      virtual const Code getCode() const = 0;
    };

    class StructDeclaration : public Declaration{
    public:
      const Code getCode() const override{
         return Code();
      }
    };

    class VariableDeclaration : public Declaration{
    public:
        static VariableDeclaration create(DataTypePtr type, const std::string& identifier);
        const Code getCode() const override{
          std::stringstream str;
          str << type_->getCode()->code_ << " " << identifier_ << ";";
          return str.str();
        }
    private:
        VariableDeclaration(DataTypePtr type, const std::string& identifier);
        DataTypePtr type_;
        std::string identifier_;
    };

    VariableDeclaration::VariableDeclaration(DataTypePtr type, const std::string& identifier)
      : type_(type), identifier_(identifier){}

    VariableDeclaration VariableDeclaration::create(DataTypePtr type, const std::string& identifier){
      return VariableDeclaration(type,identifier);
    }

    class FunctionDeclaration : public Declaration{
    public:
      const Code getCode() const override{
         return Code();
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
      std::vector<std::string> parameters;
      std::vector<Statement> instructions;
      FunctionBuilder();
    public:
      static FunctionBuilder create(const std::string& function_name);

      FunctionBuilder &returns(DataTypePtr returnType_);
      FunctionBuilder &addParameter(VariableDeclaration var_decl);
      FunctionBuilder &addStatement(const Statement &statement);
      FunctionDeclaration build();
    };

    FunctionBuilder::FunctionBuilder(){

    }

    FunctionBuilder FunctionBuilder::create(const std::string& function_name){
      return FunctionBuilder();
    }

    FunctionDeclaration FunctionBuilder::build(){
      return FunctionDeclaration();
    }

    FunctionBuilder &FunctionBuilder::returns(DataTypePtr returnType_){
        return *this;
    }

    FunctionBuilder &FunctionBuilder::addParameter(VariableDeclaration var_decl){
      return *this;
    }
    FunctionBuilder &FunctionBuilder::addStatement(const Statement &statement){
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
      static FileBuilder create(const std::string& function_name);
      FileBuilder& addDeclaration(const Declaration&);
      CodeFile build();
    };

    FileBuilder FileBuilder::create(const std::string& function_name){
      FileBuilder builder;
      builder.declations << "#include <cstdint>" << std::endl;
      return builder;
    }
    FileBuilder& FileBuilder::addDeclaration(const Declaration& decl){
      declations << decl.getCode();
      return *this;
    }
    CodeFile FileBuilder::build(){
      CodeFile file;
      file.code = declations.str();
      return file;
    }


    int CodeGenTest(){

        VariableDeclaration var_decl = VariableDeclaration::create(createDataType(BasicType::UINT32),"i");

        FunctionDeclaration main_function = FunctionBuilder::create("compiled_query")
            .returns(createDataType(BasicType(INT32)))
            .addParameter(VariableDeclaration::create(
                            createDataType(BasicType(INT32)),
                            "thread_id")).build();
        CodeFile file = FileBuilder::create("query.cpp")
            .addDeclaration(var_decl)
            .addDeclaration(main_function)
            .build();

        PipelineStagePtr stage = compile(file);

      return 0;

    }

}
