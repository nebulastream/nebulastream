
#include <string>
#include <vector>


#include <Core/DataTypes.hpp>
#include <CodeGen/C_CodeGen/CodeCompiler.hpp>
#include <CodeGen/PipelineStage.hpp>

namespace iotdb {

    class Statement{

    };


    class Declaration{

    };

    class StructDeclaration : public Declaration{

    };

    class VariableDeclaration : public Declaration{
    public:
        static VariableDeclaration create(DataTypePtr type, const std::string& identifier);
    };

    VariableDeclaration VariableDeclaration::create(DataTypePtr type, const std::string& identifier){
      return VariableDeclaration();
    }

    class FunctionDeclaration : public Declaration{

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
    public:
      static FileBuilder create(const std::string& function_name);
      FileBuilder& addDeclaration(const Declaration&);
      CodeFile build();
    };

    FileBuilder FileBuilder::create(const std::string& function_name){
      return FileBuilder();
    }
    FileBuilder& FileBuilder::addDeclaration(const Declaration&){
      return *this;
    }
    CodeFile FileBuilder::build(){
      return CodeFile();
    }


    int CodeGenTest(){

        FunctionDeclaration main_function = FunctionBuilder::create("compiled_query")
            .returns(createDataType(BasicType(INT32)))
            .addParameter(VariableDeclaration::create(
                            createDataType(BasicType(INT32)),
                            "thread_id")).build();
        CodeFile file = FileBuilder::create("query.cpp")
            .addDeclaration(main_function)
            .build();

        PipelineStagePtr stage = compile(file);

      return 0;

    }

}
