
#include <memory>
#include <string>

#include <Core/DataTypes.hpp>

#include <CodeGen/C_CodeGen/CodeCompiler.hpp>
#include <CodeGen/C_CodeGen/Declaration.hpp>
#include <CodeGen/C_CodeGen/FileBuilder.hpp>
#include <CodeGen/C_CodeGen/Statement.hpp>
#include <CodeGen/CodeExpression.hpp>
#include <CodeGen/PipelineStage.hpp>
#include <Util/ErrorHandling.hpp>

namespace iotdb {

PipelineStagePtr compile(const CodeFile& file)
{
    CCodeCompiler compiler;
    CompiledCCodePtr compiled_code = compiler.compile(file.code);
    return createPipelineStage(compiled_code);
}

FileBuilder FileBuilder::create(const std::string& file_name)
{
    FileBuilder builder;
    builder.declations << "#include <cstdint>" << std::endl;
    builder.declations << "#include <string.h>" << std::endl;
    return builder;
}
FileBuilder& FileBuilder::addDeclaration(const Declaration& decl)
{
    declations << decl.getCode() << ";";
    return *this;
}
CodeFile FileBuilder::build()
{
    CodeFile file;
    file.code = declations.str();
    return file;
}

} // namespace iotdb
