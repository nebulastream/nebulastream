
#include <string>

#include <QueryCompiler/C_CodeGen/CodeCompiler.hpp>
#include <QueryCompiler/C_CodeGen/Declaration.hpp>
#include <QueryCompiler/C_CodeGen/FileBuilder.hpp>
#include <QueryCompiler/PipelineStage.hpp>
#include <Util/ErrorHandling.hpp>

namespace iotdb {

PipelineStagePtr compile(const CodeFile &file) {
  CCodeCompiler compiler;
  CompiledCCodePtr compiled_code = compiler.compile(file.code);
  return createPipelineStage(compiled_code);
}

FileBuilder FileBuilder::create(const std::string &file_name) {
  FileBuilder builder;
  builder.declations << "#include <cstdint>" << std::endl;
  builder.declations << "#include <string.h>" << std::endl;
  builder.declations << "#include \"WindowManagerLib.hpp\"" << std::endl;
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

} // namespace iotdb
