
#include <string>

#include <QueryCompiler/CCodeGenerator/CodeCompiler.hpp>
#include <QueryCompiler/CCodeGenerator/Declaration.hpp>
#include <QueryCompiler/CCodeGenerator/FileBuilder.hpp>
#include <QueryCompiler/PipelineStage.hpp>

namespace NES {

PipelineStagePtr compile(const CodeFile &file) {
  CCodeCompiler compiler;
  CompiledCCodePtr compiled_code = compiler.compile(file.code);
  return createPipelineStage(compiled_code);
}

FileBuilder FileBuilder::create(const std::string &file_name) {
  FileBuilder builder;
  builder.declations << "#include <cstdint>" << std::endl;
  builder.declations << "#include <string.h>" << std::endl;
  builder.declations << "#include <QueryLib/WindowManagerLib.hpp>" << std::endl;
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

} // namespace NES
