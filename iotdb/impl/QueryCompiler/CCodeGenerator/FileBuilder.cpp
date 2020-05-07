
#include <string>

#include <QueryCompiler/CCodeGenerator/Declaration.hpp>
#include <QueryCompiler/CCodeGenerator/FileBuilder.hpp>

namespace NES {

FileBuilder FileBuilder::create(const std::string& file_name) {
    FileBuilder builder;
    builder.declations << "#include <cstdint>" << std::endl;
    builder.declations << "#include <string.h>" << std::endl;
    builder.declations << "#include <QueryLib/WindowManagerLib.hpp>" << std::endl;
    builder.declations << "#include <NodeEngine/TupleBuffer.hpp>" << std::endl;
    builder.declations << "#include <QueryCompiler/PipelineExecutionContext.hpp>" << std::endl;
    return builder;
}
FileBuilder& FileBuilder::addDeclaration(const Declaration& decl) {
    declations << decl.getCode() << ";";
    return *this;
}
CodeFile FileBuilder::build() {
    CodeFile file;
    file.code = declations.str();
    return file;
}

}// namespace NES
