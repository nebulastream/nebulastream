#ifndef NES_INCLUDE_QUERYCOMPILER_COMPILER_SYSTEMCOMPILERCOMPILEDCODE_HPP_
#define NES_INCLUDE_QUERYCOMPILER_COMPILER_SYSTEMCOMPILERCOMPILEDCODE_HPP_
#include <QueryCompiler/Compiler/CompiledCode.hpp>
#include <Util/TimeMeasurement.hpp>

namespace NES {

class SharedLibrary;
typedef std::shared_ptr<SharedLibrary> SharedLibraryPtr;

class SystemCompilerCompiledCode : public CompiledCode {
  public:
    SystemCompilerCompiledCode(SharedLibraryPtr library,
                               const std::string& baseName);

    ~SystemCompilerCompiledCode() override;

  protected:
    void* getFunctionPointerImpl(const std::string& name) final;

  private:
    void cleanUp();

    SharedLibraryPtr library;
    std::string baseFileName;
};

CompiledCodePtr createSystemCompilerCompiledCode(SharedLibraryPtr library,
                                                 const std::string& baseName);

}// namespace NES

#endif//NES_INCLUDE_QUERYCOMPILER_COMPILER_SYSTEMCOMPILERCOMPILEDCODE_HPP_
