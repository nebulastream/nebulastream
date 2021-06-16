#ifndef NES_INCLUDE_COMPILER_CPPCOMPILER_CPPCOMPILERFLAGS_HPP_
#define NES_INCLUDE_COMPILER_CPPCOMPILER_CPPCOMPILERFLAGS_HPP_
#include <string>
#include <vector>
namespace NES::Compiler {

class CPPCompilerFlags {
  public:
    // sets the cpp language version for the code
    inline static const std::string CXX_VERSION = "-std=c++17";
    // disables trigraphs
    inline static const std::string NO_TRIGRAPHS = "-fno-trigraphs";
    //Position Independent Code means that the generated machine code is not dependent on being located at a specific address in order to work.
    inline static const std::string FPIC = "-fpic";
    // Turn warnings into errors.
    inline static const std::string WERROR = "-Werror";
    // warning: equality comparison with extraneous parentheses
    inline static const std::string WPARENTHESES_EQUALITY = "-Wparentheses-equality";

    //    inline static const std::string LOGGING_FATAL_FLAG = "-DNES_LOGGING_FATAL_ERROR_LEVEL=1";
    //    inline static const std::string LOGGING_FATAL_FLAG = "-DNES_LOGGING_TRACE_LEVEL=1";

    inline static const std::string ALL_OPTIMIZATIONS = "-O3";
    inline static const std::string TUNE = "-mtune=native";
    inline static const std::string ARCH = "-march=native";
    inline static const std::string DEBUGGING = "-g";

    // Vector extensions
    inline static const std::string SSE_4_1 = "-msse4.1";
    inline static const std::string SSE_4_2 = "-msse4.2";
    inline static const std::string AVX = "-mavx";
    inline static const std::string AVX2 = "-mavx2";

    static CPPCompilerFlags create();
    static CPPCompilerFlags createDefaultCompilerFlags();
    void enableOptimizationFlags();
    void enableDebugFlags();

    [[nodiscard]] std::vector<std::string> getFlags() const;
    void addFlag(const std::string& flag);

  private:
    CPPCompilerFlags() = default;
    std::vector<std::string> compilerFlags;
};

}// namespace NES::Compiler

#endif//NES_INCLUDE_COMPILER_CPPCOMPILER_CPPCOMPILERFLAGS_HPP_
