#ifndef INCLUDE_CODE_COMPILER_HPP_
#define INCLUDE_CODE_COMPILER_HPP_

#include <Util/TimeMeasurement.hpp>
#include <memory>
#include <string>
#include <vector>

namespace NES {

class SharedLibrary;
typedef std::shared_ptr<SharedLibrary> SharedLibraryPtr;

class CompiledCode;
typedef std::shared_ptr<CompiledCode> CompiledCodePtr;

/**
 * @brief The Compiler compiles a source code fragment to a dynamic library.
 */
class Compiler {
  public:
    Compiler();
    /**
     * @brief Compiles the source string.
     * @param source
     * @return CompiledCode
     */
    CompiledCodePtr compile(const std::string& source);

  private:
    void init();
    void initCompilerArgs();
    /**
     * @brief Creates a precompiled header for the runtime lib.
     * TODO this is currently not used and need a rework see issue #392.
     * @return
     */
    Timestamp createPrecompiledHeader();
    bool rebuildPrecompiledHeader();

    CompiledCodePtr compileWithSystemCompiler(const std::string& source, const Timestamp pch_time);

    void callSystemCompiler(const std::vector<std::string>& args);

    void handleDebugging(const std::string& source);

    std::vector<std::string> getPrecompiledHeaderCompilerArgs();
    std::vector<std::string> getCompilerArgs();
    bool show_generated_code_ = false;
    bool debug_code_generator_ = false;
    bool keep_last_generated_query_code_ = false;
    std::vector<std::string> compiler_args_;

    const static std::string IncludePath;
    const static std::string MinimalApiHeaderPath;
    std::string PrecompiledHeaderName;
};

void exportSourceToFile(const std::string& filename, const std::string& source);
void pretty_print_code(const std::string& source);

}// namespace NES

#endif// INCLUDE_CODE_COMPILER_HPP_
