#ifndef C_CODE_COMPILER_HPP
#define C_CODE_COMPILER_HPP



#include <clang/Basic/LLVM.h>

namespace clang {
  class CompilerInstance;
}

namespace iotdb {
  class Pipeline;
  typedef boost::shared_ptr<Pipeline> PipelinePtr;

  class SharedLibrary;
  typedef boost::shared_ptr<SharedLibrary> SharedLibraryPtr;

  class CompiledCCode {
   public:
    virtual ~CompiledCCode() {}

    template <typename Function>
    Function getFunctionPointer(const std::string& name) {
      // INFO
      // http://www.trilithium.com/johan/2004/12/problem-with-dlsym/
      // No real solution in 2016.
      static_assert(sizeof(void*) == sizeof(Function),
                    "Void pointer to function pointer conversion will not work!"
                    " If you encounter this, run!");

      union converter {
        void* v_ptr;
        Function f_ptr;
      };

      converter conv;
      conv.v_ptr = getFunctionPointerImpl(name);

      return conv.f_ptr;
    }

    double getCompileTimeInSeconds() const {
      return compile_time_in_ns_ / double(1e9);
    }

   protected:
    CompiledCCode(Timestamp compile_time) : compile_time_in_ns_(compile_time) {}

    virtual void* getFunctionPointerImpl(const std::string& name) = 0;

   private:
    Timestamp compile_time_in_ns_;
  };

  typedef boost::shared_ptr<CompiledCCode> CompiledCCodePtr;

  class CCodeCompiler {
   public:
    CCodeCompiler();

    CompiledCCodePtr compile(const std::string& source);

   private:
    void init();
    void initCompilerArgs();

    Timestamp createPrecompiledHeader();
    bool rebuildPrecompiledHeader();

    std::vector<std::string> getPrecompiledHeaderCompilerArgs();
    std::vector<std::string> getCompilerArgs();

    CompiledCCodePtr compileWithSystemCompiler(const std::string& source,
                                               const Timestamp pch_time);

    void callSystemCompiler(const std::vector<std::string>& args);

    CompiledCCodePtr compileWithJITCompiler(const std::string& source,
                                            const Timestamp pch_time);

    void initLLVM();

    void prepareClangCompiler(const std::string& source,
                              const std::vector<const char*>& args,
                              clang::CompilerInstance& compiler);

    std::pair<boost::shared_ptr<llvm::LLVMContext>,
              boost::shared_ptr<llvm::ExecutionEngine>>
    createLLVMContextAndEngine(clang::CompilerInstance& compiler);

    std::vector<const char*> convertStringToCharPtrVec(
        const std::vector<std::string>& data);

    void handleDebugging(const std::string& source);

    bool use_clang_jit_ = false;
    bool show_generated_code_ = false;
    bool debug_code_generator_ = false;
    bool keep_last_generated_query_code_ = false;
    std::vector<std::string> compiler_args_;

    const static std::string IncludePath;
    const static std::string MinimalApiHeaderPath;
    std::string PrecompiledHeaderName;
  };

  void exportSourceToFile(const std::string& filename,
                          const std::string& source);
  void pretty_print_code(const std::string& source);

}

#endif  // C_CODE_COMPILER_HPP
