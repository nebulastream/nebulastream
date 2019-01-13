#include <CodeGen/C_CodeGen/CodeCompiler.hpp>

#include <Util/SharedLibrary.hpp>
#include <Util/ErrorHandling.hpp>

#include <boost/filesystem/operations.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>

#include <fstream>
#include <sstream>
#include <utility>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wstrict-aliasing"

#include <clang/CodeGen/CodeGenAction.h>
#include <clang/Frontend/CompilerInstance.h>
#if LLVM_VERSION >= 40
#include <clang/Lex/PreprocessorOptions.h>
#endif

#include <llvm/ExecutionEngine/ExecutionEngine.h>
#include <llvm/ExecutionEngine/MCJIT.h>
#include <llvm/ExecutionEngine/SectionMemoryManager.h>
#include <llvm/IR/LLVMContext.h>
#include <llvm/Support/MemoryBuffer.h>
#include <llvm/Support/TargetSelect.h>

#pragma GCC diagnostic pop

namespace iotdb {

const std::string CCodeCompiler::IncludePath =
    PATH_TO_IOTDB_SOURCE_CODE"/iotdb/include/";

const std::string CCodeCompiler::MinimalApiHeaderPath =
    PATH_TO_IOTDB_SOURCE_CODE
    "/include/CodeGen/"
    "MinimalAPI.h";

CCodeCompiler::CCodeCompiler() { init(); }

CompiledCCodePtr CCodeCompiler::compile(const std::string& source) {
  handleDebugging(source);
  auto pch_time = createPrecompiledHeader();

  if (use_clang_jit_) {
    return compileWithJITCompiler(source, pch_time);
  } else {
    return compileWithSystemCompiler(source, pch_time);
  }
}

void CCodeCompiler::init() {
  use_clang_jit_ = false;

  show_generated_code_ = true;

  debug_code_generator_ = true;

  keep_last_generated_query_code_ = false;

#ifndef NDEBUG
  PrecompiledHeaderName = "MinimalAPI.debug.h.pch";
#else
  PrecompiledHeaderName = "MinimalAPI.release.h.pch";
#endif
  initCompilerArgs();
}

void CCodeCompiler::initCompilerArgs() {
  compiler_args_ = {"-std=c11",        "-fno-trigraphs", "-fpic", "-Werror",
#ifdef SSE41_FOUND
                    "-msse4.1",
#endif
#ifdef SSE42_FOUND
                    "-msse4.2",
#endif
#ifdef AVX_FOUND
                    "-mavx",
#endif
#ifdef AVX2_FOUND
                    "-mavx2",
#endif
                    "-I" + IncludePath};

#ifndef NDEBUG
  compiler_args_.push_back("-g");
#else
  compiler_args_.push_back("-O3");
  compiler_args_.push_back("-g");
#endif
}

Timestamp CCodeCompiler::createPrecompiledHeader() {
  if (!rebuildPrecompiledHeader()) {
    return 0;
  }

  auto start = getTimestamp();
  callSystemCompiler(getPrecompiledHeaderCompilerArgs());
  return getTimestamp() - start;
}

bool CCodeCompiler::rebuildPrecompiledHeader() {
  if (!boost::filesystem::exists(PrecompiledHeaderName)) {
    return true;
  } else {
    auto last_access_pch =
        boost::filesystem::last_write_time(PrecompiledHeaderName);
    auto last_access_header =
        boost::filesystem::last_write_time(MinimalApiHeaderPath);

    /* pre-compiled header outdated? */
    return last_access_header > last_access_pch;
  }
}

std::vector<std::string> CCodeCompiler::getPrecompiledHeaderCompilerArgs() {
  auto args = compiler_args_;

  std::stringstream pch_option;
  pch_option << "-o" << PrecompiledHeaderName;
  args.push_back(MinimalApiHeaderPath);
  args.push_back(pch_option.str());
  args.push_back("-xc-header");

  return args;
}

std::vector<std::string> CCodeCompiler::getCompilerArgs() {
  auto args = compiler_args_;

  args.push_back("-xc");
#ifndef NDEBUG
  args.push_back("-includeMinimalAPI.debug.h");
#else
  args.push_back("-includeMinimalAPI.release.h");
#endif

#ifdef __APPLE__
  args.push_back("-framework OpenCL");
  args.push_back("-undefined dynamic_lookup");
#endif

  return args;
}

void CCodeCompiler::callSystemCompiler(const std::vector<std::string>& args) {
  std::stringstream compiler_call;
  compiler_call << CLANG_EXECUTABLE << " ";

  for (const auto& arg : args) {
    compiler_call << arg << " ";
  }
  std::cout << "system '" << compiler_call.str() << "'" << std::endl;
  auto ret = system(compiler_call.str().c_str());

  if (ret != 0) {
    IOTDB_FATAL_ERROR("PrecompiledHeader compilation failed!");
  }
}

void pretty_print_code(const std::string& source) {
  int ret = system("which clang-format > /dev/null");
  if (ret != 0) {
    IOTDB_ERROR(
        "Did not find external tool 'clang-format'. "
        "Please install 'clang-format' and try again."
        "If 'clang-format-X' is installed, try to create a "
        "symbolic link.");
    return;
  }
  const std::string filename = "temporary_file.c";

  exportSourceToFile(filename, source);

  std::string format_command = std::string("clang-format ") + filename;
  /* try a syntax highlighted output first */
  /* command highlight available? */
  ret = system("which highlight > /dev/null");
  if (ret == 0) {
    format_command += " | highlight --src-lang=c -O ansi";
  }
  ret = system(format_command.c_str());
  std::string cleanup_command = std::string("rm ") + filename;
  ret = system(cleanup_command.c_str());
}

void CCodeCompiler::handleDebugging(const std::string& source) {
  if (!show_generated_code_ && !debug_code_generator_ &&
      !keep_last_generated_query_code_) {
    return;
  }

  if (keep_last_generated_query_code_ || debug_code_generator_) {
    exportSourceToFile("last_generated_query.c", source);
  }

  if (show_generated_code_ || debug_code_generator_) {
    std::cout << std::string(80, '=') << std::endl;
    std::cout << "<<< Generated Host Code:" << std::endl;
    pretty_print_code(source);
    std::cout << ">>> Generated Host Code" << std::endl;
    std::cout << std::string(80, '=') << std::endl;
  }
}

void exportSourceToFile(const std::string& filename,
                        const std::string& source) {
  std::ofstream result_file(filename, std::ios::trunc | std::ios::out);
  result_file << source;
}

class SystemCompilerCompiledCCode : public CompiledCCode {
 public:
  SystemCompilerCompiledCCode(Timestamp compile_time, SharedLibraryPtr library,
                              const std::string& base_name)
      : CompiledCCode(compile_time),
        library_(library),
        base_file_name_(base_name) {}

  ~SystemCompilerCompiledCCode() { cleanUp(); }

 protected:
  void* getFunctionPointerImpl(const std::string& name) override final {
    return library_->getSymbol(name);
  }

 private:
  void cleanUp() {
      if (boost::filesystem::exists(base_file_name_ + ".c")) {
        boost::filesystem::remove(base_file_name_ + ".c");
      }

      if (boost::filesystem::exists(base_file_name_ + ".o")) {
        boost::filesystem::remove(base_file_name_ + ".o");
      }

      if (boost::filesystem::exists(base_file_name_ + ".so")) {
        boost::filesystem::remove(base_file_name_ + ".so");
      }

      if (boost::filesystem::exists(base_file_name_ + ".c.orig")) {
        boost::filesystem::remove(base_file_name_ + ".c.orig");
      }
  }

  SharedLibraryPtr library_;
  std::string base_file_name_;
};

CompiledCCodePtr CCodeCompiler::compileWithSystemCompiler(
    const std::string& source, const Timestamp pch_time) {
  auto start = getTimestamp();

  boost::uuids::uuid uuid = boost::uuids::random_generator()();
  std::string basename = "gen_query_" + boost::uuids::to_string(uuid);
  std::string filename = basename + ".c";
  std::string library_name = basename + ".so";
  exportSourceToFile(filename, source);

  auto args = getCompilerArgs();
  args.push_back("--shared");
  args.push_back("-o" + library_name);
  args.push_back(filename);

  callSystemCompiler(args);

  auto shared_library = SharedLibrary::load("./" + library_name);

  auto end = getTimestamp();

  auto compile_time = end - start + pch_time;
  return std::make_shared<SystemCompilerCompiledCCode>(
      compile_time, shared_library, basename);
}

class JITCompilerCompiledCCode : public CompiledCCode {
 public:
  JITCompilerCompiledCCode(
      Timestamp compile_time,
      const std::shared_ptr<llvm::LLVMContext>& context,
      const std::shared_ptr<llvm::ExecutionEngine>& engine)
      : CompiledCCode(compile_time), context_(context), engine_(engine) {}

 protected:
  void* getFunctionPointerImpl(const std::string& name) override final {
    return reinterpret_cast<void*>(engine_->getFunctionAddress(name));
  }

 private:
  std::shared_ptr<llvm::LLVMContext> context_;
  std::shared_ptr<llvm::ExecutionEngine> engine_;
};

CompiledCCodePtr CCodeCompiler::compileWithJITCompiler(
    const std::string& source, const Timestamp pch_time) {
  auto start = getTimestamp();

  initLLVM();

  auto args = getCompilerArgs();
  args.insert(args.begin(), "");
  args.push_back("string-input");
  args.push_back("-I/usr/include/linux");
  args.push_back("-I/usr/lib/clang/" QUERY_COMPILATION_CLANG_VERSION
                 "/include");

    std::stringstream compiler_call;
    compiler_call << CLANG_EXECUTABLE << " ";
    for (const auto& arg : args) {
      compiler_call << arg << " ";
    }
    std::cout << "JIT Compiler Options: " << compiler_call.str() << std::endl;

  clang::CompilerInstance compiler;
  prepareClangCompiler(source, convertStringToCharPtrVec(args), compiler);

  auto ctxt_and_engine = createLLVMContextAndEngine(compiler);

  auto end = getTimestamp();

  auto compile_time = end - start + pch_time;
  return std::make_shared<JITCompilerCompiledCCode>(
      compile_time, ctxt_and_engine.first, ctxt_and_engine.second);
}

void CCodeCompiler::initLLVM() {
  llvm::InitializeAllTargets();
  llvm::InitializeAllAsmPrinters();
  llvm::InitializeNativeTarget();
}

void CCodeCompiler::prepareClangCompiler(const std::string& source,
                                         const std::vector<const char*>& args,
                                         clang::CompilerInstance& compiler) {
  auto llvm_args = clang::ArrayRef<const char*>(args);

  std::unique_ptr<clang::CompilerInvocation> invocation(
      clang::createInvocationFromCommandLine(llvm_args));
  if (invocation.get() == nullptr) {
    IOTDB_FATAL_ERROR("Failed to create compiler invocation!");
  }

#if LLVM_VERSION == 60
  clang::CompilerInvocation::setLangDefaults(
      *invocation->getLangOpts(), clang::InputKind::CXX,
      llvm::Triple(invocation->getTargetOpts().Triple),
      invocation->getPreprocessorOpts(), clang::LangStandard::lang_c11);
#elif LLVM_VERSION >= 39
  clang::CompilerInvocation::setLangDefaults(
      *invocation->getLangOpts(), clang::IK_C,
      llvm::Triple(invocation->getTargetOpts().Triple),
      invocation->getPreprocessorOpts(), clang::LangStandard::lang_c11);
#else
  clang::CompilerInvocation::setLangDefaults(
      *invocation->getLangOpts(), clang::IK_C, clang::LangStandard::lang_c11);
#endif

  // make sure we free memory (by default it does not)
  invocation->getFrontendOpts().DisableFree = false;
  invocation->getCodeGenOpts().DisableFree = false;

// initialize the compiler
#if LLVM_VERSION >= 40
  compiler.setInvocation(
      std::shared_ptr<clang::CompilerInvocation>(invocation.release()));
#else
  compiler.setInvocation(invocation.release());
#endif
  compiler.createDiagnostics();

  clang::PreprocessorOptions& po =
      compiler.getInvocation().getPreprocessorOpts();
  // replace the string-input argument with the actual source code
  po.addRemappedFile("string-input",
                     llvm::MemoryBuffer::getMemBufferCopy(source).release());
}

std::pair<std::shared_ptr<llvm::LLVMContext>,
          std::shared_ptr<llvm::ExecutionEngine>>
CCodeCompiler::createLLVMContextAndEngine(clang::CompilerInstance& compiler) {
  auto context = std::make_shared<llvm::LLVMContext>();
  clang::EmitLLVMOnlyAction action(context.get());
  if (!compiler.ExecuteAction(action)) {
    IOTDB_FATAL_ERROR("Failed to execute action!");
  }

  std::string errStr;
  auto engine = std::shared_ptr<llvm::ExecutionEngine>(
      llvm::EngineBuilder(action.takeModule())
          .setErrorStr(&errStr)
          .setEngineKind(llvm::EngineKind::JIT)
          .setMCJITMemoryManager(std::unique_ptr<llvm::SectionMemoryManager>(
              new llvm::SectionMemoryManager()))
          .setVerifyModules(true)
          .create());

  if (!engine) {
    IOTDB_FATAL_ERROR("Could not create ExecutionEngine: " + errStr);
  }

  engine->finalizeObject();

  return std::make_pair(context, engine);
}

std::vector<const char*> CCodeCompiler::convertStringToCharPtrVec(
    const std::vector<std::string>& data) {
  std::vector<const char*> result;
  result.reserve(data.size());

  for (const auto& str : data) {
    result.push_back(str.c_str());
  }

  return result;
}

}

