
#include <CodeGen/CodeGen.hpp>
#include <CodeGen/C_CodeGen/CodeCompiler.hpp>

#include <sstream>
#include <iostream>
#include <list>

#include <clang/AST/AST.h>
#include <clang/AST/Decl.h>
#include <clang/Frontend/TextDiagnosticPrinter.h>
#include <clang/Driver/Driver.h>
#include <clang/Driver/DriverDiagnostic.h>

#include <llvm/Support/Program.h>

#include <clang/Basic/Builtins.h>
#include <clang/Driver/Compilation.h>

#include <Util/ErrorHandling.hpp>

namespace iotdb {

  class GeneratedCode {
   public:
    std::stringstream kernel_header_and_types_code_block_;
    std::stringstream header_and_types_code_block_;
    std::stringstream fetch_input_code_block_;
    std::stringstream declare_variables_code_block_;
    std::stringstream init_variables_code_block_;
    std::list<std::string> upper_code_block_;
    std::list<std::string> lower_code_block_;
    std::stringstream after_for_loop_code_block_;
    std::stringstream create_result_table_code_block_;
    std::stringstream clean_up_code_block_;
  };

  typedef std::shared_ptr<GeneratedCode> GeneratedCodePtr;

  bool addToCode(GeneratedCodePtr code, GeneratedCodePtr code_to_add);

  CodeGenerator::~CodeGenerator(){}



  class C_CodeGenerator : public CodeGenerator {
    bool addOperator(OperatorPtr, const CodeGenArgs &);
    PipelineStagePtr compile(const CompilerArgs &);
    ~C_CodeGenerator();
  };

  bool C_CodeGenerator::addOperator(OperatorPtr, const CodeGenArgs &){

    return false;
  }

  PipelineStagePtr C_CodeGenerator::compile(const CompilerArgs &){
      CCodeCompiler compiler;
      CompiledCCodePtr code = compiler.compile("");

      //code->getFunctionPointer("");
      return PipelineStagePtr();
  }

  C_CodeGenerator::~C_CodeGenerator(){

  }

  CodeGeneratorPtr createCodeGenerator(){
    return CodeGeneratorPtr();
  }

  void generate_LLVM_AST(){
    // Path to the C file
    std::string inputPath = "getinmemory.c";

    // Path to clang (e.g. /usr/local/bin/clang)
//    auto clangPath = llvm::sys::findProgramByName("clang++");
//    if(clangPath.getError()){
//        IOTDB_FATAL_ERROR("Could not find clang++ compiler: " + clangPath.getError().message());
//    }

    // Arguments to pass to the clang driver:
    //	clang getinmemory.c -lcurl -v
    std::vector<const char *> args;
    args.push_back(inputPath.c_str());
    args.push_back("-o a.exe");
    args.push_back("-v");		// verbose

                                                // The clang driver needs a DiagnosticsEngine so it can report problems
    clang::DiagnosticOptions diagnosticOptions;
    clang::TextDiagnosticPrinter *DiagClient = new clang::TextDiagnosticPrinter(llvm::errs(), &diagnosticOptions);
    if(!DiagClient)
      IOTDB_FATAL_ERROR("clang::TextDiagnosticPrinter is NULL!");
    //clang::IntrusiveRefCntPtr<clang::DiagnosticIDs> DiagID(new clang::DiagnosticIDs());

    llvm::IntrusiveRefCntPtr<clang::DiagnosticIDs> diagIds = new clang::DiagnosticIDs();
    //auto Diags = std::make_unique<clang::DiagnosticsEngine>(diagIds, &diagnosticOptions, DiagClient);
  //FIXME: The following line creates a memory leak.
  // For some odd reason the program crashes when we call the destructor, this needs further investitation
    clang::DiagnosticsEngine* Diags = new clang::DiagnosticsEngine(diagIds, &diagnosticOptions, DiagClient);



    // Create the clang driver
    //const char* const val = clangPath.get().c_str();
    //auto val2 = Diags.get();
    clang::driver::Driver TheDriver(CLANG_EXECUTABLE, llvm::sys::getDefaultTargetTriple(), *Diags);


    // Create the set of actions to perform
    auto C = std::unique_ptr<clang::driver::Compilation>(TheDriver.BuildCompilation(args));

    // Print the set of actions
    TheDriver.PrintActions(*C);

    // This is referenced by `FileMgr` and will be released by `FileMgr` when it
    // is deleted.
    clang::IntrusiveRefCntPtr<clang::vfs::InMemoryFileSystem> InMemoryFileSystem(
        new clang::vfs::InMemoryFileSystem);

    std::unique_ptr<clang::FileManager> FileMgr(
        new clang::FileManager(clang::FileSystemOptions(), InMemoryFileSystem));
    // This is passed to `SM` as reference, so the pointer has to be referenced
    // by `Environment` due to the same reason above.
//    std::unique_ptr<clang::DiagnosticsEngine> Diagnostics(new clang::DiagnosticsEngine(
//        clang::IntrusiveRefCntPtr<clang::DiagnosticIDs>(new clang::DiagnosticIDs),
//        new clang::DiagnosticOptions));
    // This will be stored as reference, so the pointer has to be stored in
    // due to the same reason above.
    std::unique_ptr<clang::SourceManager> VirtualSM(
        new clang::SourceManager(*Diags, *FileMgr));


    clang::VarDecl* vardecl;

    clang::Decl::Kind kind = clang::Decl::Kind::Var;

    (void) vardecl;
    (void) kind;

    clang::LangOptions lang_opts;

    clang::LangOptions LangOpts;
    clang::IdentifierTable idents(LangOpts);

    clang::SelectorTable sels;
    clang::Builtin::Context builtins;

    clang::ASTContext context (LangOpts, *VirtualSM, idents,
                   sels, builtins);



#ifdef NON
#endif

    std::cout << "LLVM initialized!" << std::endl;


  }


}
