#ifndef NES_NES_COMPILER_INCLUDE_COMPILER_CLANGCOMPILER_NESJIT_HPP_
#define NES_NES_COMPILER_INCLUDE_COMPILER_CLANGCOMPILER_NESJIT_HPP_

#include <bits/unique_ptr.h>

#include <sstream>
#include <llvm/InitializePasses.h>
#include <llvm/ExecutionEngine/ExecutionEngine.h>
#include <llvm/ExecutionEngine/MCJIT.h>
#include <llvm/ExecutionEngine/SectionMemoryManager.h>
#include <llvm/IR/DataLayout.h>
#include <llvm/IR/LLVMContext.h>
#include <llvm/IR/PassManager.h>
#include <llvm/Passes/PassBuilder.h>
#include <llvm/Support/MemoryBuffer.h>
#include <llvm/Support/TargetSelect.h>
#include <llvm/Support/TargetRegistry.h>
#include <llvm/Support/Host.h>
#include <llvm/Support/raw_ostream.h>

#include "llvm/ExecutionEngine/JITSymbol.h"
#include "llvm/ExecutionEngine/Orc/CompileUtils.h"
#include "llvm/ExecutionEngine/Orc/Core.h"
#include "llvm/ExecutionEngine/Orc/ExecutionUtils.h"
#include "llvm/ExecutionEngine/Orc/IRCompileLayer.h"
#include "llvm/ExecutionEngine/Orc/JITTargetMachineBuilder.h"
#include "llvm/ExecutionEngine/Orc/RTDyldObjectLinkingLayer.h"

//added for #include approach
#include "llvm/Demangle/Demangle.h"
#include "llvm/ExecutionEngine/Orc/LLJIT.h"

#include <clang/Basic/DiagnosticOptions.h>
#include <clang/Basic/Diagnostic.h>
#include <clang/Basic/FileManager.h>
#include <clang/Basic/FileSystemOptions.h>
#include <clang/Basic/LangOptions.h>
#include <clang/Basic/SourceManager.h>
#include <clang/Basic/TargetInfo.h>
#include <clang/CodeGen/CodeGenAction.h>
#include <clang/Frontend/CompilerInstance.h>
#include <clang/Frontend/CompilerInvocation.h>
#include <clang/Frontend/TextDiagnosticPrinter.h>
#include <clang/Lex/HeaderSearch.h>
#include <clang/Lex/HeaderSearchOptions.h>
#include <clang/Lex/Preprocessor.h>
#include <clang/Lex/PreprocessorOptions.h>
#include <clang/Parse/ParseAST.h>
#include <clang/Sema/Sema.h>
#include <clang/AST/ASTContext.h>
#include <clang/AST/ASTConsumer.h>

class NESJIT // :public
{
  private:
    clang::IntrusiveRefCntPtr<clang::DiagnosticOptions> diagnosticOptions;
    clang::IntrusiveRefCntPtr<clang::DiagnosticsEngine> diagnosticsEngine;
    std::unique_ptr<clang::TextDiagnosticPrinter> textDiagnosticPrinter;
    clang::CompilerInstance compilerInstance;
    clang::IntrusiveRefCntPtr<clang::DiagnosticIDs> diagIDs;

    std::unique_ptr<llvm::orc::LLJIT> lljit;
    std::unique_ptr<clang::CodeGenAction> action;
    std::unique_ptr<llvm::Module> module;
    std::unique_ptr<llvm::orc::ThreadSafeContext> tsContext;

    typedef std::unordered_map<std::string, std::string> FunctionNameMap;
    FunctionNameMap functionNames;
    std::string demangledFunctionName;

    void optimizationPass(uint8_t optimizationType, clang::CompilerInvocation compilerInvocation);
    void addCppStdLibHeaderSearchPaths(clang::HeaderSearchOptions& headerSearchOptions);
    void loadModule(const std::string& functionName, uint8_t optimizationType);

  public:
    NESJIT(bool debug = false);
    // TODO implement destructor
    ~NESJIT();

    void loadModuleFromFile(const std::string& file, const std::string& functionName, uint8_t optimizationType);
    void loadModuleFromBuffer(const std::string& buffer, const std::string& functionName, uint8_t optimizationType);
    llvm::JITTargetAddress lookup();
};

#endif//NES_NES_COMPILER_INCLUDE_COMPILER_CLANGCOMPILER_NESJIT_HPP_
