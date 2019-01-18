//===--- tools/ast-builder/AstBuilder.cpp - ast-builder tool --------------===//
//
//                     The LLVM Compiler Infrastructure
//
// This file is distributed under the University of Illinois Open Source
// License. See LICENSE.TXT for details.
//
//===----------------------------------------------------------------------===//
//
//  \file
//  \brief This file implements an example of building a Clang AST manually
//
//===----------------------------------------------------------------------===//

#include "llvm/Support/Host.h"

#include "clang/AST/ASTContext.h"
#include "clang/AST/ASTConsumer.h"
#include "clang/AST/Decl.h"
#include "clang/AST/Expr.h"
#include "clang/AST/Stmt.h"
#include "clang/Basic/IdentifierTable.h"
#include "clang/Basic/Specifiers.h"
#include "clang/Basic/TargetInfo.h"
#include "clang/Frontend/ASTConsumers.h"
#include "clang/Frontend/CompilerInstance.h"
#include "clang/Lex/Preprocessor.h"

using namespace llvm;

namespace {

} // namespace

int main(int argc, const char **argv) {
  //CommonOptionsParser OptionsParser(argc, argv, ClangCheckCategory);
  //ClangTool Tool(OptionsParser.getCompilations(),
  //               OptionsParser.getSourcePathList());
  
  // Clang context construction
  std::unique_ptr<clang::CompilerInstance> Clang(new clang::CompilerInstance());
  Clang->createDiagnostics();
  std::shared_ptr<clang::TargetOptions> TO(new clang::TargetOptions());
  TO->Triple = sys::getProcessTriple();;
  Clang->setTarget(clang::TargetInfo::CreateTargetInfo(Clang->getDiagnostics(), TO));
  Clang->createFileManager();
  Clang->createSourceManager(Clang->getFileManager());
  Clang->createPreprocessor(clang::TU_Complete);
  Clang->createASTContext();
  clang::ASTContext &Context = Clang->getASTContext();
  clang::IdentifierTable &Identifiers = Clang->getPreprocessor().getIdentifierTable();
  
  // AST building
  clang::TranslationUnitDecl *TopDecl = Context.getTranslationUnitDecl();

  std::vector<clang::QualType> ArgsTy;
  ArgsTy.push_back(Context.DoubleTy);
  clang::QualType FTy = Context.getFunctionType(Context.IntTy, ArrayRef<clang::QualType>(ArgsTy),
          clang::FunctionProtoType::ExtProtoInfo());
  /* function decl */ {
    clang::FunctionDecl *FD = clang::FunctionDecl::Create(Context, TopDecl, clang::SourceLocation(), clang::SourceLocation(),
            clang::DeclarationName(&Identifiers.get("myfunction")),
            FTy, NULL, clang::SC_None, /*inline*/false);
    std::vector<clang::ParmVarDecl*> NewParamInfo;
    NewParamInfo.push_back(clang::ParmVarDecl::Create(Context, FD, clang::SourceLocation(), clang::SourceLocation(),
            &Identifiers.get("arg1"), Context.DoubleTy, NULL, clang::SC_None, NULL));
    FD->setParams(ArrayRef<clang::ParmVarDecl*>(NewParamInfo));
    TopDecl->addDecl(FD);
  }

  /* function body */ {
    clang::FunctionDecl *FD = clang::FunctionDecl::Create(Context, TopDecl, clang::SourceLocation(), clang::SourceLocation(),
            clang::DeclarationName(&Identifiers.get("myfunction")),
            FTy, NULL, clang::SC_None, /*inline*/false);
    std::vector<clang::ParmVarDecl*> NewParamInfo;
    NewParamInfo.push_back(clang::ParmVarDecl::Create(Context, FD, clang::SourceLocation(), clang::SourceLocation(),
            &Identifiers.get("arg1"), Context.DoubleTy, NULL, clang::SC_None, NULL));
    FD->setParams(ArrayRef<clang::ParmVarDecl*>(NewParamInfo));
    clang::CompoundStmt *CS = new (Context) clang::CompoundStmt(clang::SourceLocation());
    clang::Stmt *S = new (Context) clang::ReturnStmt(clang::SourceLocation(),
            clang::FloatingLiteral::Create(Context, APFloat(12.345), true, Context.DoubleTy, clang::SourceLocation()),
            NULL);
    llvm::ArrayRef<clang::Stmt*> array_ref_s(&S, 1);
    //CS->setStmts(Context, &S, 1);
    CS->setStmts(Context, array_ref_s);
    FD->setBody(CS);
    TopDecl->addDecl(FD);
  }

  // Decl printing
  llvm::outs() << "-ast-print: \n";
  Context.getTranslationUnitDecl()->print(llvm::outs());
  llvm::outs() << "\n";

  llvm::outs() << "-ast-dump: \n";
  Context.getTranslationUnitDecl()->dump(llvm::outs());
  llvm::outs() << "\n";

  return 0;
}
