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


    clang::VarDecl* vardecl = clang::VarDecl::Create(Context, TopDecl, clang::SourceLocation(), clang::SourceLocation(),
                                                   &Identifiers.get("myint"), Context.IntTy, nullptr, clang::SC_None);
    TopDecl->addDecl(vardecl);



  /* function body */ {

    /* create function header */
    clang::FunctionDecl *FD = clang::FunctionDecl::Create(Context, TopDecl, clang::SourceLocation(), clang::SourceLocation(),
            clang::DeclarationName(&Identifiers.get("myfunction")),
            FTy, NULL, clang::SC_None, /*inline*/false);
    std::vector<clang::ParmVarDecl*> NewParamInfo;
    NewParamInfo.push_back(clang::ParmVarDecl::Create(Context, FD, clang::SourceLocation(), clang::SourceLocation(),
            &Identifiers.get("arg1"), Context.DoubleTy, NULL, clang::SC_None, NULL));
    FD->setParams(ArrayRef<clang::ParmVarDecl*>(NewParamInfo));

    /* create function body */

    clang::CompoundStmt *CS = new (Context) clang::CompoundStmt(clang::SourceLocation());


    /* i = 5; */
    clang::DeclarationNameLoc declnameloc (clang::DeclarationName(&Identifiers.get("myint")));
    clang::DeclRefExpr* vardeclref = clang::DeclRefExpr::Create(Context,
                                                               clang::NestedNameSpecifierLoc (),
                                                               clang::SourceLocation(),
                                                               vardecl,
                                                               true,
                                                               clang::SourceLocation(),
                                                               Context.IntTy,
                                                               clang::ExprValueKind::VK_LValue);


    clang::IntegerLiteral* intlit = clang::IntegerLiteral::Create(Context,
                                                                  llvm::APInt(32,5,true),
                                                                  Context.IntTy,
                                                                  clang::SourceLocation());

    clang::BinaryOperator* bin_op = new (Context) clang::BinaryOperator(vardeclref,
                                                              intlit,
                                                              clang::BinaryOperator::Opcode::BO_Assign,
                                                              Context.IntTy,
                                                              clang::ExprValueKind::VK_LValue,
                                                              clang::ExprObjectKind::OK_Ordinary,
                                                              clang::SourceLocation(), false);

    /* return 12.345; */
    clang::Stmt *S = new (Context) clang::ReturnStmt(clang::SourceLocation(),
            clang::FloatingLiteral::Create(Context, APFloat(12.345), true, Context.DoubleTy, clang::SourceLocation()),
            NULL);

    std::vector<clang::Stmt*> statements;
    statements.push_back(bin_op);
    statements.push_back(S);

    llvm::ArrayRef<clang::Stmt*> array_ref_s(statements);
    //llvm::ArrayRef<clang::Stmt*> array_ref_s(&S, 1);
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
