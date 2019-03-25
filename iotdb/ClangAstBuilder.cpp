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

#include "clang/AST/ASTConsumer.h"
#include "clang/AST/ASTContext.h"
#include "clang/AST/Decl.h"
#include "clang/AST/Expr.h"
#include "clang/AST/Stmt.h"
#include "clang/Basic/IdentifierTable.h"
#include "clang/Basic/Specifiers.h"
#include "clang/Basic/TargetInfo.h"
#include "clang/CodeGen/CodeGenAction.h"
#include "clang/Frontend/ASTConsumers.h"
#include "clang/Frontend/CompilerInstance.h"
#include "clang/Lex/Preprocessor.h"

#include "clang/Tooling/CommonOptionsParser.h"
#include "clang/Tooling/Tooling.h"
#include "llvm/Support/raw_ostream.h"

#include <llvm/Analysis/LoopPass.h>
#include <llvm/ExecutionEngine/ExecutionEngine.h>
#include <llvm/ExecutionEngine/MCJIT.h>
#include <llvm/ExecutionEngine/SectionMemoryManager.h>
#include <llvm/IR/DataLayout.h>
#include <llvm/IR/LLVMContext.h>
#include <llvm/IR/PassManager.h>
#include <llvm/InitializePasses.h>
#include <llvm/Passes/PassBuilder.h>
#include <llvm/Support/MemoryBuffer.h>
#include <llvm/Support/TargetSelect.h>
#include <llvm/Support/raw_ostream.h>

#include <iostream>

using namespace llvm;

namespace {

} // namespace

bool LLVMinit = false;

void InitializeLLVM()
{
    if (LLVMinit) {
        return;
    }
    // We have not initialized any pass managers for any device yet.
    // Run the global LLVM pass initialization functions.
    llvm::InitializeNativeTarget();
    llvm::InitializeNativeTargetAsmPrinter();
    llvm::InitializeNativeTargetAsmParser();

    auto& Registry = *llvm::PassRegistry::getPassRegistry();

    llvm::initializeCore(Registry);
    llvm::initializeScalarOpts(Registry);
    llvm::initializeVectorization(Registry);
    llvm::initializeIPO(Registry);
    llvm::initializeAnalysis(Registry);
    llvm::initializeTransformUtils(Registry);
    llvm::initializeInstCombine(Registry);
    llvm::initializeInstrumentation(Registry);
    llvm::initializeTarget(Registry);

    LLVMinit = true;
}

static llvm::cl::OptionCategory ToolingSampleCategory("Tooling Sample");

int main(int argc, const char** argv)
{

    InitializeLLVM();

    // CommonOptionsParser OptionsParser(argc, argv, ClangCheckCategory);
    // ClangTool Tool(OptionsParser.getCompilations(),
    //               OptionsParser.getSourcePathList());

    // Clang context construction
    std::unique_ptr<clang::CompilerInstance> Clang(new clang::CompilerInstance());
    Clang->createDiagnostics();
    std::shared_ptr<clang::TargetOptions> TO(new clang::TargetOptions());
    TO->Triple = sys::getProcessTriple();
    ;
    Clang->setTarget(clang::TargetInfo::CreateTargetInfo(Clang->getDiagnostics(), TO));
    Clang->createFileManager();
    Clang->createSourceManager(Clang->getFileManager());
    Clang->createPreprocessor(clang::TU_Complete);
    Clang->createASTContext();
    clang::ASTContext& Context = Clang->getASTContext();
    clang::IdentifierTable& Identifiers = Clang->getPreprocessor().getIdentifierTable();
    clang::CodeGenOptions& codeGenOptions = Clang->getCodeGenOpts();

    // AST building
    clang::TranslationUnitDecl* TopDecl = Context.getTranslationUnitDecl();

    std::vector<clang::QualType> ArgsTy;
    ArgsTy.push_back(Context.DoubleTy);
    clang::QualType FTy = Context.getFunctionType(Context.IntTy, ArrayRef<clang::QualType>(ArgsTy),
                                                  clang::FunctionProtoType::ExtProtoInfo());
    /* function decl */ {
        clang::FunctionDecl* FD = clang::FunctionDecl::Create(
            Context, TopDecl, clang::SourceLocation(), clang::SourceLocation(),
            clang::DeclarationName(&Identifiers.get("myfunction")), FTy, NULL, clang::SC_None, /*inline*/ false);
        std::vector<clang::ParmVarDecl*> NewParamInfo;
        NewParamInfo.push_back(clang::ParmVarDecl::Create(Context, FD, clang::SourceLocation(), clang::SourceLocation(),
                                                          &Identifiers.get("arg1"), Context.DoubleTy, NULL,
                                                          clang::SC_None, NULL));
        FD->setParams(ArrayRef<clang::ParmVarDecl*>(NewParamInfo));
        TopDecl->addDecl(FD);
    }

    clang::VarDecl* vardecl = clang::VarDecl::Create(Context, TopDecl, clang::SourceLocation(), clang::SourceLocation(),
                                                     &Identifiers.get("myint"), Context.IntTy, nullptr, clang::SC_None);
    TopDecl->addDecl(vardecl);

    /* function body */ {

        /* create function header */
        clang::FunctionDecl* FD = clang::FunctionDecl::Create(
            Context, TopDecl, clang::SourceLocation(), clang::SourceLocation(),
            clang::DeclarationName(&Identifiers.get("myfunction")), FTy, NULL, clang::SC_None, /*inline*/ false);
        std::vector<clang::ParmVarDecl*> NewParamInfo;
        NewParamInfo.push_back(clang::ParmVarDecl::Create(Context, FD, clang::SourceLocation(), clang::SourceLocation(),
                                                          &Identifiers.get("arg1"), Context.DoubleTy, NULL,
                                                          clang::SC_None, NULL));
        FD->setParams(ArrayRef<clang::ParmVarDecl*>(NewParamInfo));

        /* create function body */

        clang::CompoundStmt* CS = new (Context) clang::CompoundStmt(clang::SourceLocation());

        /* i = 5; */
        clang::DeclarationNameLoc declnameloc(clang::DeclarationName(&Identifiers.get("myint")));
        clang::DeclRefExpr* vardeclref =
            clang::DeclRefExpr::Create(Context, clang::NestedNameSpecifierLoc(), clang::SourceLocation(), vardecl, true,
                                       clang::SourceLocation(), Context.IntTy, clang::ExprValueKind::VK_LValue);

        clang::IntegerLiteral* intlit =
            clang::IntegerLiteral::Create(Context, llvm::APInt(32, 5, true), Context.IntTy, clang::SourceLocation());

        clang::BinaryOperator* bin_op = new (Context) clang::BinaryOperator(
            vardeclref, intlit, clang::BinaryOperator::Opcode::BO_Assign, Context.IntTy,
            clang::ExprValueKind::VK_LValue, clang::ExprObjectKind::OK_Ordinary, clang::SourceLocation(), false);

        /* return 12; */

        //    clang::Stmt *S = new (Context) clang::ReturnStmt(clang::SourceLocation(),
        //            clang::IntegerLiteral::Create(Context, APInt(32,12), Context.IntTy, clang::SourceLocation()),
        //            NULL);

        clang::IntegerLiteral* int_literal =
            clang::IntegerLiteral::Create(Context, APInt(32, 12), Context.IntTy, clang::SourceLocation());

        clang::ImplicitCastExpr* impl_cast_expr =
            clang::ImplicitCastExpr::Create(Context, Context.IntTy, clang::CastKind::CK_LValueToRValue, int_literal,
                                            nullptr, clang::ExprValueKind::VK_RValue);

        clang::Stmt* S = new (Context) clang::ReturnStmt(clang::SourceLocation(), impl_cast_expr, NULL);

        // ReturnStmt(SourceLocation RL, Expr *E, const VarDecl *NRVOCandidate)

        std::vector<clang::Stmt*> statements;
        // statements.push_back(bin_op);
        statements.push_back(S);

        llvm::ArrayRef<clang::Stmt*> array_ref_s(statements);
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

    clang::tooling::CommonOptionsParser op(argc, argv, ToolingSampleCategory);
    clang::tooling::ClangTool Tool(op.getCompilations(), op.getSourcePathList());

    std::cout << "Create Clang AST" << std::endl;
    std::shared_ptr<clang::PCHContainerOperations> PCHContainerOps = std::make_shared<clang::PCHContainerOperations>();
    std::unique_ptr<clang::ASTUnit> ast_unit =
        clang::tooling::buildASTFromCode("int myfunction(double arg1) { return 12; }", "test.cpp", PCHContainerOps);

    if (!ast_unit) {
        std::cerr << "Error" << std::endl;
        return -1;
    }

    /* ---------------- code generation for the AST starts here! -------------------------- */

    llvm::LLVMContext context;
    std::unique_ptr<clang::CodeGenAction> action = std::make_unique<clang::EmitLLVMAction>(&context);

    Clang->getTargetOpts().Triple = "x86_64-pc-linux-gnu";
    llvm::outs() << "Triple: " << Clang->getTargetOpts().Triple << "\n";

    if (!Clang->ExecuteAction(*action)) {
        llvm::outs() << "Emitting LLVM Code Failed!\n";
        return -1;
    }

    /* get the IR module from the previous action */
    std::unique_ptr<llvm::Module> module = action->takeModule();
    if (!module) {
        llvm::outs() << "Retrieving Module Failed!\n";
        return -1;
    }
    module->dump();

    /* LLVM IR optimization passes */

    Clang->getTargetOpts().Triple = llvm::sys::getDefaultTargetTriple();
    // targetOptions.Triple =

    llvm::PassBuilder passBuilder;
    // llvm::LoopAnalysisManager loopAnalysisManager(codeGenOptions.DebugPassManager);
    llvm::FunctionAnalysisManager functionAnalysisManager(true);
    llvm::CGSCCAnalysisManager cGSCCAnalysisManager(true);
    llvm::ModuleAnalysisManager moduleAnalysisManager(true);

    passBuilder.registerModuleAnalyses(moduleAnalysisManager);
    passBuilder.registerCGSCCAnalyses(cGSCCAnalysisManager);
    passBuilder.registerFunctionAnalyses(functionAnalysisManager);
    // passBuilder.registerLoopAnalyses(loopAnalysisManager);
    // passBuilder.crossRegisterProxies(loopAnalysisManager, functionAnalysisManager, cGSCCAnalysisManager,
    // moduleAnalysisManager);

    llvm::ModulePassManager modulePassManager;
    passBuilder.parsePassPipeline(modulePassManager, "");
    modulePassManager.run(*module);

    // llvm::AssemblyAnnotationWriter asm_annotator;

    /* compile IR to machine code, get function, execute function */

    llvm::EngineBuilder builder(std::move(module));
    builder.setMCJITMemoryManager(std::make_unique<llvm::SectionMemoryManager>());
    builder.setOptLevel(llvm::CodeGenOpt::Level::Aggressive);
    auto executionEngine = builder.create();

    if (!executionEngine) {
        llvm::outs() << "Error Building module!\n";
        return -1;
    }

    typedef int (*function)(double);
    function func = reinterpret_cast<function>(executionEngine->getFunctionAddress("myfunction"));

    int ret = (*func)(double(3));
    llvm::outs() << ret << "\n";

    //         llvm::ModulePassManager modulePassManager = passBuilder.
    //         buildPerModuleDefaultPipeline(llvm::PassBuilder::OptimizationLevel::O3); modulePassManager.run(*module,
    //         moduleAnalysisManager);

    return 0;
}
