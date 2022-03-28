//===- Attribute.cpp ------------------------------------------------------===//
//
// Part of the LLVM Project, under the Apache License v2.0 with LLVM Exceptions.
// See https://llvm.org/LICENSE.txt for license information.
// SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception
//
//===----------------------------------------------------------------------===//
//
// Example clang plugin which adds an an annotation to file-scope declarations
// with the 'example' attribute.
//
//===----------------------------------------------------------------------===//
#include "iostream"
#include "sstream"
#include "clang/AST/AST.h"
#include "clang/AST/ASTConsumer.h"
#include "clang/AST/ASTContext.h"
#include "clang/AST/Attr.h"
#include "clang/AST/DeclCXX.h"
#include "clang/AST/Mangle.h"
#include "clang/AST/RecursiveASTVisitor.h"
#include "clang/Basic/Specifiers.h"
#include "clang/Basic/Visibility.h"
#include "clang/Frontend/CompilerInstance.h"
#include "clang/Frontend/FrontendAction.h"
#include "clang/Frontend/FrontendPluginRegistry.h"
#include "clang/Lex/LexDiagnostic.h"
#include "clang/Lex/Preprocessor.h"
#include "clang/Sema/ParsedAttr.h"
#include "clang/Sema/Sema.h"
#include "clang/Sema/SemaDiagnostic.h"
#include "llvm/IR/Attributes.h"
#include <filesystem>
#include <fstream>
#include <iostream>
using namespace clang;

namespace {

enum FunctionType { Member, Global };
enum ValueType {
    INT_64,
};
struct Parameter {
    std::string name;
    std::string type;
    std::string typeQualifiedName;
};
struct ProxyFunctionInfo {
    std::string name;
    std::string namespaceName;
    std::string sourceFile;
    std::string mangledName;
    std::string returnType;
    FunctionType type;
    std::vector<Parameter> parameters;
};
static std::vector<ProxyFunctionInfo> proxyFunctions;
static std::string genPATH = "./ProxyFunctions/";
static std::string genPATH_HEADER = "./ProxyFunctions/ProxyFunctions.hpp";
static std::string genPATH_SRC = "./ProxyFunctions/ProxyFunctions.cpp";

struct ExampleAttrInfo : public ParsedAttrInfo {

    ExampleAttrInfo() {
        // Can take up to 15 optional arguments, to emulate accepting a variadic
        // number of arguments. This just illustrates how many arguments a
        // `ParsedAttrInfo` can hold, we will not use that much in this example.
        OptArgs = 15;
        // GNU-style __attribute__(("example")) and C++-style [[example]] and
        // [[plugin::example]] supported.
        static constexpr Spelling S[] = {{ParsedAttr::AS_CXX11, "proxyfunction"}, {ParsedAttr::AS_CXX11, "nes::proxyfunction"}};
        Spellings = S;
    }

    bool diagAppertainsToDecl(Sema& S, const ParsedAttr& Attr, const Decl* D) const override {
        // This attribute appertains to functions only.
        if (!isa<FunctionDecl>(D)) {
            S.Diag(Attr.getLoc(), diag::warn_attribute_wrong_decl_type_str) << Attr << "functions";
            return false;
        }
        return true;
    }

    AttrHandling handleDeclAttribute(Sema& S, Decl* D, const ParsedAttr& Attr) const override {

        if (std::filesystem::exists(genPATH_HEADER)) {
            return AttributeApplied;
        };
        // Check if the decl is at file scope.
        //if (!D->getDeclContext()->isFileContext()) {
        //    unsigned ID = S.getDiagnostics().getCustomDiagID(
        //       DiagnosticsEngine::Error,
        //      "'example' attribute only allowed at file scope");
        //  S.Diag(Attr.getLoc(), ID);
        // return AttributeNotApplied;
        //}
        ProxyFunctionInfo functionInfo;
        if (!D->getDeclContext()->isFileContext()) {
            functionInfo.type = Member;
        } else {
            functionInfo.type = Global;
        }

        if (D->isFunctionOrFunctionTemplate()) {

            auto functionDeclaration = D->getAsFunction();
            functionInfo.returnType = functionDeclaration->getReturnType().getAsString();
            auto cxxMethod = isa<CXXMethodDecl>(functionDeclaration);
            if (cxxMethod) {
                auto nsC = static_cast<CXXMethodDecl*>(functionDeclaration);
                if (nsC->getAccess() == clang::AS_private || nsC->getAccess() == clang::AS_protected) {
                    unsigned ID =
                        S.getDiagnostics().getCustomDiagID(DiagnosticsEngine::Error,
                                                           "'proxyfunctions' are not supported on private members, but is ");
                    S.Diag(Attr.getLoc(), ID);
                    return AttributeNotApplied;
                }
                // std::cout << "cxxMethod " << nsC->getParent()->getNameAsString() << std::endl;
                // std::cout << "cxxMethod getThisType " << nsC->getThisType().getAsString() << std::endl;
                // std::cout << "cxxMethod getThisObjectType " << nsC->getThisObjectType().getAsString() << std::endl;
                Parameter parameter;
                parameter.type = nsC->getParent()->getNameAsString();
                parameter.typeQualifiedName = nsC->getParent()->getQualifiedNameAsString();
                parameter.name = "thisPtr";
                functionInfo.parameters.emplace_back(parameter);
            }

            auto uncastNamespace = functionDeclaration->getEnclosingNamespaceContext();
            if (uncastNamespace->isNamespace()) {
                auto nsC = static_cast<NamespaceDecl*>(uncastNamespace);
                functionInfo.namespaceName = nsC->getQualifiedNameAsString();
            }
            auto sourceLoc = Attr.getLoc().printToString(S.getSourceManager());
            sourceLoc = sourceLoc.substr(0, sourceLoc.find(':'));
            sourceLoc = sourceLoc.substr(58, sourceLoc.size());
            functionInfo.sourceFile = sourceLoc;
            functionInfo.name = functionDeclaration->getName().str();
            auto mangleContext = clang::ItaniumMangleContext::create(S.getASTContext(), S.getDiagnostics());

            llvm::raw_string_ostream os(functionInfo.mangledName);
            mangleContext->mangleName(functionDeclaration, os);

            // get parameter
            auto numParams = functionDeclaration->getNumParams();
            for (uint64_t i = 0; i < numParams; i++) {
                Parameter parameter;
                auto param = functionDeclaration->getParamDecl(i);
                parameter.type = param->getType().getAsString();
                parameter.name = param->getNameAsString();
                functionInfo.parameters.emplace_back(parameter);
            }
            proxyFunctions.push_back(functionInfo);
        }

        return AttributeApplied;
    }

    ~ExampleAttrInfo() {

        // std::cout << "DISPOSE PLUGIN" << std::endl;
        std::ostringstream headerFile;
        std::ostringstream cppFile;
        if (proxyFunctions.empty())
            return;

        headerFile << "namespace NES::Runtime::ProxyFunctions {\n";
        cppFile << "namespace NES::Runtime::ProxyFunctions {\n";
        for (auto& pf : proxyFunctions) {
            cppFile << "#include <" << pf.sourceFile << ">\n";
            std::string namespace_idenfier = pf.namespaceName;
            std::replace(namespace_idenfier.begin(), namespace_idenfier.end(), ':', '_');// replace all 'x' to 'y'
            std::ostringstream poxyFunctionDeclaration;
            poxyFunctionDeclaration << pf.returnType << " ";
            poxyFunctionDeclaration << namespace_idenfier;
            std::ostringstream functionCall;
            functionCall << "return ";
            if (pf.type == Member) {
                poxyFunctionDeclaration << "__" << pf.parameters[0].type;
                functionCall << pf.parameters[0].name << "_"
                             << "->";

            } else {
                functionCall << pf.namespaceName << "::";
            }
            functionCall << pf.name << "(";
            poxyFunctionDeclaration << "__" << pf.name;
            poxyFunctionDeclaration << "(";
            std::ostringstream body;

            for (auto& parm : pf.parameters) {
                if (pf.type == Member && &parm == &pf.parameters.front()) {
                    poxyFunctionDeclaration << "void *"
                                            << " " << parm.name;
                    if (&parm != &pf.parameters.back()) {
                        poxyFunctionDeclaration << ",";
                    }
                    body << "auto* " << parm.name << "_ "
                         << "=(" << parm.typeQualifiedName << "*)" << parm.name << ";";
                    continue;
                } else {
                    poxyFunctionDeclaration << parm.type << " " << parm.name;
                    functionCall << parm.name;
                }

                if (&parm != &pf.parameters.back()) {
                    poxyFunctionDeclaration << ",";
                    functionCall << ",";
                }
            }
            poxyFunctionDeclaration << ")";
            std::ostringstream poxyFunctionImpl;
            poxyFunctionImpl << poxyFunctionDeclaration.str();
            poxyFunctionImpl << "{" << std::endl;
            functionCall << ");";
            body << "\n" << functionCall.str();
            poxyFunctionImpl << body.str() << "\n}\n";
            headerFile << poxyFunctionDeclaration.str() << ";\n";
            cppFile << poxyFunctionImpl.str() << ";\n";
        }
        headerFile << "}";
        cppFile << "}";
        //std::cout << headerFile.str() << std::endl;
        //std::ofstream proxyFunctionsHeader;
        //proxyFunctionsHeader.open(genPATH_HEADER);
        //proxyFunctionsHeader << headerFile.str();
        // proxyFunctionsHeader.close();

        std::ofstream proxyFunctionsCPP;
        proxyFunctionsCPP.open(genPATH_HEADER);
        proxyFunctionsCPP << cppFile.str();
        proxyFunctionsCPP.close();
    }
};

}// namespace

//static FrontendPluginRegistry::Add<PrintFunctionNamesAction> X("example", "print function names");
static ParsedAttrInfoRegistry::Add<ExampleAttrInfo> X("example", "");