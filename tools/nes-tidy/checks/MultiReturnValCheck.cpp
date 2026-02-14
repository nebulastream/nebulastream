// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at

//    https://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "MultiReturnValCheck.hpp"
#include "clang/AST/ASTContext.h"
#include "clang/AST/DeclTemplate.h"
#include "clang/AST/RecursiveASTVisitor.h"
#include "clang/ASTMatchers/ASTMatchFinder.h"
#include "clang/Lex/Lexer.h"

using namespace clang::ast_matchers;

namespace clang::tidy::nes {

namespace {

/// Returns true if an expression tree contains a call to singleReturnWrapper.
static bool isWrapperCall(const Expr *E) {
    if (!E)
        return false;
    // Strip all implicit nodes, parens, and casts.
    E = E->IgnoreUnlessSpelledInSource();
    if (const auto *CE = dyn_cast<CallExpr>(E)) {
        if (const auto *Callee = CE->getDirectCallee())
            if (Callee->getNameAsString() == "singleReturnWrapper")
                return true;
    }
    // Also check through CXXConstructExpr (implicit conversion via constructor).
    if (const auto *CCE = dyn_cast<CXXConstructExpr>(E)) {
        for (const auto *Arg : CCE->arguments())
            if (isWrapperCall(Arg))
                return true;
    }
    return false;
}

/// Visitor that counts return statements and detects SINGLE_RETURN_WRAPPER usage.
class ReturnCounter : public RecursiveASTVisitor<ReturnCounter> {
public:
    /// Number of return statements whose value is NOT a singleReturnWrapper call.
    unsigned BareCount = 0;
    /// Whether any singleReturnWrapper call exists in the body.
    bool HasWrapper = false;

    bool VisitReturnStmt(ReturnStmt *RS) {
        if (!isWrapperCall(RS->getRetValue()))
            ++BareCount;
        return true;
    }

    bool VisitCallExpr(CallExpr *CE) {
        if (auto *Callee = CE->getDirectCallee()) {
            if (Callee->getNameAsString() == "singleReturnWrapper")
                HasWrapper = true;
        }
        return true;
    }

    /// Do not descend into nested lambdas/blocks — their returns are their own.
    bool TraverseLambdaExpr(LambdaExpr *) { return true; }
    bool TraverseBlockExpr(BlockExpr *) { return true; }

    /// Returns true if the function needs wrapping: multiple bare returns,
    /// or any bare return alongside an existing SINGLE_RETURN_WRAPPER.
    bool needsWrapping() const { return BareCount > 1 || (BareCount > 0 && HasWrapper); }
};

} // namespace

bool MultiReturnValCheck::isNautilusValType(QualType Type) {
    const auto *RD = Type.getCanonicalType()->getAsCXXRecordDecl();
    if (!RD)
        return false;

    const auto *CTSD = dyn_cast<ClassTemplateSpecializationDecl>(RD);
    if (!CTSD)
        return false;

    if (auto *CTD = CTSD->getSpecializedTemplate()) {
        std::string QualName = CTD->getQualifiedNameAsString();
        return QualName == "nautilus::val";
    }
    return false;
}

bool MultiReturnValCheck::containsValType(
    QualType Type, llvm::SmallPtrSetImpl<const RecordDecl *> &Visited) {
    // Strip references and pointers.
    QualType Underlying = Type.getNonReferenceType();
    while (Underlying->isPointerType())
        Underlying = Underlying->getPointeeType();

    // Direct match.
    if (isNautilusValType(Underlying))
        return true;

    // For record types, recursively inspect fields, bases, and template args.
    const auto *RD = Underlying.getCanonicalType()->getAsCXXRecordDecl();
    if (!RD || !RD->hasDefinition())
        return false;

    RD = RD->getDefinition();

    // Check the per-TU cache first.
    auto CacheIt = ContainsValCache.find(RD);
    if (CacheIt != ContainsValCache.end())
        return CacheIt->second;

    // Avoid infinite recursion on circular types.
    if (!Visited.insert(RD).second)
        return false;

    bool Result = false;

    // Check base classes.
    for (const auto &Base : RD->bases()) {
        if (containsValType(Base.getType(), Visited)) {
            Result = true;
            break;
        }
    }

    // Check fields.
    if (!Result) {
        for (const auto *Field : RD->fields()) {
            if (containsValType(Field->getType(), Visited)) {
                Result = true;
                break;
            }
        }
    }

    // Check template arguments (covers std::variant, std::optional, etc.).
    if (!Result) {
        if (const auto *CTSD = dyn_cast<ClassTemplateSpecializationDecl>(RD)) {
            for (const auto &Arg : CTSD->getTemplateArgs().asArray()) {
                if (Arg.getKind() == TemplateArgument::Type) {
                    if (containsValType(Arg.getAsType(), Visited)) {
                        Result = true;
                        break;
                    }
                }
            }
        }
    }

    ContainsValCache[RD] = Result;
    return Result;
}

void MultiReturnValCheck::registerMatchers(MatchFinder *Finder) {
    Finder->addMatcher(functionDecl(isDefinition(), hasBody(stmt())).bind("func"), this);
}

void MultiReturnValCheck::check(const MatchFinder::MatchResult &Result) {
    const auto *Func = Result.Nodes.getNodeAs<FunctionDecl>("func");
    if (!Func || !Func->getBody())
        return;

    // Skip functions expanded from SINGLE_RETURN_WRAPPER (the inner lambda).
    SourceLocation Loc = Func->getBeginLoc();
    if (Loc.isMacroID()) {
        StringRef MacroName = Lexer::getImmediateMacroName(
            Loc, *Result.SourceManager, getLangOpts());
        if (MacroName == "SINGLE_RETURN_WRAPPER")
            return;
    }

    // Check if the return type is directly nautilus::val<T> (for auto-fix)
    // or transitively contains one (warning only).
    QualType RetType = Func->getReturnType().getNonReferenceType();
    bool IsDirectVal = isNautilusValType(RetType);

    if (!IsDirectVal) {
        llvm::SmallPtrSet<const RecordDecl *, 16> Visited;
        if (!containsValType(Func->getReturnType(), Visited))
            return;
    }

    // Count return statements (excluding nested lambdas/blocks).
    ReturnCounter Counter;
    Counter.TraverseStmt(const_cast<Stmt *>(Func->getBody()));

    if (!Counter.needsWrapping())
        return;

    auto D = diag(Func->getLocation(),
                  "function %0 returns a type containing nautilus::val and has %1 "
                  "return statements; consider single return")
             << Func << Counter.BareCount;

    // Only provide automatic fix when the return type is directly nautilus::val<T>
    // by value (not a reference), and the function is not a lambda operator().
    // Use FixedLocations to deduplicate fixes for the same source location
    // (e.g. multiple template instantiations of the same function).
    bool IsReference = Func->getReturnType()->isReferenceType();
    bool IsLambda = false;
    if (const auto *MD = dyn_cast<CXXMethodDecl>(Func))
        IsLambda = MD->getParent()->isLambda();

    if (IsDirectVal && !IsReference && !IsLambda) {
        SourceLocation BodyLoc = Func->getBody()->getBeginLoc();
        if (FixedLocations.insert(BodyLoc).second) {
            const auto *Body = cast<CompoundStmt>(Func->getBody());
            SourceLocation AfterLBrace = Body->getLBracLoc().getLocWithOffset(1);
            SourceLocation RBrace = Body->getRBracLoc();

            D << FixItHint::CreateInsertion(AfterLBrace,
                                            "\nreturn SINGLE_RETURN_WRAPPER({")
              << FixItHint::CreateInsertion(RBrace, "});\n");
        }
    }
}

} // namespace clang::tidy::nes
