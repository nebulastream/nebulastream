#include <clang/Tooling/CommonOptionsParser.h>
#include <iostream>
#include <string>
#include <vector>

#include "clang/ASTMatchers/ASTMatchFinder.h"
#include "clang/ASTMatchers/ASTMatchers.h"
#include "clang/Frontend/FrontendActions.h"
#include "clang/Tooling/CompilationDatabase.h"
#include "clang/Tooling/Tooling.h"

llvm::ExitOnError ExitOnError;
static llvm::cl::OptionCategory ProxyFunctionLocatorCategory("ProxyFunctionLocator options");

using namespace clang;
using namespace clang::ast_matchers;
using namespace clang::tooling;

struct FileWithProxyFunctions {
    std::string filePath;
    std::vector<std::string> proxyFunctions;
};

class CallFunctionMatcher : public MatchFinder::MatchCallback {
  public:
    CallFunctionMatcher() = default;

    void run(const MatchFinder::MatchResult& Result) override {
        if (const auto* IS = Result.Nodes.getNodeAs<clang::CallExpr>("call")) {
            auto arg = IS->getArg(1);
            auto proxyFunction = dyn_cast<DeclRefExpr>(arg->IgnoreImpCasts());
            if (proxyFunction) {
                std::cout << Result.Context->getSourceManager().getFilename(proxyFunction->getLocation()).str() << ": ";
                std::cout << proxyFunction->getNameInfo().getAsString() << std::endl;
            }
        }
    }
};

int main(int argc, const char** argv) {

    llvm::cl::SetVersionPrinter([](llvm::raw_ostream& OS) {
        OS << "ProxyFunctionLocator 1.0\n";
    });

    auto options = ExitOnError(clang::tooling::CommonOptionsParser::create(argc, argv, ProxyFunctionLocatorCategory));

    auto files = options.getSourcePathList();
    auto& db = options.getCompilations();

    std::vector<std::string> runtimeFiles;
    for (const auto& item : db.getAllFiles()) {
        if (item.find("nes-runtime") != std::string::npos) {
            runtimeFiles.push_back(item);
        }
    }

    // Create a Clang tool with a default syntax-only action.
    ClangTool Tool(db, runtimeFiles);

    // Set up the AST matcher.
    CallFunctionMatcher Matcher;
    MatchFinder Finder;
    Finder.addMatcher(callExpr(callee(functionDecl(hasName("FunctionCall")))).bind("call"), &Matcher);

    // Run the tool.
    return Tool.run(newFrontendActionFactory(&Finder).get());
}