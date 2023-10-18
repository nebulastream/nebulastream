//
// Created by ls on 22.09.23.
//

#ifndef COMPILEWITHCLANG_COMPILERINVOKER_H
#define COMPILEWITHCLANG_COMPILERINVOKER_H

#include <boost/outcome.hpp>
#include <string>
#include <vector>

struct CompilerError {
    std::string message;
};

using Result = boost::outcome_v2::result<std::string, CompilerError>;

class CompilerInvoker {
  public:
    explicit CompilerInvoker(std::vector<std::string> includePaths) : includePaths(std::move(includePaths)) {}

    Result compileToObject(const std::string& sourceCode, const std::string& outputPath) const;
    Result compile(const std::string& sourceCode) const;

  private:
    std::vector<std::string> includePaths;
};

#endif//COMPILEWITHCLANG_COMPILERINVOKER_H