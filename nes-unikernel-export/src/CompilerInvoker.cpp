//
// Created by ls on 22.09.23.
//

#define BOOST_PROCESS_NO_DEPRECATED 1
#include "CompilerInvoker.h"
#include <boost/asio.hpp>
#include <boost/outcome.hpp>
#include <boost/process.hpp>

Result CompilerInvoker::compileToObject(const std::string& sourceCode, const std::string& outputPath) const {
    namespace bp = boost::process;
    bp::ipstream out_pipe;
    bp::ipstream err_pipe;
    bp::opstream in_pipe;

    boost::asio::io_context context;

    std::error_code ec;

    std::vector<std::string> args{"-x",
                                  "c++",
                                  "-",
                                  "-DUNIKERNEL_LIB",
                                  "-DNES_COMPILE_TIME_LOG_LEVEL=1",
                                  "-c",
                                  "-std=c++20",
                                  "-o",
                                  outputPath};

    for (const auto& include : includePaths) {
        args.emplace_back("-I");
        args.push_back(include);
    }

    bp::child c(bp::search_path("clang++"), bp::args(args), (bp::std_err & bp::std_out) > out_pipe, bp::std_in < in_pipe, ec);

    in_pipe << sourceCode << std::endl;
    in_pipe.pipe().close();
    in_pipe.close();

    std::stringstream ss;
    std::string line;
    while (out_pipe && std::getline(out_pipe, line)) {
        ss << line << std::endl;
    }

    c.wait();
    auto exit_code = c.exit_code();

    if (exit_code == 0) {
        return ss.str();
    } else {
        return CompilerError{ss.str()};
    }
}

Result CompilerInvoker::compile(const std::string& sourceCode) const {
    namespace bp = boost::process;
    bp::ipstream out_pipe;
    bp::ipstream err_pipe;
    bp::opstream in_pipe;

    boost::asio::io_context context;

    std::error_code ec;

    std::vector<std::string> args{"-x",
                                  "c++",
                                  "-",
                                  "-DUNIKERNEL_LIB",
                                  "-DNES_COMPILE_TIME_LOG_LEVEL=1",
                                  "-std=c++20",
                                  "-emit-llvm",
                                  "-S",
                                  "-o",
                                  "-"};

    for (const auto& include : includePaths) {
        args.emplace_back("-I");
        args.push_back(include);
    }

    bp::child c(bp::search_path("clang++"), bp::args(args), (bp::std_err & bp::std_out) > out_pipe, bp::std_in < in_pipe, ec);

    in_pipe << sourceCode << std::endl;
    in_pipe.pipe().close();
    in_pipe.close();

    std::stringstream ss;
    std::string line;
    while (out_pipe && std::getline(out_pipe, line)) {
        ss << line << std::endl;
    }

    c.wait();
    auto exit_code = c.exit_code();

    if (exit_code == 0) {
        return ss.str();
    } else {
        return CompilerError{ss.str()};
    }
}
