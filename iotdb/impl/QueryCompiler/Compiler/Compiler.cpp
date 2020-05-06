#include <QueryCompiler/Compiler/CompiledCode.hpp>
#include <QueryCompiler/Compiler/Compiler.hpp>
#include <QueryCompiler/Compiler/SystemCompilerCompiledCode.hpp>

#include <Util/SharedLibrary.hpp>

#include <boost/filesystem/operations.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>

#include <fstream>
#include <sstream>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wstrict-aliasing"

#include <Util/Logger.hpp>

#pragma GCC diagnostic pop

namespace NES {

const std::string Compiler::IncludePath = PATH_TO_NES_SOURCE_CODE "/include/";

const std::string Compiler::MinimalApiHeaderPath = PATH_TO_NES_SOURCE_CODE "/include/QueryCompiler/"
                                                                           "MinimalApi.hpp";

class CompilerFlags {
  public:
    inline static const std::string CXX_VERSION = "-std=c++17";
    // disables trigraphs
    inline static const std::string NO_TRIGRAPHS = "-fno-trigraphs";
    //Position Independent Code means that the generated machine code is not dependent on being located at a specific address in order to work.
    inline static const std::string FPIC = "-fpic";
    inline static const std::string WERROR = "-Werror";
    inline static const std::string WPARENTHESES_EQUALITY =
        "-Wparentheses-equality";
    // Vector extensions
    inline static const std::string SSE_4_1 = "-msse4.1";
    inline static const std::string SSE_4_2 = "-msse4.2";
    inline static const std::string AVX = "-mavx";
    inline static const std::string AVX2 = "-mavx2";
};

Compiler::Compiler() {
    init();
}

CompiledCodePtr Compiler::compile(const std::string& source) {
    handleDebugging(source);
    //auto pch_time = createPrecompiledHeader();
    return compileWithSystemCompiler(source, 0);
}

void Compiler::init() {
    show_generated_code_ = true;
    debug_code_generator_ = true;
    keep_last_generated_query_code_ = false;
    initCompilerArgs();
}

void Compiler::initCompilerArgs() {
    compiler_args_ = {CompilerFlags::CXX_VERSION, CompilerFlags::NO_TRIGRAPHS,
                      CompilerFlags::FPIC, CompilerFlags::WERROR,
                      CompilerFlags::WPARENTHESES_EQUALITY,
#ifdef SSE41_FOUND
                      CompilerFlags::SSE_4_1,
#endif
#ifdef SSE42_FOUND
                      CompilerFlags::SSE_4_2,
#endif
#ifdef AVX_FOUND
                      CompilerFlags::AVX,
#endif
#ifdef AVX2_FOUND
                      CompilerFlags::AVX2,
#endif
                      "-I" + IncludePath};

#ifndef NDEBUG
    compiler_args_.push_back("-g");
#else
    compiler_args_.push_back("-O3");
    compiler_args_.push_back("-g");
#endif
}

Timestamp Compiler::createPrecompiledHeader() {
    if (!rebuildPrecompiledHeader()) {
        return 0;
    }

    auto start = getTimestamp();
    callSystemCompiler(getPrecompiledHeaderCompilerArgs());
    return getTimestamp() - start;
}

bool Compiler::rebuildPrecompiledHeader() {
    if (!boost::filesystem::exists(PrecompiledHeaderName)) {
        return true;
    } else {
        auto last_access_pch = boost::filesystem::last_write_time(
            PrecompiledHeaderName);
        auto last_access_header = boost::filesystem::last_write_time(
            MinimalApiHeaderPath);

        /* pre-compiled header outdated? */
        return last_access_header > last_access_pch;
    }
}

std::vector<std::string> Compiler::getPrecompiledHeaderCompilerArgs() {
    auto args = compiler_args_;

    std::stringstream pch_option;
    pch_option << "-o" << PrecompiledHeaderName;
    args.push_back(MinimalApiHeaderPath);
    args.push_back(pch_option.str());
    args.push_back("-xc++-header");

    return args;
}

std::vector<std::string> Compiler::getCompilerArgs() {
    auto args = compiler_args_;

    args.push_back("-xc++");
#ifndef NDEBUG
    //args.push_back("-include.debug.hpp");
#else
    //args.push_back("-include.release.hpp");
#endif

#ifdef __APPLE__
    args.push_back("-framework OpenCL");
    args.push_back("-undefined dynamic_lookup");
#endif

    return args;
}

void Compiler::callSystemCompiler(const std::vector<std::string>& args) {
    std::stringstream compiler_call;
    compiler_call << CLANG_EXECUTABLE << " ";

    for (const auto& arg : args) {
        compiler_call << arg << " ";
    }
    NES_DEBUG("Compiler: compile with: '" << compiler_call.str() << "'");
    auto ret = system(compiler_call.str().c_str());

    if (ret != 0) {
        NES_ERROR("Compiler: compilation failed!");
        throw "Compilation failed!";
    }
}

void pretty_print_code(const std::string& source) {
    int ret = system("which clang-format > /dev/null");
    if (ret != 0) {
        NES_ERROR("Compiler: Did not find external tool 'clang-format'. "
                  "Please install 'clang-format' and try again."
                  "If 'clang-format-X' is installed, try to create a "
                  "symbolic link.");
        return;
    }
    const std::string filename = "temporary_file.c";

    exportSourceToFile(filename, source);

    std::string format_command = std::string("clang-format ") + filename;
    /* try a syntax highlighted output first */
    /* command highlight available? */
    ret = system("which highlight > /dev/null");
    if (ret == 0) {
        format_command += " | highlight --src-lang=c -O ansi";
    }
    ret = system(format_command.c_str());
    std::string cleanup_command = std::string("rm ") + filename;
    ret = system(cleanup_command.c_str());
}

void Compiler::handleDebugging(const std::string& source) {
    if (!show_generated_code_ && !debug_code_generator_
        && !keep_last_generated_query_code_) {
        return;
    }

    if (keep_last_generated_query_code_ || debug_code_generator_) {
        exportSourceToFile("last_generated_query.c", source);
    }

    if (show_generated_code_ || debug_code_generator_) {
        std::cout << std::string(80, '=') << std::endl;
        std::cout << "<<< Generated Host Code:" << std::endl;
        pretty_print_code(source);
        std::cout << ">>> Generated Host Code" << std::endl;
        std::cout << std::string(80, '=') << std::endl;
    }
}

void exportSourceToFile(const std::string& filename,
                        const std::string& source) {
    std::ofstream result_file(filename, std::ios::trunc | std::ios::out);
    result_file << source;
}

CompiledCodePtr Compiler::compileWithSystemCompiler(
    const std::string& source, const Timestamp pch_time) {
    auto start = getTimestamp();

    boost::uuids::uuid uuid = boost::uuids::random_generator()();
    std::string basename = "gen_query_" + boost::uuids::to_string(uuid);
    std::string filename = basename + ".c";
    std::string library_name = basename + ".so";
    exportSourceToFile(filename, source);

    auto args = getCompilerArgs();
    args.push_back("--shared");
    args.push_back("-o" + library_name);
    args.push_back(filename);

    callSystemCompiler(args);

    auto shared_library = SharedLibrary::load("./" + library_name);

    auto end = getTimestamp();
    auto compile_time = end - start + pch_time;
    NES_DEBUG("Compiler: compile Time with system compiler: " + std::to_string(compile_time) + "ms");
    return createSystemCompilerCompiledCode(shared_library, basename);
}

}// namespace NES
