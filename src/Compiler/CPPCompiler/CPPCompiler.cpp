#include <Compiler/CPPCompiler/CPPCompiler.hpp>
#include <Compiler/CPPCompiler/CPPCompilerFlags.hpp>
#include <Compiler/CPPCompiler/SharedLibrary.hpp>
#include <Compiler/CompilationRequest.hpp>
#include <Compiler/CompilationResult.hpp>
#include <Compiler/File.hpp>
#include <Compiler/SourceCode.hpp>
#include <Compiler/Util/ClangFormat.hpp>

namespace NES::Compiler {

const std::string NESIncludePath = PATH_TO_NES_SOURCE_CODE "/include/";
const std::string DEBSIncludePath = PATH_TO_DEB_SOURCE_CODE "/include/";

std::shared_ptr<LanguageCompiler> CPPCompiler::create() { return std::make_shared<CPPCompiler>(); }

CPPCompiler::CPPCompiler() : format(std::make_unique<ClangFormat>(CPP)) {}

Language CPPCompiler::getLanguage() const { return CPP; }

std::unique_ptr<const CompilationResult> CPPCompiler::compile(std::unique_ptr<const CompilationRequest> request) const {

    auto sourceFileName = request->getName() + ".cpp";
    auto libraryFileName = request->getName() + ".so";
    auto compilationFlags = CPPCompilerFlags::createDefaultCompilerFlags();
    if (request->enableOptimizations()) {
        compilationFlags.enableOptimizationFlags();
    }
    if (request->enableDebugging()) {
        compilationFlags.enableDebugFlags();
    }

    compilationFlags.addFlag("--shared");
    // add header of NES Source
    compilationFlags.addFlag("-I" + NESIncludePath);
    // add header of all dependencies
    compilationFlags.addFlag("-I" + DEBSIncludePath);

    compilationFlags.addFlag("-o" + libraryFileName);

    auto file = File::createFile(sourceFileName, request->getSourceCode()->getCode());

    format->formatFile(file);
    file->print();
    compileSourceFile(compilationFlags, file, libraryFileName);
    // load shared lib
    auto sharedLibrary = SharedLibrary::load(libraryFileName);

    return std::make_unique<CompilationResult>();
}

void CPPCompiler::compileSourceFile(CPPCompilerFlags flags, std::shared_ptr<File> sourceFile, std::string libraryFileName) {
    std::stringstream compilerCall;
    compilerCall << CLANG_EXECUTABLE << " ";
    for (const auto& arg : flags->getFlags()) {
        compilerCall << arg << " ";
    }
    NES_DEBUG("Compiler: compile with: '" << compilerCall.str() << "'");
    // Creating a pointer to an open stream and a buffer, to read the output of the compiler
    FILE* fp = nullptr;
    char buffer[10000];

    // Redirecting stderr to stdout, to be able to read error messages
    compilerCall << " 2>&1";

    // Calling the compiler in a new process
    fp = popen(compilerCall.str().c_str(), "r");

    if (fp == nullptr) {
        NES_ERROR("Compiler: failed to run command\n");
        return;
    }

    // Collecting the output of the compiler to a string stream
    std::ostringstream strstream;
    while (fgets(buffer, sizeof(buffer), fp) != nullptr) {
        strstream << buffer;
    }

    // Closing the stream, which also gives us the exit status of the compiler call
    auto ret = pclose(fp);

    // If the compilation did't return with 0, we throw an exception containing the compiler output
    if (ret != 0) {
        NES_ERROR("Compiler: compilation of " << filename << " failed.");
        throw std::runtime_error(strstream.str());
    }
}

void CPPCompiler::formatSourceFile(std::shared_ptr<File> sourceFile) {}

}// namespace NES::Compiler