#include <Compiler/Util/ClangFormat.hpp>
#include <Util/Logger.hpp>
namespace NES::Compiler {

ClangFormat::ClangFormat(Language language): language(language) {}

void ClangFormat::formatFile(std::shared_ptr<File> file) {
    int ret = system("which clang-format > /dev/null");

    NES_ASSERT(ret == 0,
               "Compiler: Did not find external tool 'clang-format'. "
               "Please install 'clang-format' and try again."
               "If 'clang-format-X' is installed, try to create a "
               "symbolic link.");

    auto formatCommand = "clang-format -i " + file->getPath();

    auto* res = popen(formatCommand.c_str(), "r");
    NES_ASSERT(res != nullptr, "Compiler: popen() failed!");
    // wait till command is complete executed.
    pclose(res);
}

}