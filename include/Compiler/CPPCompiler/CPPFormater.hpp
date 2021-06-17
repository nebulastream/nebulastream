#ifndef NES_INCLUDE_COMPILER_CPPCOMPILER_CPPFORMATER_HPP_
#define NES_INCLUDE_COMPILER_CPPCOMPILER_CPPFORMATER_HPP_

namespace NES::Compiler{

class CPPFormat{

  public:
    void format(){
        int ret = system("which clang-format > /dev/null");

        NES_ASSERT(ret == 0,
                   "Compiler: Did not find external tool 'clang-format'. "
                   "Please install 'clang-format' and try again."
                   "If 'clang-format-X' is installed, try to create a "
                   "symbolic link.");

        auto formatCommand = "clang-format -i " + filename;

        auto* res = popen(formatCommand.c_str(), "r");
        NES_ASSERT(res != nullptr, "Compiler: popen() failed!");
        // wait till command is complete executed.
        pclose(res);
        // read source file in
        std::ifstream file(filename);
        file.clear();
        std::string sourceCode((std::istreambuf_iterator<char>(file)), std::istreambuf_iterator<char>());
        NES_DEBUG("Compiler: generate code: \n" << sourceCode);
        return sourceCode;..
    }

};

}

#endif//NES_INCLUDE_COMPILER_CPPCOMPILER_CPPFORMATER_HPP_
