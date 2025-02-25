/*
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/
#include <Compiler/Exceptions/CompilerException.hpp>
#include <Compiler/Util/ExecutablePath.hpp>
#include <Util/Logger/Logger.hpp>
#include <filesystem>
#include <vector>

namespace NES::Compiler::ExecutablePath {

std::string UNIX_INSTALL_BIN_DIR = "/usr/local/bin";
std::string DEFAULT_PUBLIC_INCLUDE_DIR_UNIX_INSTALL = "/usr/local/include/nebulastream";
std::string DEFAULT_LIB_UNIX_INSTALL = "/usr/local/lib";
std::string DEFAULT_CLANG_PATH_UNIX_INSTALL = "/usr/local/bin/nes-clang";

bool isInBuildDir() {
    auto executablePath = getExecutablePath();
    return executablePath.parent_path().string().starts_with(PATH_TO_BINARY_DIR);
}

bool isInBuildDirIsAvailable() { return exists(std::filesystem::path(CLANG_EXECUTABLE)); }

bool isInUNIXInstallDir() {
    auto executablePath = getExecutablePath();
    return executablePath.parent_path().string() == UNIX_INSTALL_BIN_DIR;
}

bool isInLocalInstallDir() {
    auto executablePath = getExecutablePath().parent_path();
    return std::filesystem::exists(executablePath.append("bin")) && std::filesystem::exists(executablePath.append("lib"))
        && std::filesystem::exists(executablePath.append("include"));
}

std::ostream& operator<<(std::ostream& os, const RuntimePathConfig& config) {
    os << "\nclangBinaryPath: " << config.clangBinaryPath << "\n";
    os << "includePaths: \n";
    for (auto includeDir : config.includePaths) {
        os << "\t" << includeDir << "\n";
    }
    os << "libPaths:";
    for (auto libDirs : config.libPaths) {
        os << "\n\t" << libDirs;
    }
    return os;
}

RuntimePathConfig loadRuntimePathConfig() {
    auto runtimePathConfig = RuntimePathConfig();
#if defined(__APPLE__)
    runtimePathConfig.libs.push_back("-lnes-window-types");
    runtimePathConfig.libs.push_back("-lnes-operators");
    runtimePathConfig.libs.push_back("-lnes-runtime");
    runtimePathConfig.libs.push_back("-lnes-common");
    runtimePathConfig.libs.push_back("-lnes-client");
    runtimePathConfig.libs.push_back("-lnes-data-types");
    runtimePathConfig.libs.push_back("-lc++");
#endif

    if (isInUNIXInstallDir()) {
        NES_DEBUG("Detected a unix install dir as a execution location");
        runtimePathConfig.clangBinaryPath = DEFAULT_CLANG_PATH_UNIX_INSTALL;
        runtimePathConfig.includePaths.push_back(DEFAULT_PUBLIC_INCLUDE_DIR_UNIX_INSTALL);
        runtimePathConfig.libPaths.push_back(DEFAULT_LIB_UNIX_INSTALL);
    } else if (isInLocalInstallDir()) {
        NES_DEBUG("Detected a local install dir as a execution location");
        auto executablePath = getExecutablePath().parent_path();
        runtimePathConfig.clangBinaryPath = executablePath.append("bin/nes-clang");
        runtimePathConfig.includePaths.push_back(executablePath.append("include/nebulatstream"));
        runtimePathConfig.libPaths.push_back(executablePath.append("lib"));
    } else if (isInBuildDir() || isInBuildDirIsAvailable()) {
        NES_DEBUG("Detected a build dir as a execution location");
        const std::string commonBinaryDir = PATH_TO_BINARY_DIR "/nes-common/";
        const std::string runtimeBinaryDir = PATH_TO_BINARY_DIR "/nes-runtime/";
        const std::string clientBinaryDir = PATH_TO_BINARY_DIR "/nes-client/";
        const std::string operatorsBinaryDir = PATH_TO_BINARY_DIR "/nes-operators/";
        const std::string dataTypesBinaryDir = PATH_TO_BINARY_DIR "/nes-data-types/";
        const std::string windowTypesBinaryDir = PATH_TO_BINARY_DIR "/nes-window-types";
        runtimePathConfig.clangBinaryPath = CLANG_EXECUTABLE;
        runtimePathConfig.libPaths.push_back(windowTypesBinaryDir);
        runtimePathConfig.libPaths.push_back(clientBinaryDir);
        runtimePathConfig.libPaths.push_back(runtimeBinaryDir);
        runtimePathConfig.libPaths.push_back(commonBinaryDir);
        runtimePathConfig.libPaths.push_back(dataTypesBinaryDir);
        runtimePathConfig.libPaths.push_back(operatorsBinaryDir);
        runtimePathConfig.includePaths.push_back(PATH_TO_BINARY_DIR "/include/nebulastream");
    } else {
        throw CompilerException("Runtime environment can't be detected.");
    }

    // verify is runtime path config is valid
    if (!std::filesystem::exists(runtimePathConfig.clangBinaryPath)) {
        throw CompilerException("Selected clang binary path dose not exists. Path: " + runtimePathConfig.clangBinaryPath);
    }

    for (auto includeDir : runtimePathConfig.includePaths) {
        if (!std::filesystem::exists(includeDir)) {
            throw CompilerException("Selected include path dose not exists. Path: " + includeDir);
        }
    }

    for (auto libDir : runtimePathConfig.libPaths) {
        if (!std::filesystem::exists(libDir)) {
            throw CompilerException("Selected lib path dose not exists. Path: " + libDir);
        }
    }
    std::stringstream runtimePath;
    runtimePath << runtimePathConfig;
    NES_INFO("RuntimePathConfig: {}", runtimePath.str());

    return runtimePathConfig;
}

namespace detail {

std::filesystem::path recursiveFindFileReverse(std::filesystem::path currentPath, const std::string targetFileName) {
    while (!std::filesystem::is_directory(currentPath)) {
        currentPath = currentPath.parent_path();
    }
    while (currentPath != currentPath.root_directory()) {
        for (const auto& entry : std::filesystem::directory_iterator(currentPath)) {
            if (entry.is_directory()) {

                auto fname = entry.path().string();
                if (fname.ends_with(targetFileName)) {
                    return entry.path();
                }
            }

            auto path = entry.path();
            auto fname = path.filename();
            if (fname.string().compare(targetFileName) == 0) {
                return path;
            }
        }
        currentPath = currentPath.parent_path();
    }
    return currentPath;
}
}// namespace detail

#if __APPLE__
#include <mach-o/dyld.h>

std::filesystem::path getExecutablePath() {
    typedef std::vector<char> char_vector;
    char_vector buf(1024, 0);
    uint32_t size = static_cast<uint32_t>(buf.size());
    bool havePath = false;
    bool shouldContinue = true;
    do {
        int result = _NSGetExecutablePath(&buf[0], &size);
        if (result == -1) {
            buf.resize(size + 1);
            std::fill(std::begin(buf), std::end(buf), 0);
        } else {
            shouldContinue = false;
            if (buf.at(0) != 0) {
                havePath = true;
            }
        }
    } while (shouldContinue);
    if (!havePath) {
        return std::filesystem::current_path();
    }
    std::error_code ec;
    std::string path(&buf[0], size);
    std::filesystem::path p(std::filesystem::canonical(path, ec));
    if (!ec) {
        return p.make_preferred();
    }
    return std::filesystem::current_path();
}

std::filesystem::path getLibPath(std::string libName) {
    auto executablePath = getExecutablePath();
    auto libPath = detail::recursiveFindFileReverse(executablePath, libName);

    if (std::filesystem::is_regular_file(libPath)) {
        std::stringstream pathAsString;
        pathAsString << libPath.parent_path();
        NES_DEBUG("Library {} found at: {}", libName, pathAsString.str());
        return libPath;
    } else {
        std::stringstream libPathStr;
        libPathStr << libPath.parent_path();
        NES_DEBUG("Invalid {} file found at {}. Searching next in DYLD_LIBRARY_PATH.", libName, libPathStr.str());

        std::stringstream dyld_string(std::getenv("DYLD_LIBRARY_PATH"));
        std::string path;

        while (std::getline(dyld_string, path, ':')) {
            if (path == "") {
                continue;
            }
            libPath = detail::recursiveFindFileReverse(path, libName);
            if (std::filesystem::is_regular_file(libPath)) {
                std::stringstream pathStr;
                pathStr << libPath.parent_path();
                NES_DEBUG("Library {} found at: {}", libName, pathStr.str());
                return libPath;
            }
        }
    }
    NES_FATAL_ERROR("No valid {} found in executable path or DYLD_LIBRARY_PATH.", libName);
    return std::filesystem::current_path();
}

#elif __linux__
#include <limits.h>
#include <unistd.h>

std::filesystem::path getExecutablePath() {
    char result[PATH_MAX];
    ssize_t count = readlink("/proc/self/exe", result, PATH_MAX);
    auto resultString = std::string(result, (count > 0) ? count : 0);
    std::error_code ec;
    std::filesystem::path p(std::filesystem::canonical(resultString, ec));
    if (!ec) {
        return p.make_preferred();
    }
    return std::filesystem::current_path();
}

std::filesystem::path getLibPath(std::string libName) {
    auto executablePath = getExecutablePath().parent_path().parent_path();
    auto libPath = detail::recursiveFindFileReverse(executablePath, "lib").append(libName);

    if (std::filesystem::is_regular_file(libPath)) {
        std::stringstream pathAsString;
        pathAsString << libPath.parent_path();
        NES_DEBUG("Library {} found at: {}", libName, pathAsString.str());
        return libPath;
    }
    throw CompilerException("Path to " + libName + " not found. Executable path is: " + executablePath.string());
}

#endif

std::filesystem::path getPublicIncludes() {
    auto executablePath = getExecutablePath();
    auto includePath = detail::recursiveFindFileReverse(executablePath, "include").append("nebulastream");
    if (exists(includePath)) {
        std::stringstream path;
        path << includePath;
        NES_DEBUG("NebulaStream include path found at {}", path.str());
        return includePath;
    }
    throw CompilerException("NebulaStream include path found not found. Executable path is: " + executablePath.string());
}
std::filesystem::path getClangPath() {
    // Depending on the current environment the clang executable could be found at different places.
    // 1. if the system is installed then we should find a nes-clang executable next to the current executable, e.g. nesCoordinator.
    // 2. if we are in the build environment CLANG_EXECUTABLE should indicate the location of clang.
    // TODO check locations on MacOS.
    auto executablePath = getExecutablePath();
    auto nesClangPath = executablePath.parent_path().append("nes-clang");
    if (std::filesystem::exists(nesClangPath)) {
        std::stringstream path;
        path << nesClangPath;
        NES_DEBUG("Clang found at: {}", path.str());
        return std::filesystem::path(nesClangPath);
    } else if (std::filesystem::exists(CLANG_EXECUTABLE)) {
        NES_DEBUG("Clang found at: {}", CLANG_EXECUTABLE);
        return std::filesystem::path(CLANG_EXECUTABLE);
    }
    throw CompilerException("Path to clang executable not found");
}

}// namespace NES::Compiler::ExecutablePath
