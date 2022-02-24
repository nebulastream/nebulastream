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
#include <Compiler/Util/ExecutablePath.hpp>
#include <Compiler/Exceptions/CompilerException.hpp>
#include <Util/Logger.hpp>
#include <filesystem>
#include <vector>

namespace NES::Compiler::ExecutablePath {

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

std::filesystem::path NES::ExecutablePath::getExecutablePath() {
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

std::filesystem::path NES::ExecutablePath::getLibPath(std::string libName) {
    auto executablePath = getExecutablePath();
    auto libPath = detail::recursiveFindFileReverse(executablePath, libName);

    if (std::filesystem::is_regular_file(libPath)) {
        NES_DEBUG("Library " << libName << " found at: " << libPath.parent_path());
        return libPath;
    } else {
        NES_DEBUG("Invalid " << libName << " file found at " << libPath << ". Searching next in DYLD_LIBRARY_PATH.");

        std::stringstream dyld_string(std::getenv("DYLD_LIBRARY_PATH"));
        std::string path;

        while (std::getline(dyld_string, path, ':')) {
            if (path == "") {
                continue;
            }
            libPath = detail::recursiveFindFileReverse(path, libName);
            if (std::filesystem::is_regular_file(libPath)) {
                NES_DEBUG("Library " << libName << "found at: " << libPath.parent_path());
                return libPath;
            }
        }
    }
    NES_FATAL_ERROR("No valid " << libName << " found in executable path or DYLD_LIBRARY_PATH.");
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
    auto executablePath = getExecutablePath();
    auto libPath = detail::recursiveFindFileReverse(executablePath, libName);

    if (std::filesystem::is_regular_file(libPath)) {
        NES_DEBUG("Library " << libName << " found at: " << libPath.parent_path());
        return libPath;
    }
    return std::filesystem::current_path();
}

#endif

std::filesystem::path getPublicIncludes() {
    auto executablePath = getExecutablePath();
    auto includePath = detail::recursiveFindFileReverse(executablePath, "include");
    if (exists(includePath.append("nebulastream"))) {
        return includePath;
    }
    throw CompilerException("Path to the nebulastreams include files was not found");
}
std::filesystem::path getClangPath() {
    // Depending on the current environment the clang executable could be found at different places.
    // 1. if the system is installed then we should find a nes-clang executable next to the current executable, e.g. nesCoordinator.
    // 2. if we are in the build environment CLANG_EXECUTABLE should indicate the location of clang.
    // TODO check locations on MacOS.
    auto executablePath = getExecutablePath();
    auto nesClangPath = executablePath.parent_path().append("nes-clang");
    if (std::filesystem::exists(nesClangPath)) {
        NES_DEBUG("Clang found at: " << nesClangPath);
        return std::filesystem::path(nesClangPath);
    } else if (std::filesystem::exists(CLANG_EXECUTABLE)) {
        NES_DEBUG("Clang found at: " << CLANG_EXECUTABLE);
        return std::filesystem::path(CLANG_EXECUTABLE);
    }
    throw CompilerException("Path to clang executable not found");
}

}// namespace NES::ExecutablePath