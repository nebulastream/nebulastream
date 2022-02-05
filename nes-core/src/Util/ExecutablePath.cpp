#include <Util/ExecutablePath.hpp>
#include <Util/Logger.hpp>
#include <filesystem>
#include <vector>

namespace NES::ExecutablePath {

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

std::filesystem::path NES::ExecutablePath::getExecutablePath() {
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

std::filesystem::path NES::ExecutablePath::getLibPath(std::string libName) {
    auto executablePath = getExecutablePath();
    auto libPath = detail::recursiveFindFileReverse(executablePath, libName);

    if (std::filesystem::is_regular_file(libPath)) {
        NES_DEBUG("Library " << libName << " found at: " << libPath.parent_path());
        return libPath;
    }
    return std::filesystem::current_path();
}

#endif

std::filesystem::path NES::ExecutablePath::getPublicIncludes() {
    auto executablePath = getExecutablePath();
    auto includePath = detail::recursiveFindFileReverse(executablePath, "include");
    return includePath;
}

}// namespace NES::ExecutablePath