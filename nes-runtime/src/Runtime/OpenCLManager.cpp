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

#include <Runtime/OpenCLManager.hpp>
#include <Util/Logger/Logger.hpp>
#include <regex>
#include <stdexcept>
#include <string>
#include <utility>

namespace NES::Runtime {

#ifdef ENABLE_OPENCL
// Shut up -Wweak-vtables warning. This exception type is only used internally during the construction of the OpenCLManager,
// so we don't want to put it into a separate translation unit and header file.
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wweak-vtables"
class OpenCLInitializationException : public std::runtime_error {
  public:
    OpenCLInitializationException(const std::string& message, cl_int statusCode)
        : std::runtime_error(message), statusCode(statusCode) {}
    cl_int status() { return statusCode; }

  private:
    cl_int statusCode;
};
#pragma clang diagnostic pop

#define ASSERT_OPENCL_SUCCESS(status, message)                                                                                   \
    do {                                                                                                                         \
        if (status != CL_SUCCESS) {                                                                                              \
            throw OpenCLInitializationException(message, status);                                                                \
        }                                                                                                                        \
    } while (false)

// Helper method to retrieve the installed OpenCL platforms
std::vector<cl_platform_id> retrieveOpenCLPlatforms() {
    cl_int status;
    cl_uint numPlatforms = 0;
    status = clGetPlatformIDs(0, nullptr, &numPlatforms);
    ASSERT_OPENCL_SUCCESS(status, "Failed to get number of OpenCL platforms");
    auto platforms = std::vector<cl_platform_id>(numPlatforms);
    if (numPlatforms == 0) {
        NES_DEBUG("Did not find any OpenCL platforms.");
    } else {
        status = clGetPlatformIDs(numPlatforms, platforms.data(), nullptr);
        ASSERT_OPENCL_SUCCESS(status, "Failed to get OpenCL platform IDs");
    }
    return platforms;
}

// Helper method to retrieve some information about an OpenCL platform.
std::pair<std::string, std::string> retrieveOpenCLPlatformInfo(const cl_platform_id platformId) {
    cl_int status;
    char buffer[1024];
    status = clGetPlatformInfo(platformId, CL_PLATFORM_VENDOR, sizeof(buffer), buffer, nullptr);
    ASSERT_OPENCL_SUCCESS(status, "Failed to get platform vendor");
    std::string platformVendor(buffer);
    status = clGetPlatformInfo(platformId, CL_PLATFORM_NAME, sizeof(buffer), buffer, nullptr);
    ASSERT_OPENCL_SUCCESS(status, "Failed to get platform name");
    std::string platformName(buffer);
    return {platformVendor, platformName};
}

// Helper method to retrieve OpenCL devices of an OpenCL platform.
std::vector<cl_device_id> retrieveOpenCLDevices(const cl_platform_id platformId) {
    cl_uint numDevices = 0;
    auto status = clGetDeviceIDs(platformId, CL_DEVICE_TYPE_ALL, 0, nullptr, &numDevices);
    ASSERT_OPENCL_SUCCESS(status, "Failed to get number of devices");
    auto devices = std::vector<cl_device_id>(numDevices);
    if (numDevices == 0) {
        NES_DEBUG("Did not find any OpenCL device.");
    } else {
        status = clGetDeviceIDs(platformId, CL_DEVICE_TYPE_ALL, numDevices, devices.data(), nullptr);
        ASSERT_OPENCL_SUCCESS(status, "Failed to get OpenCL device IDs");
    }
    return devices;
}

// Wrapper around clGetDeviceInfo which adds error handling and optionally converts to a non-OpenCL type used in NES.
template<typename CLType, typename NESType>
NESType retrieveOpenCLDeviceInfo(const cl_device_id deviceId, const cl_device_info param, const std::string& paramName) {
    CLType result;
    auto status = clGetDeviceInfo(deviceId, param, sizeof(result), &result, nullptr);
    ASSERT_OPENCL_SUCCESS(status, "Could not get information for OpenCL device parameter " + paramName);
    return NESType(result);
}

// Specialization to retrieve OpenCL device parameters that are strings.
template<>
std::string retrieveOpenCLDeviceInfo<char[], std::string>(const cl_device_id deviceId,
                                                          const cl_device_info param,
                                                          const std::string& paramName) {
    size_t size;
    auto status = clGetDeviceInfo(deviceId, param, 0, nullptr, &size);
    ASSERT_OPENCL_SUCCESS(status, "Could not get result size for OpenCL device parameter " + paramName);
    // The size returned by the OpenCL API includes the end-of-string terminator \0.
    // We must allocate enough space, so that the API can write the result.
    // However, if we print this string, it would include the terminator as a special character in the output.
    // Therefore, we resize the string after retrieving the value to exclude the terminator.
    std::string result(size, '\0');
    status = clGetDeviceInfo(deviceId, param, size, const_cast<char*>(result.c_str()), nullptr);
    ASSERT_OPENCL_SUCCESS(status, "Could not get information for OpenCL device parameter " + paramName);
    result.resize(size - 1);
    return result;
}

// Special function to retrieve information about support for double-precision floating point operations.
bool retrieveOpenCLDoubleFPSupport(const cl_device_id deviceId) {
    // From https://registry.khronos.org/OpenCL/sdk/3.0/docs/man/html/clGetDeviceInfo.html
    // Double precision is an optional feature so the mandated minimum double precision floating-point capability is 0.
    // If double precision is supported by the device, then the minimum double precision floating-point capability for OpenCL 2.0
    // or newer devices is:
    //   CL_FP_FMA | CL_FP_ROUND_TO_NEAREST | CL_FP_INF_NAN | CL_FP_DENORM.
    // or for OpenCL 1.0, OpenCL 1.1 or OpenCL 1.2 devices:
    //   CL_FP_FMA | CL_FP_ROUND_TO_NEAREST | CL_FP_ROUND_TO_ZERO | CL_FP_ROUND_TO_INF | CL_FP_INF_NAN | CL_FP_DENORM.
    auto deviceFpConfig = retrieveOpenCLDeviceInfo<cl_device_fp_config, cl_device_fp_config>(deviceId,
                                                                                             CL_DEVICE_DOUBLE_FP_CONFIG,
                                                                                             "CL_DEVICE_DOUBLE_FP_CONFIG");
    auto deviceVersion = retrieveOpenCLDeviceInfo<char[], std::string>(deviceId, CL_DEVICE_VERSION, "CL_DEVICE_VERSION");
    // From https://registry.khronos.org/OpenCL/sdk/3.0/docs/man/html/clGetDeviceInfo.html
    // The version string has the following format:
    //   OpenCL<space><major_version.minor_version><space><vendor-specific information>
    cl_device_fp_config expectedBits;
    std::smatch regexMatch;
    if (std::regex_match(deviceVersion, regexMatch, std::regex("OpenCL (.*)\\.(.*) .*")) && regexMatch.size() == 3) {
        auto major = std::stoi(regexMatch[1]);
        auto minor = std::stoi(regexMatch[2]);
        if (major == 1 && (minor == 0 || minor == 1 || minor == 2)) {
            expectedBits =
                CL_FP_FMA | CL_FP_ROUND_TO_NEAREST | CL_FP_ROUND_TO_ZERO | CL_FP_ROUND_TO_INF | CL_FP_INF_NAN | CL_FP_DENORM;
        } else if (major >= 2) {
            expectedBits = CL_FP_FMA | CL_FP_ROUND_TO_NEAREST | CL_FP_INF_NAN | CL_FP_DENORM;
        } else {
            throw OpenCLInitializationException("Unknown OpenCL device version: " + deviceVersion, 0);
        }
    } else {
        throw OpenCLInitializationException("Could not parse OpenCL device version: " + deviceVersion, 0);
    }
    return (deviceFpConfig & expectedBits) == expectedBits;
}

// Special function to retrieve information about the maximum work group size.
// Assumes and enforces that the OpenCL ND range has 3 dimensions.
std::array<size_t, OpenCLDeviceInfo::GRID_DIMENSIONS> retrieveOpenCLMaxWorkItems(const cl_device_id deviceId) {
    auto dimensions = retrieveOpenCLDeviceInfo<cl_uint, unsigned>(deviceId,
                                                                  CL_DEVICE_MAX_WORK_ITEM_DIMENSIONS,
                                                                  "CL_DEVICE_MAX_WORK_ITEM_DIMENSIONS");
    if (dimensions != OpenCLDeviceInfo::GRID_DIMENSIONS) {
        throw OpenCLInitializationException("Only three work item dimensions are supported", dimensions);
    }
    std::array<size_t, OpenCLDeviceInfo::GRID_DIMENSIONS> maxWorkItems;
    cl_uint status = clGetDeviceInfo(deviceId, CL_DEVICE_MAX_WORK_ITEM_SIZES, sizeof(maxWorkItems), maxWorkItems.data(), nullptr);
    ASSERT_OPENCL_SUCCESS(status, "Could not get CL_DEVICE_MAX_WORK_ITEM_SIZES information");
    return maxWorkItems;
}

// Special function to retrieve information about the device type.
std::string retrieveOpenCLDeviceType(const cl_device_id deviceId) {
    auto deviceType = retrieveOpenCLDeviceInfo<cl_device_type, cl_device_type>(deviceId, CL_DEVICE_TYPE, "CL_DEVICE_TYPE");
    switch (deviceType) {
        case CL_DEVICE_TYPE_CPU: return "CL_DEVICE_TYPE_CPU";
        case CL_DEVICE_TYPE_GPU: return "CL_DEVICE_TYPE_GPU";
        case CL_DEVICE_TYPE_ACCELERATOR: return "CL_DEVICE_TYPE_ACCELERATOR";
        case CL_DEVICE_TYPE_CUSTOM: return "CL_DEVICE_TYPE_CUSTOM";
        default: throw OpenCLInitializationException("Unknown device type", deviceType);
    }
}

OpenCLManager::OpenCLManager() {
    try {
        NES_DEBUG("Determining installed OpenCL devices ...");
        auto platforms = retrieveOpenCLPlatforms();
        for (const auto& platformId : platforms) {
            auto [platformVendor, platformName] = retrieveOpenCLPlatformInfo(platformId);
            NES_DEBUG("Found OpenCL platform: {} {}", platformVendor, platformName);
            auto devices = retrieveOpenCLDevices(platformId);
            for (const auto& device : devices) {
                auto deviceName = retrieveOpenCLDeviceInfo<char[], std::string>(device, CL_DEVICE_NAME, "CL_DEVICE_NAME");
                auto deviceAddressBits =
                    retrieveOpenCLDeviceInfo<cl_uint, unsigned>(device, CL_DEVICE_ADDRESS_BITS, "CL_DEVICE_ADDRESS_BITS");
                auto deviceExtensions =
                    retrieveOpenCLDeviceInfo<char[], std::string>(device, CL_DEVICE_EXTENSIONS, "CL_DEVICE_EXTENSIONS");
                auto availableProcessors = retrieveOpenCLDeviceInfo<cl_uint, unsigned>(device,
                                                                                       CL_DEVICE_MAX_COMPUTE_UNITS,
                                                                                       "CL_DEVICE_MAX_COMPUTE_UNITS");
                auto doubleFPSupport = retrieveOpenCLDoubleFPSupport(device);
                auto maxWorkItems = retrieveOpenCLMaxWorkItems(device);
                auto deviceType = retrieveOpenCLDeviceType(device);
                NES_INFO("Found OpenCL device: [platformVendor={}, platformName={}, deviceName={}, doubleFPSupport={}, "
                         "maxWorkItems=({}, {}, {}), deviceAddressBits={}, deviceType={}, deviceExtensions={}, "
                         "availableProcessors={}]",
                         platformVendor,
                         platformName,
                         deviceName,
                         doubleFPSupport,
                         maxWorkItems[0],
                         maxWorkItems[1],
                         maxWorkItems[2],
                         deviceAddressBits,
                         deviceType,
                         deviceExtensions,
                         availableProcessors);
                this->devices.emplace_back(platformId,
                                           device,
                                           OpenCLDeviceInfo(platformVendor,
                                                            platformName,
                                                            deviceName,
                                                            doubleFPSupport,
                                                            maxWorkItems,
                                                            deviceAddressBits,
                                                            deviceType,
                                                            deviceExtensions,
                                                            availableProcessors));
            }
        }
    } catch (OpenCLInitializationException e) {
        NES_ERROR("{}: {}", e.what(), e.status());
    }
}
#else // ENABLE_OPENCL
// Define an (almost) empty constructor for OpenCLManager.
// This will leave OpenCLManager::devices as an empty vector, indicating that there are no installed OpenCL devices.
OpenCLManager::OpenCLManager() { NES_DEBUG("OpenCL support was disabled at build time"); }
#endif// ENABLE_OPENCL

const std::vector<WorkerOpenCLDeviceInfo>& OpenCLManager::getDevices() const { return devices; }

}// namespace NES::Runtime
