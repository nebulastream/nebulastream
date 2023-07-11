/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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
#include <stdexcept>
#include <string>
#include <utility>

class OpenCLInitializationException : public std::runtime_error {
  public:
    OpenCLInitializationException(const std::string& message, cl_int statusCode)
        : std::runtime_error(message), statusCode(statusCode) {}
    cl_int status() { return statusCode; }

  private:
    cl_int statusCode;
};

#define ASSERT_OPENCL_SUCCESS(status, message)                                                                                   \
    do {                                                                                                                         \
        if (status != CL_SUCCESS) {                                                                                              \
            throw OpenCLInitializationException(message, status);                                                                \
        }                                                                                                                        \
    } while (false)

namespace NES::Runtime {

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
std::pair<std::string, std::string> retrieveOpenCLPlatformInfo(const cl_platform_id platform) {
    cl_int status;
    char buffer[1024];
    status = clGetPlatformInfo(platform, CL_PLATFORM_VENDOR, sizeof(buffer), buffer, nullptr);
    ASSERT_OPENCL_SUCCESS(status, "Failed to get platform vendor");
    std::string platformVendor(buffer);
    status = clGetPlatformInfo(platform, CL_PLATFORM_NAME, sizeof(buffer), buffer, nullptr);
    ASSERT_OPENCL_SUCCESS(status, "Failed to get platform name");
    std::string platformName(buffer);
    return {platformVendor, platformName};
}

// Helper method to retrieve OpenCL devices of an OpenCL platform.
std::vector<cl_device_id> retrieveOpenCLDevices(const cl_platform_id platform) {
    cl_uint numDevices = 0;
    auto status = clGetDeviceIDs(platform, CL_DEVICE_TYPE_ALL, 0, nullptr, &numDevices);
    ASSERT_OPENCL_SUCCESS(status, "Failed to get number of devices");
    auto devices = std::vector<cl_device_id>(numDevices);
    if (numDevices == 0) {
        NES_DEBUG("Did not find any OpenCL device.");
    } else {
        status = clGetDeviceIDs(platform, CL_DEVICE_TYPE_ALL, numDevices, devices.data(), nullptr);
        ASSERT_OPENCL_SUCCESS(status, "Failed to get OpenCL device IDs");
    }
    return devices;
}

std::string retrieveOpenCLDeviceName(const cl_device_id device) {
    char buffer[1024];
    auto status = clGetDeviceInfo(device, CL_DEVICE_NAME, sizeof(buffer), buffer, nullptr);
    ASSERT_OPENCL_SUCCESS(status, "Could not get device name");
    std::string deviceName(buffer);
    return deviceName;
}

bool retrieveOpenCLDoubleFPSupport(const cl_device_id device) {
    cl_device_fp_config deviceFpConfig;
    auto status = clGetDeviceInfo(device, CL_DEVICE_DOUBLE_FP_CONFIG, sizeof(cl_device_fp_config), &deviceFpConfig, nullptr);
    ASSERT_OPENCL_SUCCESS(status, "Could not get CL_DEVICE_DOUBLE_FP_CONFIG information");
    return deviceFpConfig > 0;// TODO How to evaluate this bitfield?
}

std::array<size_t, 3> retrieveOpenCLMaxWorkItems(const cl_device_id device) {
    cl_uint dimensions;
    auto status = clGetDeviceInfo(device, CL_DEVICE_MAX_WORK_ITEM_DIMENSIONS, sizeof(cl_uint), &dimensions, nullptr);
    ASSERT_OPENCL_SUCCESS(status, "Could not get CL_DEVICE_MAX_WORK_ITEM_DIMENSIONS information");
    if (dimensions != 3) {
        throw OpenCLInitializationException("Only three work items are supported", dimensions);
    }
    std::array<size_t, 3> maxWorkItems;
    status = clGetDeviceInfo(device, CL_DEVICE_MAX_WORK_ITEM_SIZES, sizeof(maxWorkItems), maxWorkItems.data(), nullptr);
    ASSERT_OPENCL_SUCCESS(status, "Could not get CL_DEVICE_MAX_WORK_ITEM_SIZES information");
    return maxWorkItems;
}

unsigned retrieveOpenCLDeviceAddressBits(const cl_device_id device) {
    cl_uint deviceAddressBits;
    auto status = clGetDeviceInfo(device, CL_DEVICE_ADDRESS_BITS, sizeof(cl_uint), &deviceAddressBits, nullptr);
    ASSERT_OPENCL_SUCCESS(status, "Could not get CL_DEVICE_ADDRESS_BITS information");
    return deviceAddressBits;
}

std::string retrieveOpenCLDeviceType(const cl_device_id device) {
    cl_device_type deviceType;
    auto status = clGetDeviceInfo(device, CL_DEVICE_TYPE, sizeof(deviceType), &deviceType, nullptr);
    ASSERT_OPENCL_SUCCESS(status, "Could not get CL_DEVICE_TYPE information");
    switch (deviceType) {
        case CL_DEVICE_TYPE_CPU: return "CL_DEVICE_TYPE_CPU"; break;
        case CL_DEVICE_TYPE_GPU: return "CL_DEVICE_TYPE_GPU"; break;
        case CL_DEVICE_TYPE_ACCELERATOR: return "CL_DEVICE_TYPE_ACCELERATOR"; break;
        case CL_DEVICE_TYPE_CUSTOM: return "CL_DEVICE_TYPE_CUSTOM"; break;
        default: throw OpenCLInitializationException("Unknown device type", deviceType);
    }
}

std::string retrieveOpenCLDeviceExtensions(const cl_device_id device) {
    char buffer[1024];
    auto status = clGetDeviceInfo(device, CL_DEVICE_EXTENSIONS, sizeof(buffer), buffer, nullptr);
    ASSERT_OPENCL_SUCCESS(status, "Could not get device extensions");
    std::string extensions(buffer);
    return extensions;
}

unsigned retrieveOpenCLAvailableProcessors(const cl_device_id device) {
    cl_uint maxComputeUnits;
    auto status = clGetDeviceInfo(device, CL_DEVICE_MAX_COMPUTE_UNITS, sizeof(cl_uint), &maxComputeUnits, nullptr);
    ASSERT_OPENCL_SUCCESS(status, "Could not get CL_DEVICE_MAX_COMPUTE_UNITS information");
    return maxComputeUnits;
}

OpenCLManager::OpenCLManager() {
    try {
        NES_DEBUG("Determining installed OpenCL devices ...");
        auto platforms = retrieveOpenCLPlatforms();
        for (const auto& platform : platforms) {
            auto [platformVendor, platformName] = retrieveOpenCLPlatformInfo(platform);
            NES_DEBUG("Found OpenCL platform: {} {}", platformVendor, platformName);
            auto devices = retrieveOpenCLDevices(platform);
            for (const auto& device : devices) {
                auto deviceName = retrieveOpenCLDeviceName(device);
                auto doubleFPSupport = retrieveOpenCLDoubleFPSupport(device);
                auto maxWorkItems = retrieveOpenCLMaxWorkItems(device);
                auto deviceAddressBits = retrieveOpenCLDeviceAddressBits(device);
                auto deviceType = retrieveOpenCLDeviceType(device);
                auto deviceExtensions = retrieveOpenCLDeviceExtensions(device);
                auto availableProcessors = retrieveOpenCLAvailableProcessors(device);
                NES_INFO(
                    "Found OpenCL device: [platformVendor={}, platformName={}, deviceName={}, doubleFPSupport={}, "
                    "maxWorkItems=({}, {}, {}), deviceAddressBits={}, deviceType={}, deviceExtensions={}, availableProcessors={}",
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
                OpenCLDeviceInfo deviceInfo(platform,
                                            device,
                                            platformVendor,
                                            platformName,
                                            deviceName,
                                            doubleFPSupport,
                                            maxWorkItems,
                                            deviceAddressBits,
                                            deviceType,
                                            deviceExtensions,
                                            availableProcessors);
            }
        }
    } catch (OpenCLInitializationException e) {
        NES_ERROR("{}: {}", e.what(), e.status());
    }
}

}// namespace NES::Runtime