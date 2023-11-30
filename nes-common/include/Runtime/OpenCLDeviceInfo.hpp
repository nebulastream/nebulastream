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

#ifndef NES_COMMON_INCLUDE_RUNTIME_OPENCLDEVICEINFO_HPP_
#define NES_COMMON_INCLUDE_RUNTIME_OPENCLDEVICEINFO_HPP_

#include <array>
#include <nlohmann/json.hpp>
#include <string>

namespace NES::Runtime {

/**
 * Data structure to hold the information needed by the ELEGANT Planner and Acceleration Service to compile a kernel for an OpenCL device.
 */
struct OpenCLDeviceInfo {
  public:
    OpenCLDeviceInfo(const std::string& platformVendor,
                     const std::string& platformName,
                     const std::string& deviceName,
                     bool doubleFPSupport,
                     std::array<size_t, 3> maxWorkItems,
                     unsigned deviceAddressBits,
                     const std::string& deviceType,
                     const std::string& deviceExtensions,
                     unsigned availableProcessors,
                     unsigned long globalMemory)
        : platformVendor(platformVendor), platformName(platformName), deviceName(deviceName), doubleFPSupport(doubleFPSupport),
          maxWorkItems(maxWorkItems), deviceAddressBits(deviceAddressBits), deviceType(deviceType),
          deviceExtensions(deviceExtensions), availableProcessors(availableProcessors), globalMemory(globalMemory) {}

  public:
    constexpr static unsigned GRID_DIMENSIONS = 3;

  public:
    std::string platformVendor;
    std::string platformName;
    std::string deviceName;
    bool doubleFPSupport;
    std::array<size_t, GRID_DIMENSIONS> maxWorkItems;
    unsigned deviceAddressBits;
    std::string deviceType;
    std::string deviceExtensions;
    unsigned availableProcessors;
    unsigned long globalMemory;
};

// Define helper methods to convert OpenCLDeviceInfo to JSON and back.
NLOHMANN_DEFINE_TYPE_NON_INTRUSIVE(OpenCLDeviceInfo,
                                   platformVendor,
                                   platformName,
                                   deviceName,
                                   doubleFPSupport,
                                   maxWorkItems,
                                   deviceAddressBits,
                                   deviceType,
                                   deviceExtensions,
                                   availableProcessors);

}// namespace NES::Runtime

#endif// NES_COMMON_INCLUDE_RUNTIME_OPENCLDEVICEINFO_HPP_
