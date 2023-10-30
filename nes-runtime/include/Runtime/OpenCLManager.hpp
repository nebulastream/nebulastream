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

#ifndef NES_RUNTIME_INCLUDE_RUNTIME_OPENCLMANAGER_HPP_
#define NES_RUNTIME_INCLUDE_RUNTIME_OPENCLMANAGER_HPP_

#include <Runtime/OpenCLDeviceInfo.hpp>
#include <vector>

#ifdef ENABLE_OPENCL
#ifdef __APPLE__
#include <OpenCL/cl.h>
#else
#include <CL/cl.h>
#endif
#else
// Define some OpenCL types, so that this header file compiles, and we don't need #ifdef's everywhere.
using cl_platform_id = unsigned;
using cl_device_id = unsigned;
#endif

namespace NES::Runtime {

/**
 * Data structure to hold the information about the available OpenCL devices in a worker.
 *
 * In contrast to OpenCLDeviceInfo, this struct also holds the OpenCL platform ID and device ID to access the device using the OpenCL API.
 */
struct WorkerOpenCLDeviceInfo {
  public:
    WorkerOpenCLDeviceInfo(const cl_platform_id platformId, const cl_device_id deviceId, const OpenCLDeviceInfo& deviceInfo)
        : platformId(platformId), deviceId(deviceId), deviceInfo(deviceInfo) {}
  public:
    const cl_platform_id platformId;
    const cl_device_id deviceId;
    const OpenCLDeviceInfo deviceInfo;
};

/**
 * Retrieve and store information about installed OpenCL devices.
 */
class OpenCLManager {
  public:
    OpenCLManager();

    const std::vector<WorkerOpenCLDeviceInfo>& getDevices() const;

  private:
    std::vector<WorkerOpenCLDeviceInfo> devices;
};

}// namespace NES::Runtime

#endif// NES_RUNTIME_INCLUDE_RUNTIME_OPENCLMANAGER_HPP_
