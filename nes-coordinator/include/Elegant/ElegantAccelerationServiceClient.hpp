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

#ifndef ELEGANTACCELERATIONSERVICECLIENT_HPP
#define ELEGANTACCELERATIONSERVICECLIENT_HPP

#include <filesystem>
#include <string>
#include <cpr/cprtypes.h>
#include <cpr/timeout.h>
#include <Operators/OperatorForwardDeclaration.hpp>
#include <Runtime/OpenCLDeviceInfo.hpp>

namespace NES::ELEGANT {

class ElegantAccelerationServiceClient {
public:
    ElegantAccelerationServiceClient(
        const std::string_view& baseUrl,
        const Catalogs::UDF::JavaUdfDescriptorPtr& javaUdfDescriptor,
        const Runtime::OpenCLDeviceInfo& openCLDeviceInfo);
    ElegantAccelerationServiceClient(const ElegantAccelerationServiceClient&) = delete;
    ElegantAccelerationServiceClient& operator=(const ElegantAccelerationServiceClient&) = delete;
    ~ElegantAccelerationServiceClient();
    const std::string retrieveOpenCLKernel() const;
private:
    static const std::filesystem::path createTemporaryPath();
    void storeUdfJavaClasses() const;
    void decompileUdfJavaClasses() const;
    std::string readJavaUdfSource() const;
    unsigned executeSubmitRequest() const;
    const std::string executeStateRequest(unsigned requestId) const;
    const std::string executeRetrieveRequest(unsigned requestId) const;
    void executeDeleteRequest(unsigned requestId) const;
    void waitForRequestCompletion(unsigned requestId) const;
    cpr::Url baseUrl;
    cpr::Timeout timeout{3000};
    const Catalogs::UDF::JavaUdfDescriptorPtr& javaUdfDescriptor;
    [[maybe_unused]] const Runtime::OpenCLDeviceInfo& openCLDeviceInfo;
    const std::filesystem::path tmpDir;
    const std::filesystem::path classesDir;
    const std::filesystem::path sourcesDir;
};

} // NES::ELEGANT

#endif //ELEGANTACCELERATIONSERVICECLIENT_HPP
