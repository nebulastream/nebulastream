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

#include <unistd.h>
#include <Elegant/ElegantAccelerationServiceClient.hpp>
#include <Exceptions/ElegantServiceException.hpp>
#include <Operators/LogicalOperators/UDFs/JavaUDFDescriptor.hpp>
#include <cpr/cpr.h>
#include <Util/Logger/Logger.hpp>

namespace NES::ELEGANT {
ElegantAccelerationServiceClient::ElegantAccelerationServiceClient(
    const std::string_view& baseUrl,
    const Catalogs::UDF::JavaUdfDescriptorPtr& udfDescriptor)
    : baseUrl(cpr::Url{baseUrl.data(), baseUrl.size()} + "/api/acceleration"),
      udfDescriptor(udfDescriptor)
{ }

const std::string ElegantAccelerationServiceClient::retrieveOpenCLKernel() const {
    const auto requestId = executeSubmitRequest();
    waitForRequestCompletion(requestId);
    std::string openCLCode = executeRetrieveRequest(requestId);
    executeDeleteRequest(requestId);
    return openCLCode;
}

unsigned ElegantAccelerationServiceClient::executeSubmitRequest() const {
#ifdef USE_DUMMY_OPENCL_KERNEL
    auto url = baseUrl + "/submit_prebuilt";
    NES_DEBUG("Submitting request to ELEGANT acceleration service; url = {}", url.c_str());
    auto response = cpr::Post(url, cpr::Header{{"Content-Type", "application/json"}}, timeout);
    if (response.status_code != 202) {
        NES_DEBUG("ELEGANT acceleration service returned error response; status_code = {}, message = {}", response.status_code, response.text);
        throw new ElegantServiceException("Submission to ELEGANT acceleration service failed");
    }
    // Output of the submit_prebuilt request looks as follows:
    //    The request has been completed.
    //    New code acceleration request has been registered (#1)
    // The number on the second line is the request ID
    auto start_pos = response.text.find("(#");
    auto end_pos = response.text.find(")");
    if (start_pos == response.text.npos || end_pos == response.text.npos) {
        NES_DEBUG("Could not find request id in response; text = {}", response.text);
        throw new ElegantServiceException("Parsing of ELEGANT acceleration service submit request failed");
    }
    start_pos += 2; // account for "(#"
    auto parsedRequestId = response.text.substr(start_pos, end_pos - start_pos);
    NES_DEBUG("Parsed request ID: {}", parsedRequestId);
    return std::stoi(parsedRequestId);
#else
    // nlohmann::json payload;
    // payload[DEVICE_INFO_KEY] = std::any_cast<std::vector<Runtime::OpenCLDeviceInfo>>(
    //     executionNode->getTopologyNode()->getNodeProperty(Worker::Configuration::OPENCL_DEVICES))[openCLOperator->getDeviceId()];
    // payload["functionCode"] = openCLOperator->getJavaUDFDescriptor()->getMethodName();
    //
    // // The constructor of the Java UDF descriptor ensures that the byte code of the class exists.
    // jni::JavaByteCode javaByteCode = openCLOperator->getJavaUDFDescriptor()->getClassByteCode(openCLOperator->getJavaUDFDescriptor()->getClassName()).value();
    //
    // //5. Prepare the multi-part message
    // cpr::Part part1 = {"jsonFile", to_string(payload)};
    // cpr::Part part2 = {"codeFile", base64_encode(reinterpret_cast<unsigned char const*>(javaByteCode.data()), javaByteCode.size())};
    // cpr::Multipart multipartPayload = cpr::Multipart{part1, part2};
    //
    // //6. Make Acceleration Service Call
    // cpr::Response response = cpr::Post(cpr::Url{accelerationServiceURL + "/api/acceleration/submit"},
    //                                    cpr::Header{{"Content-Type", "multipart/form-data"}},
    //                                    multipartPayload,
    //                                    cpr::Timeout(ELEGANT_SERVICE_TIMEOUT));

    // Store Java classes in filesystem
    auto now = std::chrono::system_clock::now();
    auto in_time_t = std::chrono::system_clock::to_time_t(now);
    std::stringstream ss;
    ss << "nes-udf." << std::put_time(std::localtime(&in_time_t), "%Y%m%d_%H%M%S");
    const auto tmpDir = std::filesystem::temp_directory_path() / ss.str();
    NES_ASSERT(std::filesystem::create_directory(tmpDir), "Could not create temporary directory for decompiling Java UDF classes: " << tmpDir);
    const auto classesDir = tmpDir / "classes";
    NES_ASSERT(std::filesystem::create_directory(classesDir), "Could not create temporary directory for storing Java UDF classes: " << classesDir);
    for (const auto& [name, bytecode] : udfDescriptor->getByteCodeList()) {
        size_t start = 0, end = 0;
        auto classFileDir = classesDir;
        while (true) {
            end = name.find(".", start);
            if (end == std::string::npos) {
                break;
            }
            classFileDir = classFileDir / name.substr(start, end - start);
            if (!std::filesystem::exists(classFileDir)) {
                NES_ASSERT(std::filesystem::create_directory(classFileDir), "Could not create Java UDF class directory: " << classFileDir);
            }
            start = end + 1;
        }
        auto classFileName = name.substr(start) + ".class";
        auto classFilePath = classFileDir / classFileName;
        NES_DEBUG("Storing Java UDF class file: class = {}, path = {}", name, classFilePath.c_str());
        std::ofstream classFile{classFilePath, std::ofstream::binary};
        classFile.write(bytecode.data(), bytecode.size());
        classFile.close();
    }

    return 0;
#endif
}

const std::string ElegantAccelerationServiceClient::executeStateRequest(const unsigned requestId) const {
    auto url = baseUrl + "/" + std::to_string(requestId) + "/state";
    NES_DEBUG("Submitting request to ELEGANT acceleration service; url = {}", url.c_str());
    auto response = cpr::Get(url, timeout);
    if (response.status_code != 200) {
        NES_DEBUG("ELEGANT acceleration service returned error response; status_code = {}, message = {}", response.status_code, response.text);
        throw new ElegantServiceException("Retrieving ELEGANT acceleration service request state failed");
    }
    return response.text;
}

const std::string ElegantAccelerationServiceClient::executeRetrieveRequest(const unsigned requestId) const {
    auto url = baseUrl + "/" + std::to_string(requestId) + "/retrieve";
    NES_DEBUG("Submitting request to ELEGANT acceleration service; url = {}", url.c_str());
    auto response = cpr::Get(url, timeout);
    if (response.status_code != 200) {
        NES_DEBUG("ELEGANT acceleration service returned error response; status_code = {}, message = {}", response.status_code, response.text);
        throw new ElegantServiceException("Retrieving OpenCL kernel from ELEGANT acceleration service failed");
    }
    return response.text;
}

void ElegantAccelerationServiceClient::executeDeleteRequest([[maybe_unused]] unsigned requestId) const {
#ifdef USE_DUMMY_OPENCL_KERNEL
    auto url = baseUrl + "/delete_prebuilt/" + std::to_string(requestId);
    NES_DEBUG("Submitting request to ELEGANT acceleration service; url = {}", url.c_str());
    auto response = cpr::Delete(url, timeout);
    if (response.status_code != 200) {
        NES_DEBUG("ELEGANT acceleration service returned error response; status_code = {}, message = {}", response.status_code, response.text);
        throw new ElegantServiceException("Deleting request ELEGANT acceleration service failed");
    }
#else
// #error "Not implemented"
#endif
}

void ElegantAccelerationServiceClient::waitForRequestCompletion(const unsigned requestId) const {
    // Retrieve state of the ELEGANT acceleration service request until it is completed.
    // Start with a 1 second delay and then double the delay on every retry.
    static std::string_view completed = "\"COMPLETED\"";
    uint32_t tries = 1;
    uint32_t delay = 1;
    std::string state = executeStateRequest(requestId);
    while (state != completed && tries <= 7) {
        tries += 1;
        NES_DEBUG("ELEGANT acceleration request is not completed, retrying; state = {}; tries = {}, delay = {}", state, tries, delay);
        sleep(delay);
        state = executeStateRequest(requestId);
        delay *= 2;
    }
    if (state != completed) {
        NES_DEBUG("ELEGANT acceleration request did not complete after retries; tries = {}, state = {}", tries, state);
        throw new ElegantServiceException("ELEGANT acceleration service did not complete request");
    }
}

} // NES::ELEGANT
