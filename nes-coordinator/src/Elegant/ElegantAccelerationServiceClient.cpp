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
#include <sstream>
#include <Phases/QueryDeploymentPhase.hpp>
#include <nlohmann/json.hpp>

namespace NES::ELEGANT {

ElegantAccelerationServiceClient::ElegantAccelerationServiceClient(
    const std::string_view& baseUrl,
    const Catalogs::UDF::JavaUdfDescriptorPtr& javaUdfDescriptor,
    const Runtime::OpenCLDeviceInfo& openCLDeviceInfo)
    : baseUrl(cpr::Url{baseUrl.data(), baseUrl.size()} + "/api/acceleration"),
      javaUdfDescriptor(javaUdfDescriptor),
      openCLDeviceInfo(openCLDeviceInfo),
      tmpDir(createTemporaryPath()),
      classesDir(tmpDir / "classes"),
      sourcesDir(tmpDir / "sources") {
    NES_ASSERT(std::filesystem::create_directory(tmpDir),
               "Could not create temporary directory for decompiling Java UDF classes: " << tmpDir);
    NES_ASSERT(std::filesystem::create_directory(classesDir),
               "Could not create temporary directory for storing Java UDF classes: " << classesDir);
    NES_ASSERT(std::filesystem::create_directory(sourcesDir),
               "Could not create temporary directory for storing Java UDF sources: " << sourcesDir);
}

ElegantAccelerationServiceClient::~ElegantAccelerationServiceClient() {
    if (std::filesystem::exists(tmpDir)) {
        std::filesystem::remove_all(tmpDir);
    }
}

const std::string ElegantAccelerationServiceClient::retrieveOpenCLKernel() const {
    const auto requestId = executeSubmitRequest();
    waitForRequestCompletion(requestId);
    std::string openCLCode = executeRetrieveRequest(requestId);
    executeDeleteRequest(requestId);
    return openCLCode;
}

const std::filesystem::path ElegantAccelerationServiceClient::createTemporaryPath() {
    auto in_time_t = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
    std::stringstream ss;
    ss << "nes-udf." << std::put_time(std::localtime(&in_time_t), "%Y%m%d_%H%M%S");
    return std::filesystem::temp_directory_path() / ss.str();
}

void ElegantAccelerationServiceClient::storeUdfJavaClasses() const
{
    for (const auto& [name, bytecode] : javaUdfDescriptor->getByteCodeList()) {
        size_t start = 0, end = 0;
        auto classFileDir = classesDir;
        while (true) {
            end = name.find(".", start);
            if (end == std::string::npos) {
                break;
            }
            classFileDir = classFileDir / name.substr(start, end - start);
            if (!std::filesystem::exists(classFileDir)) {
                NES_ASSERT(std::filesystem::create_directory(classFileDir),
                           "Could not create Java UDF class directory: " << classFileDir);
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
}

void ElegantAccelerationServiceClient::decompileUdfJavaClasses() const {
    std::stringstream cmdline;
    cmdline << "java -jar " << FERNFLOWER_DECOMPILER_JAR << " " << classesDir.c_str() << " " << sourcesDir.c_str();
    NES_DEBUG("Decompiling Java UDF classes with Fernflower decompiler: {}", cmdline.str());
    FILE* fp = popen(cmdline.str().c_str(), "r");
    if (fp == nullptr) {
        NES_ERROR("Failed to run Java UDF classes decompiler");
        throw std::runtime_error("Failed to run Java UDF classes decompiler");
    }
    std::stringstream output;
    char buffer[8192];
    while (fgets(buffer, sizeof(buffer), fp) != nullptr) {
        output << buffer;
    }
    auto returnCode = pclose(fp);
    if (returnCode != 0) {
        NES_ERROR("Decompilation of Java UDF classes failed: {}", output.str());
        throw std::runtime_error("Failed to decompile Java UDF classes");
    }
}

std::string ElegantAccelerationServiceClient::readJavaUdfSource() const {
    auto javaUdfSourceFileName = javaUdfDescriptor->getClassName();
    std::replace(javaUdfSourceFileName.begin(), javaUdfSourceFileName.end(), '.', '/');
    javaUdfSourceFileName += ".java";
    auto javaUdfSourceFilePath = sourcesDir / javaUdfSourceFileName;
    NES_DEBUG("Reading Java UDF sources for class; className = {}, sources = {}", javaUdfDescriptor->getClassName(), javaUdfSourceFilePath.c_str())
    std::ifstream javaUdfSourceFile{javaUdfSourceFilePath};
    std::string javaUdfSources(std::istreambuf_iterator<char>{javaUdfSourceFile}, {});
    return javaUdfSources;
}

unsigned ElegantAccelerationServiceClient::executeSubmitRequest() const {
#ifdef USE_DUMMY_OPENCL_KERNEL
    auto url = baseUrl + "/submit_prebuilt";
    NES_DEBUG("Submitting request to ELEGANT acceleration service; url = {}", url.c_str());
    cpr::Response response = cpr::Post(url, cpr::Header{{"Content-Type", "application/json"}}, timeout);
#else
    storeUdfJavaClasses();
    decompileUdfJavaClasses();
    auto javaUdfSources = readJavaUdfSource();
    nlohmann::json jsonFile;
    jsonFile["deviceInfo"] = openCLDeviceInfo;
    jsonFile["functionCode"] = javaUdfDescriptor->getMethodName();
    auto jsonFilePayload = nlohmann::to_string(jsonFile);
    auto url = baseUrl + "/submit";
    cpr::Multipart payload{{"jsonFile", jsonFilePayload},
                           {"codeFile", javaUdfSources}};
    NES_DEBUG("Submitting request to ELEGANT acceleration service; url = {}, jsonFile = {}, codeFile = {}", url.c_str(), nlohmann::to_string(jsonFile), javaUdfSources);
    cpr::Response response = cpr::Post(url, cpr::Header{{"Content-Type", "multipart/form-data"}}, payload, timeout);
#endif
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
}

const std::string ElegantAccelerationServiceClient::executeStateRequest(const unsigned requestId) const {
    auto url = baseUrl + "/" + std::to_string(requestId) + "/state";
    NES_DEBUG("Submitting request to ELEGANT acceleration service; url = {}", url.c_str());
    auto response = cpr::Get(url, timeout);
    if (response.status_code != 200) {
        NES_DEBUG("ELEGANT acceleration service returned error response; status_code = {}, message = {}",
                  response.status_code,
                  response.text);
        throw new ElegantServiceException("Retrieving ELEGANT acceleration service request state failed");
    }
    return response.text;
}

const std::string ElegantAccelerationServiceClient::executeRetrieveRequest(const unsigned requestId) const {
    auto url = baseUrl + "/" + std::to_string(requestId) + "/retrieve";
    NES_DEBUG("Submitting request to ELEGANT acceleration service; url = {}", url.c_str());
    auto response = cpr::Get(url, timeout);
    if (response.status_code != 200) {
        NES_DEBUG("ELEGANT acceleration service returned error response; status_code = {}, message = {}",
                  response.status_code,
                  response.text);
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
        NES_DEBUG("ELEGANT acceleration request is not completed, retrying; state = {}; tries = {}, delay = {}",
                  state,
                  tries,
                  delay);
        sleep(delay);
        state = executeStateRequest(requestId);
        delay *= 2;
    }
    if (state != completed) {
        NES_DEBUG("ELEGANT acceleration request did not complete after retries; tries = {}, state = {}", tries, state);
        throw new ElegantServiceException("ELEGANT acceleration service did not complete request");
    }
}

}// NES::ELEGANT