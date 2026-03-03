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

#pragma once
#include "SystestState.hpp"

#include "k8s_c_api.h"
#include <string>
#include <unordered_map>
#include <nlohmann/json_fwd.hpp>
#include <filesystem>

namespace NES::Systest {

class K8sJSONSubmitter {
public:
    explicit K8sJSONSubmitter(std::string kubeNamespace = "default",
                              std::string basePath = "",
                              std::string clientCert = "",
                              std::string clientKey = "",
                              std::string caCert = "",
                              std::string bearerToken = "");
    ~K8sJSONSubmitter();

    K8sJSONSubmitter(const K8sJSONSubmitter&) = delete;
    K8sJSONSubmitter& operator=(const K8sJSONSubmitter&) = delete;

    K8sJSONSubmitter(K8sJSONSubmitter&& other) noexcept;
    K8sJSONSubmitter& operator=(K8sJSONSubmitter&& other) noexcept;

    void submitJson(const nlohmann::json& yaml);

    void ensurePVCExists(const std::string& pvcName, const std::string& storageSize = "1Gi");

    /// Write source data files into a PVC via a temporarily writer pod
    void writeSourceDataToPVC(const std::string& pvcName,
                              const std::unordered_map<std::string, std::string>& fileData);

    /// Fetch stdout logs from a worker pod identified by label selector
    std::string fetchPodLogs(const std::string& labelSelector);

    bool waitForQueryCompletion(const std::string& queryName, int timeoutSeconds = 300);

    /// Delete a custom resource (topology or query) to clean up after a test
    void deleteCustomObject(const std::string& kind, const std::string& name);

    /// Write query result from K8s pod logs to the local result file path
    static void writeResultFile(const SystestQuery& query, const std::string& podLogs);

    static K8sJSONSubmitter createForMinikube(std::string kubeNamespace = "default");

private:
    void initClient();

    /// Setup bearer token authentication
    void setupBearerTokenAuth();

    /// Setup mutual TLS authentication
    void setupMTLSAuth();

    /// Perform an HTTP request via curl using the client's auth/TLS settings
    std::pair<long, std::string> curlRequest(const std::string& method,
                                              const std::string& url,
                                              const std::string& body = "",
                                              const std::string& contentType = "application/json");

    /// Delete a pod by name
    void deletePod(const std::string& podName);

    /// Base64-encode a string (for passing file contents to writer pods)
    static std::string base64Encode(const std::string& input);

    std::string kubeNamespace;
    std::string basePath;
    std::string clientCert;
    std::string clientKey;
    std::string caCert;
    std::string bearerToken;
    apiClient_t *client;
};

}
