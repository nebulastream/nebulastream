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

    void patchSourceDataConfigMap(const std::string& configMapName,
                                  const std::unordered_map<std::string, std::string>& fileData);

    /// Create a PVC for source data if it does not already exist.
    void ensurePVCExists(const std::string& pvcName, const std::string& storageSize = "1Gi");

    /// Write source data files into a PVC via a short-lived writer pod.
    /// This avoids the 1 MiB ConfigMap limit and the ~90s propagation delay.
    void writeSourceDataToPVC(const std::string& pvcName,
                              const std::unordered_map<std::string, std::string>& fileData);

    /// Fetch stdout logs from a worker pod identified by label selector.
    /// @param labelSelector e.g. "app=worker-1"
    /// @return the pod log contents as a string
    std::string fetchPodLogs(const std::string& labelSelector);

    /// Poll a NesQuery custom resource until it reaches a terminal phase.
    /// @param queryName the metadata.name of the NesQuery CR
    /// @param timeoutSeconds maximum time to wait
    /// @return true if the query completed successfully, false on failure/timeout
    bool waitForQueryCompletion(const std::string& queryName, int timeoutSeconds = 300);

    /// Delete a custom resource (topology or query) to clean up after a test.
    void deleteCustomObject(const std::string& kind, const std::string& name);

    /// Write query result from K8s pod logs to the local result file path
    /// so that the existing checkResult() / loadQueryResult() logic works.
    static void writeResultFile(const SystestQuery& query, const std::string& podLogs);

    static K8sJSONSubmitter createForMinikube(std::string kubeNamespace = "default");

private:
    void initClient();

    /// Perform an HTTP request via curl using the client's auth/TLS settings.
    /// Returns {httpCode, responseBody}. Throws on curl failure.
    std::pair<long, std::string> curlRequest(const std::string& method,
                                              const std::string& url,
                                              const std::string& body = "",
                                              const std::string& contentType = "application/json");

    /// Delete a pod by name (best-effort, does not throw on failure).
    void deletePod(const std::string& podName);

    /// Base64-encode a string (for passing file contents to writer pods).
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
