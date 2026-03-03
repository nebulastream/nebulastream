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

#include "K8sJSONSubmitter.hpp"
#include "k8s_c_api.h"
#include <filesystem>
#include <fstream>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <nlohmann/json.hpp>
#include <DataTypes/Schema.hpp>
#include <magic_enum/magic_enum.hpp>

namespace NES::Systest {

namespace {
/// Get environment variable or return default value
std::string getEnvOr(const char* name, const std::string& defaultValue)
{
    if (const char* val = std::getenv(name)) {
        return val;
    }
    return defaultValue;
}

/// Configure SSL/TLS settings on curl handle
void setupCurlSSL(CURL* curl, apiClient_t* client)
{
    if (client->sslConfig == nullptr) {
        return;
    }

    if (client->sslConfig->clientCertFile) {
        curl_easy_setopt(curl, CURLOPT_SSLCERT, client->sslConfig->clientCertFile);
    }
    if (client->sslConfig->clientKeyFile) {
        curl_easy_setopt(curl, CURLOPT_SSLKEY, client->sslConfig->clientKeyFile);
    }
    if (client->sslConfig->CACertFile) {
        curl_easy_setopt(curl, CURLOPT_CAINFO, client->sslConfig->CACertFile);
    }

    const bool insecure = client->sslConfig->insecureSkipTlsVerify == 1;
    curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER, insecure ? 0L : 1L);
    curl_easy_setopt(curl, CURLOPT_SSL_VERIFYHOST, insecure ? 0L : 2L);
}

/// Add authorization headers from apiClient
void setupCurlHeaders(CURL* /* curl */, struct curl_slist*& headers, apiClient_t* client)
{
    if (client->apiKeys_BearerToken != nullptr) {
        listEntry_t* tokenEntry = nullptr;
        list_ForEach(tokenEntry, client->apiKeys_BearerToken) {
            keyValuePair_t* kv = static_cast<keyValuePair_t*>(tokenEntry->data);
            if (kv->key != nullptr && kv->value != nullptr) {
                std::string hdr = std::string(static_cast<char*>(kv->key)) + ": "
                    + std::string(static_cast<char*>(kv->value));
                headers = curl_slist_append(headers, hdr.c_str());
            }
        }
    }
}
}

K8sJSONSubmitter::K8sJSONSubmitter(std::string kubeNamespace,
                                   std::string basePath,
                                   std::string clientCert,
                                   std::string clientKey,
                                   std::string caCert,
                                   std::string bearerToken)
    : kubeNamespace(std::move(kubeNamespace))
    , basePath(std::move(basePath))
    , clientCert(std::move(clientCert))
    , clientKey(std::move(clientKey))
    , caCert(std::move(caCert))
    , bearerToken(std::move(bearerToken))
    , client(nullptr)
{
    initClient();
}

K8sJSONSubmitter::~K8sJSONSubmitter() {
    if (client != nullptr) {
        apiClient_free(client);
        client = nullptr;
    }
    apiClient_unsetupGlobalEnv();
}

K8sJSONSubmitter::K8sJSONSubmitter(K8sJSONSubmitter&& other) noexcept
    : kubeNamespace(std::move(other.kubeNamespace))
    , basePath(std::move(other.basePath))
    , clientCert(std::move(other.clientCert))
    , clientKey(std::move(other.clientKey))
    , caCert(std::move(other.caCert))
    , bearerToken(std::move(other.bearerToken))
    , client(other.client)
{
    other.client = nullptr;
}

K8sJSONSubmitter& K8sJSONSubmitter::operator=(K8sJSONSubmitter&& other) noexcept
{
    if (this != &other) {
        if (client != nullptr) {
            apiClient_free(client);
        }
        kubeNamespace = std::move(other.kubeNamespace);
        basePath = std::move(other.basePath);
        clientCert = std::move(other.clientCert);
        clientKey = std::move(other.clientKey);
        caCert = std::move(other.caCert);
        bearerToken = std::move(other.bearerToken);
        client = other.client;
        other.client = nullptr;
    }
    return *this;
}

void K8sJSONSubmitter::initClient()
{
    apiClient_setupGlobalEnv();

    client = apiClient_create();
    if (client == nullptr) {
        throw std::runtime_error("Failed to create Kubernetes API client");
    }

    free(client->basePath);
    const char* path = basePath.empty() ? "http://localhost:8080" : basePath.c_str();
    client->basePath = strdup(path);

    if (!bearerToken.empty()) {
        setupBearerTokenAuth();
    } else if (!clientCert.empty() && !clientKey.empty() && !caCert.empty()) {
        setupMTLSAuth();
    }
}

void K8sJSONSubmitter::setupBearerTokenAuth()
{
    /// Token-based auth: build the Authorization header
    std::string headerValue = "Bearer " + bearerToken;
    list_t* apiKeys = list_createList();
    keyValuePair_t* pair = keyValuePair_create(
        strdup("Authorization"),
        strdup(headerValue.c_str())
    );
    list_addElement(apiKeys, pair);
    client->apiKeys_BearerToken = apiKeys;

    /// Optional: CA cert for TLS verification
    if (!caCert.empty()) {
        std::ifstream f(caCert);
        if (f.good()) {
            client->sslConfig = sslConfig_create(nullptr, nullptr, caCert.c_str(), 0);
        } else {
            client->sslConfig = sslConfig_create(nullptr, nullptr, nullptr, 1);
        }
    } else {
        client->sslConfig = sslConfig_create(nullptr, nullptr, nullptr, 1);
    }
}

void K8sJSONSubmitter::setupMTLSAuth()
{
    /// Client certificate auth (mTLS)
    client->sslConfig = sslConfig_create(
        clientCert.c_str(),
        clientKey.c_str(),
        caCert.c_str(),
        0
    );
    if (client->sslConfig == nullptr) {
        throw std::runtime_error("Failed to create SSL config for Kubernetes API client");
    }
}

K8sJSONSubmitter K8sJSONSubmitter::createForMinikube(std::string kubeNamespace)
{
    /// Preferred: bearer token auth (works in Docker containers without file mounts)
    const char* tokenEnv = std::getenv("NES_K8S_BEARER_TOKEN");
    std::string apiServer = getEnvOr("NES_K8S_API_URL", "https://192.168.49.2:8443");

    if (tokenEnv != nullptr && std::strlen(tokenEnv) > 0) {
        std::string token(tokenEnv);
        std::string caPath = getEnvOr("NES_K8S_CA_CERT", "");
        return K8sJSONSubmitter(std::move(kubeNamespace), std::move(apiServer),
                                "", "", std::move(caPath), std::move(token));
    }

    /// Fallback: client certificate auth (requires ~/.minikube on the filesystem)
    const char* home = std::getenv("HOME");
    if (home == nullptr) {
        throw std::runtime_error(
            "HOME environment variable not set, cannot locate minikube certs. "
            "Consider using bearer token auth instead (set NES_K8S_BEARER_TOKEN). "
            "See scripts/setup-k8s-systest-sa.sh.");
    }

    std::string homeStr(home);
    std::string certPath = getEnvOr("NES_K8S_CLIENT_CERT", homeStr + "/.minikube/profiles/minikube/client.crt");
    std::string keyPath  = getEnvOr("NES_K8S_CLIENT_KEY",  homeStr + "/.minikube/profiles/minikube/client.key");
    std::string caPath   = getEnvOr("NES_K8S_CA_CERT",     homeStr + "/.minikube/ca.crt");


    return K8sJSONSubmitter(std::move(kubeNamespace), std::move(apiServer),
                            std::move(certPath), std::move(keyPath), std::move(caPath));
}


void K8sJSONSubmitter::submitJson(const nlohmann::json& root)
{
    const std::string jsonString = root.dump();
    cJSON* cjson = cJSON_Parse(jsonString.c_str());

    if (cjson == nullptr) {
        throw std::runtime_error("Failed to parse JSON string to cJSON");
    }

    object_t* body = object_parseFromJSON(cjson);
    cJSON_Delete(cjson);

    if (body == nullptr) {
        throw std::runtime_error("Failed to parse cJSON to object_t");
    }

    char* group = (char*)"nebulastream.com";
    char* version = (char*)"v1";
    char* ns = (char*)kubeNamespace.c_str();

    /// Determine the plural resource name from the JSON "kind" field.
    std::string pluralStr = "nes-topologies"; // default
    if (root.contains("kind")) {
        const auto& kind = root["kind"].get<std::string>();
        if (kind == "NesQuery") {
            pluralStr = "nes-queries";
        } else if (kind == "NesTopology") {
            pluralStr = "nes-topologies";
        }
    }
    char* plural = (char*)pluralStr.c_str();

    object_t* result = CustomObjectsAPI_createNamespacedCustomObject(
        client,
        group,
        version,
        ns,
        plural,
        body,
        NULL,
        NULL,
        NULL,
        NULL
    );

    object_free(body);

    if (result == nullptr || client->response_code < 200 || client->response_code >= 300) {
        auto msg = std::string("Failed to create namespaced custom object at '")
            + (client->basePath ? client->basePath : "unknown")
            + "'. HTTP response code: "
            + std::to_string(client->response_code);
        if (result != nullptr) {
            object_free(result);
        }
        throw std::runtime_error(msg);
    }
    object_free(result);
}

std::pair<long, std::string> K8sJSONSubmitter::curlRequest(const std::string& method,
                                                            const std::string& url,
                                                            const std::string& body,
                                                            const std::string& contentType)
{
    CURL* curl = curl_easy_init();
    if (curl == nullptr) {
        throw std::runtime_error("Failed to init curl for " + method + " " + url);
    }

    curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, method.c_str());

    if (!body.empty()) {
        curl_easy_setopt(curl, CURLOPT_POSTFIELDS, body.c_str());
        curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE, static_cast<long>(body.size()));
    }

    /// Setup SSL/TLS
    setupCurlSSL(curl, client);

    /// Setup headers
    struct curl_slist* headers = nullptr;
    std::string ctHeader = "Content-Type: " + contentType;
    headers = curl_slist_append(headers, ctHeader.c_str());
    headers = curl_slist_append(headers, "Accept: application/json");
    setupCurlHeaders(curl, headers, client);
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);

    /// Response capture
    std::string responseBody;
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION,
        +[](char* ptr, size_t size, size_t nmemb, void* userdata) -> size_t {
            auto* b = static_cast<std::string*>(userdata);
            b->append(ptr, size * nmemb);
            return size * nmemb;
        });
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &responseBody);

    CURLcode res = curl_easy_perform(curl);
    long httpCode = 0;
    curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &httpCode);

    curl_slist_free_all(headers);
    curl_easy_cleanup(curl);

    if (res != CURLE_OK) {
        throw std::runtime_error("curl " + method + " " + url + " failed: "
            + std::string(curl_easy_strerror(res)));
    }

    return {httpCode, responseBody};
}

std::string K8sJSONSubmitter::base64Encode(const std::string& input)
{
    static constexpr const char* TABLE =
        "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

    std::string out;
    out.reserve(((input.size() + 2) / 3) * 4);

    for (std::size_t i = 0; i < input.size(); i += 3) {
        int b0 =                          (unsigned char)input[i];
        int b1 = (i + 1 < input.size()) ? (unsigned char)input[i+1] : 0;
        int b2 = (i + 2 < input.size()) ? (unsigned char)input[i+2] : 0;

        out += TABLE[(b0 >> 2) & 0x3F];
        out += TABLE[((b0 & 0x03) << 4) | ((b1 >> 4) & 0x0F)];
        out += (i + 1 < input.size()) ? TABLE[((b1 & 0x0F) << 2) | ((b2 >> 6) & 0x03)] : '=';
        out += (i + 2 < input.size()) ? TABLE[b2 & 0x3F] : '=';
    }
    return out;
}

void K8sJSONSubmitter::deletePod(const std::string& podName)
{
    std::string url = std::string(client->basePath)
        + "/api/v1/namespaces/" + kubeNamespace + "/pods/" + podName;
    try {
        auto [code, body] = curlRequest("DELETE", url);
        std::cerr << "[K8sJSONSubmitter::deletePod] Deleted pod '" << podName
                  << "' (HTTP " << code << ")\n";
    } catch (const std::exception& e) {
        std::cerr << "[K8sJSONSubmitter::deletePod] Warning: failed to delete pod '"
                  << podName << "': " << e.what() << "\n";
    }
}

void K8sJSONSubmitter::ensurePVCExists(const std::string& pvcName, const std::string& storageSize)
{
    /// Check if PVC already exists
    std::string getUrl = std::string(client->basePath)
        + "/api/v1/namespaces/" + kubeNamespace + "/persistentvolumeclaims/" + pvcName;

    auto [getCode, getBody] = curlRequest("GET", getUrl);
    if (getCode >= 200 && getCode < 300) {
        std::cerr << "[K8sJSONSubmitter::ensurePVCExists] PVC '" << pvcName << "' already exists.\n";
        return;
    }

    /// Create PVC
    nlohmann::json pvc;
    pvc["apiVersion"] = "v1";
    pvc["kind"] = "PersistentVolumeClaim";
    pvc["metadata"]["name"] = pvcName;
    pvc["metadata"]["namespace"] = kubeNamespace;
    pvc["metadata"]["labels"]["topology"] = "nes";
    pvc["spec"]["accessModes"] = nlohmann::json::array({"ReadWriteOnce"});
    pvc["spec"]["resources"]["requests"]["storage"] = storageSize;
    pvc["spec"]["storageClassName"] = "standard";

    std::string createUrl = std::string(client->basePath)
        + "/api/v1/namespaces/" + kubeNamespace + "/persistentvolumeclaims";

    auto [code, body] = curlRequest("POST", createUrl, pvc.dump());

    if (code < 200 || code >= 300) {
        throw std::runtime_error("Failed to create PVC '" + pvcName
            + "'. HTTP " + std::to_string(code) + ". Response: " + body.substr(0, 500));
    }

    std::cerr << "[K8sJSONSubmitter::ensurePVCExists] Created PVC '" << pvcName
              << "' (" << storageSize << ").\n";
}

void K8sJSONSubmitter::writeSourceDataToPVC(const std::string& pvcName,
                                             const std::unordered_map<std::string, std::string>& fileData)
{
    if (fileData.empty()) {
        std::cerr << "[K8sJSONSubmitter::writeSourceDataToPVC] No source data, skipping.\n";
        return;
    }

    static constexpr const char* MOUNT_PATH = "/data";
    static constexpr int POLL_INTERVAL_SEC = 2;
    static constexpr int TIMEOUT_SEC = 120;

    std::cerr << "[K8sJSONSubmitter::writeSourceDataToPVC] Writing " << fileData.size()
              << " file(s) to PVC '" << pvcName << "'...\n";

    /// Build shell script that base64-decodes each file onto the PVC mount.
    std::string script = "set -e\nmkdir -p " + std::string(MOUNT_PATH) + "\n";
    for (const auto& [filename, contents] : fileData) {
        std::string encoded = base64Encode(contents);
        script += "echo '" + encoded + "' | base64 -d > "
               + MOUNT_PATH + "/" + filename + "\n";
    }
    script += "echo '[writer-pod] All files written successfully.'\n";

    /// Unique pod name
    auto now = std::chrono::steady_clock::now().time_since_epoch().count();
    std::string podName = "source-writer-" + std::to_string(now);

    /// Build the Pod JSON
    nlohmann::json pod;
    pod["apiVersion"] = "v1";
    pod["kind"] = "Pod";
    pod["metadata"]["name"] = podName;
    pod["metadata"]["namespace"] = kubeNamespace;
    pod["metadata"]["labels"]["app"] = "source-writer";

    nlohmann::json container;
    container["name"] = "writer";
    container["image"] = "busybox:1.36";
    container["imagePullPolicy"] = "IfNotPresent";
    container["command"] = nlohmann::json::array({"/bin/sh", "-c"});
    container["args"] = nlohmann::json::array({script});
    container["volumeMounts"] = nlohmann::json::array({
        {{"name", "source-data"}, {"mountPath", MOUNT_PATH}}
    });

    pod["spec"]["containers"] = nlohmann::json::array({container});
    pod["spec"]["restartPolicy"] = "Never";
    pod["spec"]["terminationGracePeriodSeconds"] = 3;
    pod["spec"]["volumes"] = nlohmann::json::array({
        {{"name", "source-data"},
         {"persistentVolumeClaim", {{"claimName", pvcName}}}}
    });

    /// Create the writer pod
    std::string createUrl = std::string(client->basePath)
        + "/api/v1/namespaces/" + kubeNamespace + "/pods";

    auto [createCode, createBody] = curlRequest("POST", createUrl, pod.dump());

    if (createCode < 200 || createCode >= 300) {
        throw std::runtime_error("Failed to create writer pod '" + podName
            + "'. HTTP " + std::to_string(createCode)
            + ". Response: " + createBody.substr(0, 500));
    }

    std::cerr << "[K8sJSONSubmitter::writeSourceDataToPVC] Created writer pod '"
              << podName << "', waiting for completion...\n";

    /// Poll until Succeeded / Failed / Timeout
    const int maxAttempts = TIMEOUT_SEC / POLL_INTERVAL_SEC;
    for (int attempt = 0; attempt < maxAttempts; ++attempt) {
        std::this_thread::sleep_for(std::chrono::seconds(POLL_INTERVAL_SEC));

        std::string readUrl = std::string(client->basePath)
            + "/api/v1/namespaces/" + kubeNamespace + "/pods/" + podName;

        auto [readCode, readBody] = curlRequest("GET", readUrl);
        if (readCode < 200 || readCode >= 300) {
            continue; // transient failure
        }

        auto podJson = nlohmann::json::parse(readBody, nullptr, false);
        if (podJson.is_discarded()) continue;

        std::string phase;
        if (podJson.contains("status") && podJson["status"].contains("phase")) {
            phase = podJson["status"]["phase"].get<std::string>();
        }

        if (phase == "Succeeded") {
            std::cerr << "[K8sJSONSubmitter::writeSourceDataToPVC] Writer pod completed successfully.\n";
            deletePod(podName);
            return;
        }
        if (phase == "Failed") {
            deletePod(podName);
            throw std::runtime_error("Writer pod '" + podName + "' failed. "
                "Check logs: kubectl logs " + podName + " -n " + kubeNamespace);
        }

        std::cerr << "[K8sJSONSubmitter::writeSourceDataToPVC] Pod phase: "
                  << (phase.empty() ? "Pending" : phase)
                  << " (attempt " << (attempt + 1) << "/" << maxAttempts << ")\n";
    }

    deletePod(podName);
    throw std::runtime_error("Timed out waiting for writer pod '" + podName
        + "' after " + std::to_string(TIMEOUT_SEC) + "s.");
}

std::string K8sJSONSubmitter::fetchPodLogs(const std::string& labelSelector)
{
    /// List pods matching the label selector
    v1_pod_list_t* podList = CoreV1API_listNamespacedPod(
        client,
        (char*)kubeNamespace.c_str(),
        NULL, NULL, NULL, NULL,
        (char*)labelSelector.c_str(),
        NULL, NULL, NULL, NULL, NULL, NULL
    );

    if (podList == nullptr || client->response_code < 200 || client->response_code >= 300) {
        if (podList) v1_pod_list_free(podList);
        throw std::runtime_error("Failed to list pods with selector '" + labelSelector
            + "'. HTTP code: " + std::to_string(client->response_code));
    }

    if (podList->items == nullptr || podList->items->count == 0) {
        v1_pod_list_free(podList);
        throw std::runtime_error("No pods found matching selector '" + labelSelector + "'");
    }

    /// Extract pod name from first result
    listEntry_t* firstEntry = (listEntry_t*)podList->items->firstEntry;
    v1_pod_t* pod = (v1_pod_t*)firstEntry->data;
    std::string podName = pod->metadata->name;
    std::cerr << "[K8sJSONSubmitter::fetchPodLogs] Found pod: " << podName << "\n";
    v1_pod_list_free(podList);

    /// Build log URL and fetch via curl (direct call avoids HTTP 406 errors)
    std::string logUrl = std::string(client->basePath)
        + "/api/v1/namespaces/" + kubeNamespace + "/pods/" + podName + "/log";
    std::cerr << "[K8sJSONSubmitter::fetchPodLogs] Fetching logs from: " << logUrl << "\n";

    CURL* curl = curl_easy_init();
    if (curl == nullptr) {
        throw std::runtime_error("Failed to init curl for pod log fetch");
    }

    curl_easy_setopt(curl, CURLOPT_URL, logUrl.c_str());
    curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, "GET");
    curl_easy_setopt(curl, CURLOPT_VERBOSE, 0L);

    /// Setup SSL and headers
    setupCurlSSL(curl, client);

    struct curl_slist* headers = nullptr;
    headers = curl_slist_append(headers, "Accept: */*");
    setupCurlHeaders(curl, headers, client);
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);

    /// Capture response
    std::string responseBody;
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION,
        +[](char* ptr, size_t size, size_t nmemb, void* userdata) -> size_t {
            auto* body = static_cast<std::string*>(userdata);
            body->append(ptr, size * nmemb);
            return size * nmemb;
        });
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &responseBody);

    CURLcode res = curl_easy_perform(curl);
    long httpCode = 0;
    curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &httpCode);

    curl_slist_free_all(headers);
    curl_easy_cleanup(curl);

    if (res != CURLE_OK) {
        throw std::runtime_error("curl_easy_perform failed for pod logs: "
            + std::string(curl_easy_strerror(res)));
    }
    if (httpCode < 200 || httpCode >= 300) {
        throw std::runtime_error("Failed to read logs for pod '" + podName
            + "'. HTTP code: " + std::to_string(httpCode)
            + ". Response: " + responseBody.substr(0, 500));
    }

    std::cerr << "[K8sJSONSubmitter::fetchPodLogs] Retrieved " << responseBody.size() << " bytes of logs\n";
    return responseBody;
}

bool K8sJSONSubmitter::waitForQueryCompletion(const std::string& queryName, int timeoutSeconds)
{
    const int pollIntervalSeconds = 5;
    int elapsed = 0;

    while (elapsed < timeoutSeconds)
    {
        std::this_thread::sleep_for(std::chrono::seconds(pollIntervalSeconds));
        elapsed += pollIntervalSeconds;

        /// The operator creates a Job with name == queryName. K8s automatically labels the Job's pods with "job-name=<queryName>".
        std::string jobPodSelector = "job-name=" + queryName;
        v1_pod_list_t* podList = CoreV1API_listNamespacedPod(
            client,
            (char*)kubeNamespace.c_str(),
            NULL, NULL, NULL, NULL,
            (char*)jobPodSelector.c_str(),
            NULL, NULL, NULL, NULL, NULL, NULL
        );

        if (podList != nullptr && client->response_code >= 200 && client->response_code < 300
            && podList->items != nullptr && podList->items->count > 0)
        {
            listEntry_t* entry = podList->items->firstEntry;
            v1_pod_t* pod = static_cast<v1_pod_t*>(entry->data);
            if (pod != nullptr && pod->status != nullptr && pod->status->phase != nullptr)
            {
                std::string podPhase(pod->status->phase);
                std::cerr << "[waitForQueryCompletion] Job pod phase='" << podPhase << "'\n";

                if (podPhase == "Succeeded") {
                    v1_pod_list_free(podList);
                    return true;
                }
                if (podPhase == "Failed") {
                    v1_pod_list_free(podList);
                    return false;
                }
            } else {
                std::cerr << "[waitForQueryCompletion] pod or status is null\n";
            }
        } else {
            std::cerr << "[waitForQueryCompletion] no pods found for '"
                      << jobPodSelector << "' (HTTP " << client->response_code
                      << ", podList=" << (podList != nullptr ? "ok" : "null")
                      << ", items=" << (podList && podList->items ? std::to_string(podList->items->count) : "null")
                      << ")\n";
        }
        if (podList != nullptr) {
            v1_pod_list_free(podList);
        }

        std::cerr << "[waitForQueryCompletion] Query '" << queryName
                  << "' still running... (elapsed: " << elapsed << "s)\n";
    }

    std::cerr << "[waitForQueryCompletion] Timed out waiting for '"
              << queryName << "' after " << timeoutSeconds << "s\n";
    return false;
}

void K8sJSONSubmitter::deleteCustomObject(const std::string& kind, const std::string& name)
{
    std::string pluralStr;
    if (kind == "NesQuery") {
        pluralStr = "nes-queries";
    } else if (kind == "NesTopology") {
        pluralStr = "nes-topologies";
    } else {
        throw std::runtime_error("Unknown kind for deletion: " + kind);
    }

    object_t* result = CustomObjectsAPI_deleteNamespacedCustomObject(
        client,
        (char*)"nebulastream.com",
        (char*)"v1",
        (char*)kubeNamespace.c_str(),
        (char*)pluralStr.c_str(),
        (char*)name.c_str(),
        NULL,  // gracePeriodSeconds
        NULL,  // orphanDependents
        NULL,  // propagationPolicy
        NULL,  // dryRun
        NULL   // body (v1DeleteOptions)
    );

    if (result != nullptr) {
        object_free(result);
    }
    std::cerr << "[K8sJSONSubmitter::deleteCustomObject] Deleted " << kind << "/" << name
              << " (HTTP " << client->response_code << ")\n";
}

namespace {
/// Trim leading/trailing whitespace from a string
std::string trimWhitespaces(std::string s)
{
    const char* ws = " \t\n\r";
    size_t start = s.find_first_not_of(ws);
    if (start == std::string::npos) return {};
    return s.substr(start, s.find_last_not_of(ws) - start + 1);
}

/// Check if all characters are digits
bool allDigits(const std::string& s)
{
    return !s.empty() && std::all_of(s.begin(), s.end(),
        [](unsigned char c) { return std::isdigit(c); });
}

/// Check if token is a valid numeric literal (integer, float)
bool isNumericToken(const std::string& s)
{
    if (s.empty()) return false;
    const char* p = s.c_str();
    if (*p == '-' || *p == '+') ++p;
    if (*p == '\0') return false;

    bool sawDigit = false;
    while (std::isdigit(static_cast<unsigned char>(*p))) { ++p; sawDigit = true; }
    if (*p == '.') { ++p; while (std::isdigit(static_cast<unsigned char>(*p))) { ++p; sawDigit = true; } }
    if (*p == 'e' || *p == 'E') { ++p; if (*p == '+' || *p == '-') ++p;
        while (std::isdigit(static_cast<unsigned char>(*p))) ++p; }

    return sawDigit && *p == '\0';
}

/// Check if a line looks like a NES log or metadata (not data output)
bool isLogOrMetadataLine(const std::string& line)
{
    if (line.empty()) return true;
    if (line.front() == '[') return true;  // NES log line
    if (line.rfind("Pipeline(", 0) == 0) return true;  // Pipeline header
    if (std::isalpha(static_cast<unsigned char>(line.front()))) return true;  // Starts with letter
    return false;
}

/// Parse a data line into tokens
std::vector<std::string> parseDataLine(const std::string& line, size_t expectedFieldCount)
{
    std::vector<std::string> commaToks;
    {
        std::istringstream ss(line);
        std::string tok;
        while (std::getline(ss, tok, ',')) { commaToks.push_back(trimWhitespaces(tok)); }
    }

    if (expectedFieldCount > 0 && commaToks.size() == expectedFieldCount) {
        return commaToks;
    }

    return {};  // No match
}

/// Validate that a token matches its field's expected type
bool isValidFieldValue(const std::string& token, const std::string& fieldType)
{
    if (token.empty()) return false;

    if (fieldType == "UINT64" || fieldType == "UINT32"
        || fieldType == "UINT16" || fieldType == "UINT8") {
        return allDigits(token);
    } else if (fieldType == "INT64" || fieldType == "INT32"
               || fieldType == "INT16" || fieldType == "INT8"
               || fieldType == "FLOAT32" || fieldType == "FLOAT64") {
        return isNumericToken(token);
    }

    return true;  // STRING, BOOLEAN, etc. accept as-is
}
}

void K8sJSONSubmitter::writeResultFile(const SystestQuery& query, const std::string& podLogs)
{
    /// Construct the schema header line in the format expected by loadQueryResult()
    std::ostringstream schemaLine;
    if (query.planInfoOrException) {
        bool first = true;
        for (const auto& field : query.planInfoOrException->sinkOutputSchema) {
            if (!first) schemaLine << ",";
            schemaLine << field.name << ":" << magic_enum::enum_name(field.dataType.type);
            first = false;
        }
    }

    /// Prepare result file for writing
    auto resultPath = query.resultFile();
    auto parentDir = resultPath.parent_path();
    if (!std::filesystem::exists(parentDir)) {
        std::filesystem::create_directories(parentDir);
    }

    std::ofstream ofs(resultPath);
    if (!ofs.good()) {
        throw std::runtime_error("Failed to open result file for writing: " + resultPath.string());
    }

    ofs << schemaLine.str() << "\n";

    /// Extract expected field count and types from schema
    const size_t expectedFieldCount = query.planInfoOrException
        ? query.planInfoOrException->sinkOutputSchema.getNumberOfFields()
        : 0;

    /// Process log lines and extract data
    std::istringstream logStream(podLogs);
    std::string line;
    int dataLines = 0;
    int skippedLines = 0;

    while (std::getline(logStream, line)) {
        /// Normalize line ending
        if (!line.empty() && line.back() == '\r') {
            line.pop_back();
        }
        line = trimWhitespaces(line);

        /// Skip empty and metadata lines
        if (isLogOrMetadataLine(line)) {
            skippedLines++;
            continue;
        }

        /// Parse tokens from line
        std::vector<std::string> tokens = parseDataLine(line, expectedFieldCount);
        if (tokens.empty()) {
            skippedLines++;
            continue;
        }

        /// Validate that each token matches its field type
        bool looksLikeData = true;
        for (size_t i = 0; i < tokens.size(); ++i) {
            const std::string& tok = tokens[i];
            if (tok.empty()) {
                looksLikeData = false;
                break;
            }

            /// Check type validity if schema information is available
            if (query.planInfoOrException && i < expectedFieldCount) {
                auto fieldType = std::string(magic_enum::enum_name(
                    query.planInfoOrException->sinkOutputSchema.getFieldAt(i).dataType.type));
                if (!isValidFieldValue(tok, fieldType)) {
                    looksLikeData = false;
                    break;
                }
            } else {
                /// No schema: just check that token contains at least one digit
                bool hasDigit = std::any_of(tok.begin(), tok.end(),
                    [](unsigned char c) { return std::isdigit(c); });
                if (!hasDigit) {
                    looksLikeData = false;
                    break;
                }
            }
        }

        if (!looksLikeData) {
            skippedLines++;
            continue;
        }

        /// Write CSV row
        for (size_t i = 0; i < tokens.size(); ++i) {
            if (i) ofs << ",";
            ofs << tokens[i];
        }
        ofs << "\n";
        dataLines++;
    }

    ofs.close();
    std::cerr << "[K8sJSONSubmitter::writeResultFile] Wrote " << dataLines << " data lines ("
              << skippedLines << " skipped) to " << resultPath << "\n";
}

}
