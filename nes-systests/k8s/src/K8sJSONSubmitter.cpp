#include "K8sJSONSubmitter.hpp"
#include "k8s_c_api.h"
#include <cstdlib>
#include <cstring>
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
/// Helper to get an env var or return a default value.
std::string getEnvOr(const char* name, const std::string& defaultValue)
{
    if (const char* val = std::getenv(name)) {
        return val;
    }
    return defaultValue;
}

/// Check that a file exists and is readable; throw with a helpful message if not.
void requireReadableFile(const std::string& path, const char* description)
{
    std::ifstream f(path);
    if (!f.good()) {
        throw std::runtime_error(
            std::string("Kubernetes SSL ") + description + " not readable at '" + path + "'. "
            "If running inside a Docker container, consider using bearer token auth instead "
            "(set NES_K8S_BEARER_TOKEN). See scripts/setup-k8s-systest-sa.sh.");
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
    std::cerr << "[K8sJSONSubmitter::dtor] this=" << (void*)this
              << " client=" << (void*)client << "\n";
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
    std::cerr << "[K8sJSONSubmitter::move-ctor] from=" << (void*)&other << " to=" << (void*)this
              << " client=" << (void*)client << "\n";
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

    /// Use apiClient_create() and manually set fields — matches the pattern from the
    /// working POC and avoids potential issues with apiClient_create_with_base_path.
    client = apiClient_create();
    if (client == nullptr) {
        throw std::runtime_error("Failed to create Kubernetes API client");
    }

    /// Override basePath (apiClient_create sets it to "http://localhost").
    free(client->basePath);
    const char* path = basePath.empty() ? "http://localhost:8080" : basePath.c_str();
    client->basePath = strdup(path);

    if (!bearerToken.empty()) {
        /// Token-based auth: build the Authorization header on the client directly.
        std::string headerValue = "Bearer " + bearerToken;
        list_t* apiKeys = list_createList();
        keyValuePair_t* pair = keyValuePair_create(
            strdup("Authorization"),
            strdup(headerValue.c_str())
        );
        list_addElement(apiKeys, pair);
        client->apiKeys_BearerToken = apiKeys;

        /// Optional: CA cert for TLS verification. Skip verification if unavailable.
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
    } else if (!clientCert.empty() && !clientKey.empty() && !caCert.empty()) {
        /// Client certificate auth (mTLS): validate that all cert files exist.
        requireReadableFile(clientCert, "client certificate");
        requireReadableFile(clientKey,  "client key");
        requireReadableFile(caCert,     "CA certificate");
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

    std::cerr << "[K8sJSONSubmitter::initClient] this=" << (void*)this << "\n";
    std::cerr << "[K8sJSONSubmitter::initClient] client=" << (void*)client << "\n";
    std::cerr << "[K8sJSONSubmitter::initClient] client->basePath=" << (client->basePath ? client->basePath : "NULL") << "\n";
    std::cerr << "[K8sJSONSubmitter::initClient] client->sslConfig=" << (void*)client->sslConfig << "\n";
    std::cerr << "[K8sJSONSubmitter::initClient] client->apiKeys_BearerToken=" << (void*)client->apiKeys_BearerToken << "\n";
    std::cerr << "[K8sJSONSubmitter::initClient] bearerToken.empty()=" << bearerToken.empty() << "\n";
}

K8sJSONSubmitter K8sJSONSubmitter::createForMinikube(std::string kubeNamespace)
{
    /// Preferred: bearer token auth (works in Docker containers without file mounts).
    const char* tokenEnv = std::getenv("NES_K8S_BEARER_TOKEN");
    std::string apiServer = getEnvOr("NES_K8S_API_URL", "https://192.168.49.2:8443");

    if (tokenEnv != nullptr && std::strlen(tokenEnv) > 0) {
        std::string token(tokenEnv);
        std::string caPath = getEnvOr("NES_K8S_CA_CERT", "");
        return K8sJSONSubmitter(std::move(kubeNamespace), std::move(apiServer),
                                "", "", std::move(caPath), std::move(token));
    }

    /// Fallback: client certificate auth (requires ~/.minikube on the filesystem).
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

    std::cerr << "[K8sJSONSubmitter::submitJson] this=" << (void*)this << "\n";
    std::cerr << "[K8sJSONSubmitter::submitJson] plural=" << plural << "\n";
    std::cerr << "[K8sJSONSubmitter::submitJson] client=" << (void*)client << "\n";
    std::cerr << "[K8sJSONSubmitter::submitJson] client->basePath=" << (client->basePath ? client->basePath : "NULL") << "\n";
    std::cerr << "[K8sJSONSubmitter::submitJson] client->sslConfig=" << (void*)client->sslConfig << "\n";
    std::cerr << "[K8sJSONSubmitter::submitJson] client->apiKeys_BearerToken=" << (void*)client->apiKeys_BearerToken << "\n";

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

void K8sJSONSubmitter::patchSourceDataConfigMap(const std::string& configMapName,
                                                 const std::unordered_map<std::string, std::string>& fileData)
{
    if (fileData.empty()) {
        std::cerr << "[K8sJSONSubmitter::patchSourceDataConfigMap] No source data to patch, skipping.\n";
        return;
    }

    /// ConfigMaps are limited to 1 MiB total. Fail early with a clear message
    /// instead of getting a cryptic 422 from the API server.
    /// TODO: For large source files, switch to a PVC-based approach.
    static constexpr std::size_t CONFIG_MAP_MAX_BYTES = 1024 * 1024;
    std::size_t totalBytes = 0;
    for (const auto& [filename, contents] : fileData) {
        totalBytes += filename.size() + contents.size();
    }
    if (totalBytes > CONFIG_MAP_MAX_BYTES) {
        throw std::runtime_error(
            "Source data (" + std::to_string(totalBytes) + " bytes) exceeds the 1 MiB ConfigMap limit. "
            "Consider using a PersistentVolumeClaim instead of a ConfigMap for large source files.");
    }

    /// Build a strategic merge patch JSON: { "data": { "<filename>": "<contents>", ... } }
    /// NOTE: We use read + replace instead of patch because the kubernetes C client
    /// sends multiple Content-Type headers for PATCH requests, which causes a 400
    /// from the K8s API server.

    /// Step 1: Read the existing ConfigMap (operator must have created it).
    v1_config_map_t* existing = CoreV1API_readNamespacedConfigMap(
        client,
        (char*)configMapName.c_str(),
        (char*)kubeNamespace.c_str(),
        NULL  // pretty
    );

    if (existing == nullptr || client->response_code < 200 || client->response_code >= 300) {
        auto msg = std::string("Failed to read ConfigMap '") + configMapName
            + "' in namespace '" + kubeNamespace
            + "'. HTTP response code: " + std::to_string(client->response_code)
            + ". Make sure the operator has created this ConfigMap before the systest writes to it.";
        if (existing != nullptr) {
            v1_config_map_free(existing);
        }
        throw std::runtime_error(msg);
    }

    /// Step 2: Merge our source file data into the ConfigMap's data field.
    if (existing->data == nullptr) {
        existing->data = list_createList();
    }
    for (const auto& [filename, contents] : fileData) {
        keyValuePair_t* pair = keyValuePair_create(
            strdup(filename.c_str()),
            strdup(contents.c_str())
        );
        list_addElement(existing->data, pair);
    }

    /// Step 3: Replace (PUT) the ConfigMap with the merged data.
    v1_config_map_t* result = CoreV1API_replaceNamespacedConfigMap(
        client,
        (char*)configMapName.c_str(),
        (char*)kubeNamespace.c_str(),
        existing,
        NULL,  // pretty
        NULL,  // dryRun
        NULL,  // fieldManager
        NULL   // fieldValidation
    );

    v1_config_map_free(existing);

    if (result == nullptr || client->response_code < 200 || client->response_code >= 300) {
        auto msg = std::string("Failed to replace ConfigMap '") + configMapName
            + "' in namespace '" + kubeNamespace
            + "'. HTTP response code: " + std::to_string(client->response_code)
            + ". Make sure the operator has created this ConfigMap before the systest writes to it.";
        if (result != nullptr) {
            v1_config_map_free(result);
        }
        throw std::runtime_error(msg);
    }

    std::cerr << "[K8sJSONSubmitter::patchSourceDataConfigMap] Successfully updated ConfigMap '"
              << configMapName << "' (HTTP " << client->response_code << ")\n";
    v1_config_map_free(result);
}

std::string K8sJSONSubmitter::fetchPodLogs(const std::string& labelSelector)
{
    /// List pods matching the label selector to find the pod name.
    v1_pod_list_t* podList = CoreV1API_listNamespacedPod(
        client,
        (char*)kubeNamespace.c_str(),
        NULL,   // pretty
        NULL,   // allowWatchBookmarks
        NULL,   // _continue
        NULL,   // fieldSelector
        (char*)labelSelector.c_str(),
        NULL,   // limit
        NULL,   // resourceVersion
        NULL,   // resourceVersionMatch
        NULL,   // sendInitialEvents
        NULL,   // timeoutSeconds
        NULL    // watch
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

    /// Take the first matching pod.
    listEntry_t* firstEntry = (listEntry_t*)podList->items->firstEntry;
    v1_pod_t* pod = (v1_pod_t*)firstEntry->data;
    std::string podName = pod->metadata->name;
    std::cerr << "[K8sJSONSubmitter::fetchPodLogs] Found pod: " << podName << "\n";
    v1_pod_list_free(podList);

    /// Read the pod logs.
    /// We cannot use CoreV1API_readNamespacedPodLog because it sends
    /// Accept: application/yaml which the logs endpoint rejects with HTTP 406.
    /// Instead, build the URL and use a direct curl call through the apiClient.
    std::string logUrl = std::string(client->basePath)
        + "/api/v1/namespaces/" + kubeNamespace + "/pods/" + podName + "/log";
    std::cerr << "[K8sJSONSubmitter::fetchPodLogs] Fetching logs from: " << logUrl << "\n";

    CURL* curl = curl_easy_init();
    if (curl == nullptr) {
        throw std::runtime_error("Failed to init curl for pod log fetch");
    }

    curl_easy_setopt(curl, CURLOPT_URL, logUrl.c_str());
    curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, "GET");
    curl_easy_setopt(curl, CURLOPT_VERBOSE, 1L);

    /// Set up SSL
    if (client->sslConfig != nullptr) {
        if (client->sslConfig->clientCertFile) {
            curl_easy_setopt(curl, CURLOPT_SSLCERT, client->sslConfig->clientCertFile);
        }
        if (client->sslConfig->clientKeyFile) {
            curl_easy_setopt(curl, CURLOPT_SSLKEY, client->sslConfig->clientKeyFile);
        }
        if (client->sslConfig->CACertFile) {
            curl_easy_setopt(curl, CURLOPT_CAINFO, client->sslConfig->CACertFile);
        }
        if (client->sslConfig->insecureSkipTlsVerify == 1) {
            curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER, 0L);
            curl_easy_setopt(curl, CURLOPT_SSL_VERIFYHOST, 0L);
        } else {
            curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER, 1L);
            curl_easy_setopt(curl, CURLOPT_SSL_VERIFYHOST, 2L);
        }
    }

    /// Set up headers: Accept + Authorization
    struct curl_slist* headers = nullptr;
    headers = curl_slist_append(headers, "Accept: */*");

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
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);

    /// Capture response body into a string
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
    /// Poll for query completion using two strategies:
    ///  1. Check the NesQuery CR's status.phase (if the operator sets it).
    ///  2. Fallback: check the nebuli Job's pod phase via CoreV1API.
    ///     The operator creates a Job whose name == queryName, and K8s automatically
    ///     labels its pods with "job-name=<queryName>".
    const int pollIntervalSeconds = 5;
    int elapsed = 0;

    while (elapsed < timeoutSeconds)
    {
        std::this_thread::sleep_for(std::chrono::seconds(pollIntervalSeconds));
        elapsed += pollIntervalSeconds;

        /// ---- Strategy 1: Check the NesQuery CR status.phase ----
        object_t* obj = CustomObjectsAPI_getNamespacedCustomObject(
            client,
            (char*)"nebulastream.com",
            (char*)"v1",
            (char*)kubeNamespace.c_str(),
            (char*)"nes-queries",
            (char*)queryName.c_str()
        );

        std::cerr << "[waitForQueryCompletion] --- poll cycle (elapsed: " << elapsed << "s) ---\n";

        /// ---- Strategy 1: Check the NesQuery CR status.phase ----
        /// NOTE: client->dataReceived is consumed by the CustomObjects API call,
        /// so we must use object_convertToJSON() to get the response back as JSON.
        if (obj != nullptr && client->response_code >= 200 && client->response_code < 300)
        {
            cJSON* cjsonResponse = object_convertToJSON(obj);
            if (cjsonResponse != nullptr) {
                char* rawJson = cJSON_Print(cjsonResponse);
                cJSON_Delete(cjsonResponse);

                if (rawJson != nullptr) {
                    auto responseJson = nlohmann::json::parse(rawJson, nullptr, false);
                    free(rawJson);

                    if (!responseJson.is_discarded() && responseJson.contains("status")
                        && responseJson["status"].is_object()
                        && responseJson["status"].contains("phase"))
                    {
                        std::string phase = responseJson["status"]["phase"].get<std::string>();
                        std::cerr << "[waitForQueryCompletion] CR status.phase='" << phase << "'\n";
                        object_free(obj);

                        if (phase == "Completed" || phase == "Succeeded") {
                            return true;
                        }
                        if (phase == "Failed" || phase == "Error") {
                            return false;
                        }
                        continue;
                    } else {
                        std::cerr << "[waitForQueryCompletion] CR has no status.phase field\n";
                    }
                }
            }
        } else {
            std::cerr << "[waitForQueryCompletion] Strategy 1: GET CR failed"
                      << " (obj=" << (obj != nullptr ? "ok" : "null")
                      << ", HTTP " << client->response_code << ")\n";
        }
        if (obj != nullptr) {
            object_free(obj);
        }

        /// ---- Strategy 2: Check the nebuli Job pod phase ----
        /// The operator creates a Job with name == queryName. K8s automatically labels
        /// the Job's pods with "job-name=<queryName>".
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
            std::cerr << "[waitForQueryCompletion] Strategy 2: found " << podList->items->count
                      << " pod(s) for selector '" << jobPodSelector << "'\n";
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
                std::cerr << "[waitForQueryCompletion] Strategy 2: pod or status is null\n";
            }
        } else {
            std::cerr << "[waitForQueryCompletion] Strategy 2: no pods found for '"
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

void K8sJSONSubmitter::writeResultFile(const SystestQuery& query, const std::string& podLogs)
{
    /// Construct the schema header line in the format expected by loadQueryResult():
    /// "field1:TYPE1,field2:TYPE2,..."
    /// The fields come from sinkOutputSchema which has qualified names like "STREAM$ID".
    std::ostringstream schemaLine;
    if (query.planInfoOrException) {
        bool first = true;
        for (const auto& field : query.planInfoOrException->sinkOutputSchema) {
            if (!first) schemaLine << ",";
            schemaLine << field.name << ":" << magic_enum::enum_name(field.dataType.type);
            first = false;
        }
    }

    /// Write to the result file path.
    auto resultPath = query.resultFile();
    auto parentDir = resultPath.parent_path();
    if (!std::filesystem::exists(parentDir)) {
        std::filesystem::create_directories(parentDir);
    }

    std::ofstream ofs(resultPath);
    if (!ofs.good()) {
        throw std::runtime_error("Failed to open result file for writing: " + resultPath.string());
    }

    /// Write schema header + pod log data.
    /// The pod logs from the Print sink contain CSV rows like "1,2,3".
    /// We need to filter out non-data lines (NES log messages, startup output, etc.)
    ofs << schemaLine.str() << "\n";

    /// Count expected fields from schema for validation.
    const size_t expectedFieldCount = query.planInfoOrException
        ? query.planInfoOrException->sinkOutputSchema.getNumberOfFields()
        : 0;

    std::istringstream logStream(podLogs);
    std::string line;
    int dataLines = 0;
    int skippedLines = 0;
    while (std::getline(logStream, line)) {
        /// Trim trailing \r if present (Windows line endings).
        if (!line.empty() && line.back() == '\r') {
            line.pop_back();
        }
        /// Skip empty lines.
        if (line.empty()) continue;
        /// Skip NES log lines (start with '[' or contain timestamp patterns).
        if (line.front() == '[') { skippedLines++; continue; }
        /// Validate field count: for multi-field schemas, the line must have
        /// exactly (expectedFieldCount - 1) commas.
        if (expectedFieldCount > 1) {
            const auto commaCount = std::count(line.begin(), line.end(), ',');
            if (commaCount != static_cast<long>(expectedFieldCount - 1)) {
                std::cerr << "[writeResultFile] Skipping line with wrong field count ("
                          << (commaCount + 1) << " vs expected " << expectedFieldCount
                          << "): " << line.substr(0, 100) << "\n";
                skippedLines++;
                continue;
            }
        } else {
            /// Single-field schema or unknown: line must start with a digit, minus, or quote.
            bool looksNumeric = (std::isdigit(static_cast<unsigned char>(line.front()))
                                || line.front() == '-'
                                || line.front() == '"');
            if (!looksNumeric) {
                std::cerr << "[writeResultFile] Skipping non-data line: " << line.substr(0, 100) << "\n";
                skippedLines++;
                continue;
            }
        }
        ofs << line << "\n";
        dataLines++;
    }

    ofs.close();
    std::cerr << "[K8sJSONSubmitter::writeResultFile] Wrote " << dataLines << " data lines ("
              << skippedLines << " skipped) to " << resultPath << "\n";
}

}
