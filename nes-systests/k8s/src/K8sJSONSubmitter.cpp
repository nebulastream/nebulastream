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

} // anonymous namespace

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

}
