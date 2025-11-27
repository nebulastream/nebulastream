#include "K8sJSONSubmitter.hpp"
#include "K8sTopologyBuilder.hpp"
#include "k8s_c_api.h"
#include <fstream>
#include <stdexcept>

namespace NES::Systest {

K8sJSONSubmitter::K8sJSONSubmitter(std::string kubeNamespace)
    : kubeNamespace(std::move(kubeNamespace)) {}

K8sJSONSubmitter::~K8sJSONSubmitter() {
    if (client) {
        apiClient_free(client);
        client = nullptr;
    }
}

void K8sJSONSubmitter::submitJson(const nlohmann::json& root)
{
    client = apiClient_create();
    std::string jsonString = root.dump();

    cJSON* cjson = cJSON_Parse(jsonString.c_str());

    if (!client) {
        throw std::runtime_error("Failed to create Kubernetes API client");
    }
    if (!cjson) {
        throw std::runtime_error("Failed to parse JSON string to cJSON");
    }


    object_t* body = object_parseFromJSON(cjson);
    cJSON_Delete(cjson);

    if (!body) {
        throw std::runtime_error("Failed to parse cJSON to object_t");
    }

    char* group = (char*)"nebulastream.com";
    char* version = (char*)"v1";
    char* ns = (char*)"default";
    char* plural = (char*)"nes-topologies";

    CustomObjectsAPI_createNamespacedCustomObject(
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
}

}
