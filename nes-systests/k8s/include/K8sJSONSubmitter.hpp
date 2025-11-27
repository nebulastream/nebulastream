#pragma once
#include "SystestState.hpp"

#include "k8s_c_api.h"
#include <string>
#include <nlohmann/json_fwd.hpp>

namespace NES::Systest {

class K8sJSONSubmitter {
public:
    explicit K8sJSONSubmitter(std::string kubeNamespace = "default");
    ~K8sJSONSubmitter();

    void submitJson(const nlohmann::json& yaml);

private:
    std::string kubeNamespace;
    apiClient_t *client;
};

}
