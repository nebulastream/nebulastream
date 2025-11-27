#pragma once
#include "SystestState.hpp"
#include <string>

namespace NES::Systest {

class K8sYAMLSubmitter {
public:
    explicit K8sYAMLSubmitter(std::string kubeNamespace = "default");

    void applyYaml(const std::string& yaml,
                   const std::string& filename) const;

    void submitQuery(const SystestQuery& q) const;
    void submitTopology(const SystestQuery& q) const;

private:
    std::string kubeNamespace;
};

}
