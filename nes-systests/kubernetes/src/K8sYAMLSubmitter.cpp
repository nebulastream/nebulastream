#include "K8sYAMLSubmitter.hpp"
#include "K8sQueryBuilder.hpp"
#include "K8sTopologyBuilder.hpp"

#include <cstdlib>
#include <fstream>
#include <stdexcept>

namespace NES::Systest {

K8sYAMLSubmitter::K8sYAMLSubmitter(std::string kubeNamespace)
    : kubeNamespace(std::move(kubeNamespace)) {}

void K8sYAMLSubmitter::applyYaml(const std::string& yaml,
                           const std::string& filename) const
{
    std::ofstream out(filename);
    if (!out) {
        throw std::runtime_error("Failed to open YAML file: " + filename);
    }
    out << yaml;
    out.close();

    std::string cmd =
        "kubectl apply -n " + kubeNamespace + " -f " + filename;
    std::cout << "applying " << cmd << std::endl;

    if (std::system(cmd.c_str()) != 0) {
        throw std::runtime_error("kubectl apply failed: " + filename);
    }
}

void K8sYAMLSubmitter::submitQuery(const SystestQuery& q) const
{
    std::string yaml = K8sQueryBuilder::toYamlString(q);
    std::string file =
        "/tmp/nes-query-" + q.testName + "-" +
        std::to_string(0) + ".yaml";

    applyYaml(yaml, file);
}

void K8sYAMLSubmitter::submitTopology(const SystestQuery& q) const
{
    std::string yaml = K8sTopologyBuilder::toYamlString(q);
    std::string file =
        "/tmp/nes-topology-" + q.testName + ".yaml";

    applyYaml(yaml, file);
}

}
