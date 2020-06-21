#include <Monitoring/Util/SystemResourcesReader.hpp>
#include <Util/Logger.hpp>
#include <fstream>
#include <vector>
#include <iterator>

namespace NES {

std::unordered_map<std::string, uint64_t> SystemResourcesReader::CPUStats() {
    std::unordered_map<std::string, uint64_t> output;

    std::ifstream fileStat("/proc/stat");
    std::string line;
    int cpuCount = 0;

    while (std::getline(fileStat, line)) {
        // line starts with "cpu"
        if (!line.compare(0, 3, "cpu")) {
            std::istringstream ss(line);
            std::vector<std::string> tokens{
                std::istream_iterator<std::string>{ss},
                std::istream_iterator<std::string>{}};

            // check columns
            if (tokens.size() != 11) {
                NES_THROW_RUNTIME_ERROR("SystemResourcesReader: /proc/stat incorrect");
            }
            cpuCount++;

            char name[8];
            int len = tokens[0].copy(name, tokens[0].size());
            name[len] = '\0';

            output["user" + std::to_string(cpuCount)] = std::stoul(tokens[1]);
            output["nice" + std::to_string(cpuCount)] = std::stoul(tokens[2]);
            output["system" + std::to_string(cpuCount)] = std::stoul(tokens[3]);
            output["idle" + std::to_string(cpuCount)] = std::stoul(tokens[4]);
            output["iowait" + std::to_string(cpuCount)] = std::stoul(tokens[5]);
            output["irq" + std::to_string(cpuCount)] = std::stoul(tokens[6]);
            output["softIrq" + std::to_string(cpuCount)] = std::stoul(tokens[7]);
            output["steal" + std::to_string(cpuCount)] = std::stoul(tokens[8]);
            output["guest" + std::to_string(cpuCount)] = std::stoul(tokens[9]);
            output["guestNice" + std::to_string(cpuCount)] = std::stoul(tokens[10]);
        }
    }
    output["cpuCount"] = cpuCount-1;
    NES_DEBUG("SystemResourcesReader: CPU stats read for number of CPUs " << output["cpuCount"]);
    return output;
}

}