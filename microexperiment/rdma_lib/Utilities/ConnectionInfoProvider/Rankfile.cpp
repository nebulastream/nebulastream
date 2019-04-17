//
// Created by Toso, Lorenzo on 2019-01-18.
//

#include "Rankfile.h"
#include <iostream>
#include <fstream>
#include <regex>
#include <Debug.h>
#include "ConnectionUtilities.h"

Rankfile Rankfile::load(const std::string &path) {
    std::vector<std::string> lines;
    std::ifstream file(path);

    if (file.is_open()) {
        std::string line;
        while (getline(file, line)) {
            if(line.size() <= 1)
                continue;
            lines.push_back(line);
        }
        file.close();
    } else {
        fprintf(stderr, "Could not open rankfile!\n");
        exit(1);
    }


    std::vector<DeviceInfo> infos;
    for(auto & line : lines){

        if(line[0] == '#')
            continue;
        std::smatch sm;
        const std::basic_regex<char> &regex = std::regex(R"(rank (\d+)=([\w-]+) slot=(\d):(\d))");

        if(!std::regex_match(line, sm, regex)){
            fprintf(stderr, "Error parsing line %s\n", line.c_str());
            continue;
        }

        size_t rank = static_cast<size_t>(std::stoi(sm[1].str()));
        std::string hostname = sm[2].str();
        int numa_node = std::stoi(sm[3].str());
        int cpu_index = std::stoi(sm[4].str());


        DeviceInfo info;
        info.rank = rank;
        info.ip = ConnectionUtilities::get_ip(hostname, numa_node, cpu_index);
        info.device_port = 1;
        info.device_index = static_cast<uint16_t>(ConnectionUtilities::get_ib_device_index(numa_node, cpu_index));
        info.numa_node = numa_node;
        info.host_name = hostname;

        TRACE("Found definition for rank %lu on node %s in Rankfile!\n", info.rank, info.host_name.c_str());

        if(infos.size() <= rank){
            infos.resize(rank + 1);
        }
        infos[rank] = info;
    }

    return Rankfile(infos);
}

const DeviceInfo &Rankfile::operator[](int index) const {
    return device_infos[index];
}

DeviceInfo &Rankfile::operator[](int index) {
    return device_infos[index];
}

Rankfile::Rankfile(const std::vector<DeviceInfo> &device_infos) : device_infos(device_infos) {}

size_t Rankfile::process_count() {
    return device_infos.size();
}
