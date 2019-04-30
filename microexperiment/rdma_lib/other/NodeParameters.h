//
// Created by Toso, Lorenzo on 2019-01-11.
//

#ifndef MAMPI_NODEPARAMETERS_H
#define MAMPI_NODEPARAMETERS_H

#include <cstring>
#include "MPIHelper.h"
#include "json11/json11.hpp"

using json11::Json;


class NodeParameters {
#define MAX_DETAIL_SIZE 255
public:
    NodeParameters() = default;
    NodeParameters(const std::string & details, bool uses_gpu, bool uses_threads)
    : uses_gpu(uses_gpu)
    , uses_threads(uses_threads) {
        int len = MAX_HOST_NAME_SIZE;
        MPIHelper::get_host_name(host_name, len);
        std::strcpy(this->details, details.c_str());
        rank = static_cast<int>(MPIHelper::get_rank());
    }

    int rank = 0;
    char host_name[MAX_HOST_NAME_SIZE] = {0};
    char details[MAX_DETAIL_SIZE] = {0};
    bool uses_gpu = false;
    bool uses_threads = false;

    Json to_json(){
        Json j = Json::object {
                {"rank", rank},
                {"host_name", std::string(host_name)},
                {"details", std::string(details)},
                {"uses_gpu", uses_gpu},
                {"uses_threads", uses_threads},
        };
        return j;
    }
};

#endif //MAMPI_NODEPARAMETERS_H
