//
// Created by Toso, Lorenzo on 2018-12-04.
//
#include "BenchmarkTools.h"
#include "TimeTools.hpp"
#include <iostream>
#include <fstream>

const std::vector<size_t> BenchmarkTools::BUFFER_SIZES =
        {//1, 4, 8, 16, 32, 64, 128, 256, 512,
                //1024, 2048, 3000, 4096, 8192, 10 * KILOBYTE, 16 * KILOBYTE, 20 * KILOBYTE,
         //32 * KILOBYTE, 40 * KILOBYTE, 60 * KILOBYTE, 100 * KILOBYTE, 128 * KILOBYTE,
                //200 * KILOBYTE, 256 * KILOBYTE, 400 * KILOBYTE, 512 * KILOBYTE, 1 * MEGABYTE,
         1500 * KILOBYTE, 3 * MEGABYTE, 5 * MEGABYTE, 16 * MEGABYTE, 32 * MEGABYTE,
         //64 * MEGABYTE, 128 * MEGABYTE//, 200*MEGABYTE, 256*MEGABYTE, 512*MEGABYTE
        };
const std::vector<size_t> BenchmarkTools::BUFFER_COUNTS = {/*1, 2, 8, */32/*, 64, 128, 512*/};

void BenchmarkTools::write_to_file(const std::string &out_path, Json json) {
    auto computation_name = json["Parameters"]["name"].string_value();
    auto timestamp = TimeTools::now() / 1000000;
    auto full_name = computation_name + "_" + std::to_string(timestamp) + ".json";

    std::string full_path;
    if (out_path[out_path.size() - 1] != '/' && out_path[out_path.size() - 1] != '\\') {
        full_path = out_path + '/' + full_name;
    } else {
        full_path = out_path + full_name;
    }
    std::ofstream out_file;
    out_file.open(full_path);
    out_file << json.dump() << std::endl;
    out_file.close();
}

void BenchmarkTools::output_results(const std::vector<size_t> &measured_times, const std::string &out_path,
                    ComputationParameters &parameters, const std::string & name,
                    size_t BUFFER_SIZE, size_t NUMBER_OF_BUFFERS, size_t REPETITIONS_PER_ITERATION) {
    parameters.name = name;

    auto interesting_times = TimeTools::get_interesting_times(measured_times);
    interesting_times.print(1, "ns");




    Json::object j;
    Json::object j_parameters = parameters.to_json();
    j["InterestingResults"] = interesting_times.to_json();
    Json::array a;
    for(auto & t : measured_times)
        a.push_back((double)t);
    j["Results"] = a;
    j_parameters["BUFFER_SIZE"] = (double)BUFFER_SIZE;
    j_parameters["NUMBER_OF_BUFFERS"] = (double)NUMBER_OF_BUFFERS;
    j_parameters["REPETITIONS_PER_ITERATION"] = (double)REPETITIONS_PER_ITERATION;
    j["Parameters"] = j_parameters;

    write_to_file(out_path, j);

}

void BenchmarkTools::output_results(
        std::vector<unsigned long> measured_times,
        const TestParameters &connection_parameters,
        ComputationParameters computationParameters,
        const std::string & name_prefix) {

    auto interesting_times = TimeTools::get_interesting_times(measured_times);
    interesting_times.print(1, "ns");


    std::string name = name_prefix +
            connection_parameters.cpu_or_gpu + "_"
            "L-Numa" + std::to_string(connection_parameters.local_numa) +
            "_L-IB" + std::to_string(connection_parameters.local_ib);

    Json::object j;
    j["InterestingResults"] = interesting_times.to_json();

    Json::array a;
    for(auto & t : measured_times)
        a.push_back((double)t);
    j["Results"] = a;


    Json::object j_parameters = computationParameters.to_json();
    j_parameters["name"] = name;
    j["Parameters"] = j_parameters;

    write_to_file(connection_parameters.result_path, j);
}
