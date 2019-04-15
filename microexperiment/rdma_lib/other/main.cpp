//
// Created by Toso, Lorenzo on 2018-11-14.
//
#include <memory>
#include <iostream>
#include <fstream>
#include <string>
#include <sys/mman.h>
#include <json11/json11.hpp>


#include "MPIHelper.h"
#include "NumaUtilities.h"
#include "TableGenerator.h"
#include "Debug.h"
#include "JoinParameters.h"
#include "MathUtils.h"
#include "ComputationParameters.h"
#include "ConnectionInfoProvider/RankfileInfoProvider.h"

#include "AbstractJoins/AbstractJoin.h"
#include "SingleNodeJoin/CPU/SingleCPUJoin.h"
#include "SingleNodeJoin/CPU/MultithreadedCPUJoin.h"
#include "SingleNodeJoin/GPU/SingleGPUJoin.h"
#include "SingleNodeJoin/GPU/UnifiedMemoryGPUJoin.h"
#include "GlobalJoin/ShipAll/ShipAllJoin.h"
#include "GlobalJoin/ShipAll/ShipAllGPUJoin.h"
#include "GlobalJoin/ShipPerfect/ShipPerfectJoin.h"
#include "GlobalJoin/ShipPerfect/ShipPerfectGPUJoin.h"
#include "GlobalJoin/ShipNone/ShipNoneJoin.h"
#include "GlobalJoin/ShipNone/ShipNoneGPUJoin.h"
#include "ETHJoins/ETHNoPartitioningJoin.h"
#include "ETHJoins/ETHRadixJoin.h"
#include "ETHJoins/ETHParallelRadixJoinHistogrambased.h"
#include "ETHJoins/ETHParallelRadixJoinHistogrambasedOptimized.h"
#include "ETHJoins/ETHParallelRadixJoinOptimized.h"
#include "ETHJoins/ETHNoPartitioningJoinSingleThreaded.h"
#include "SingleNodeJoin/CPU/MtCpuEthJoin.h"
#include "GlobalJoin/ShipAll/ShipAll2GPUJoin.h"
#include "GlobalJoin/ShipPerfect/ShipPerfect2GPUJoin.h"

void print_to_file(const std::string &result_path, ComputationParameters &parameters, std::vector<ComputationResult> &results);

void set_additional_parameters(ComputationParameters &parameters) {
    parameters.this_rank = MPIHelper::get_rank();
    parameters.build_tuple_count = BUILD_TUPLES;
    parameters.probe_tuple_count = PROBE_TUPLES;
    parameters.join_selectivity = SELECTIVITY;
    parameters.b_payload_size = sizeof(BPayload);
    parameters.p_payload_size = sizeof(PPayload);
    parameters.move_all_tuples_buffer_count = MOVE_ALL_TUPLES_BUFFER_COUNT;
    parameters.move_all_tuples_buffer_size = MOVE_ALL_TUPLES_BUFFER_SIZE;
    parameters.join_send_buffer_count = JOIN_SEND_BUFFER_COUNT;
    parameters.join_send_buffer_size = JOIN_SEND_BUFFER_SIZE;

    parameters. gpu_join_build_sub_iteration_count = GPU_JOIN_BUILD_SUB_ITERATION_COUNT;
    parameters. gpu_join_build_block_count = GPU_JOIN_BUILD_BLOCK_COUNT;
    parameters. gpu_join_build_thread_count = GPU_JOIN_BUILD_THREAD_COUNT;
    parameters. gpu_join_probe_sub_iteration_count = GPU_JOIN_PROBE_SUB_ITERATION_COUNT;
    parameters. gpu_join_probe_block_count = GPU_JOIN_PROBE_BLOCK_COUNT;
    parameters. gpu_join_probe_thread_count = GPU_JOIN_PROBE_THREAD_COUNT;
}

void write_to_file(const std::string &out_path, const Json & json) {
    auto computation_name = json["Parameters"]["name"].string_value();
    auto timestamp = TimeTools::now() / 1000000;
    auto full_name = computation_name + "_" + std::to_string(MPIHelper::get_rank()) + "_" + std::to_string(timestamp) + ".json";

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


void randomized_result_tuple_check(const std::vector<ResultTable> & resultTables) {
    if (resultTables.empty())
        return;

    const size_t random_tuple_count = 10000;

    std::mt19937 gen(PROBE_SEED); //Standard mersenne_twister_engine seeded with rd()
    std::uniform_int_distribution<size_t> table_index_dist(0, resultTables.size()-1);
    std::uniform_int_distribution<size_t> p_payload_dist(0, sizeof(PPayload)-1);
    std::uniform_int_distribution<size_t> b_payload_dist(0, sizeof(BPayload)-1);


    for(size_t i = 0; i < random_tuple_count; i++){
        auto result_table_index = table_index_dist(gen);

        const ResultTable & table = resultTables[result_table_index];

        if(table.size() == 0)
            continue;

        auto result_tuple_index = (table_index_dist(gen) * table.size()) % table.size();


        unsigned long p_payload_index = p_payload_dist(gen);
        unsigned long b_payload_index = b_payload_dist(gen);
        if(table.p_payloads[result_tuple_index][p_payload_index] != (char)table.keys[result_tuple_index])
        {
            QUICK_PRINT("FOUND A WRONG RESULT TUPLE!\n");
            return;
        }
        if(table.b_payloads[result_tuple_index][b_payload_index] != (char)table.keys[result_tuple_index])
        {
            QUICK_PRINT("FOUND A WRONG RESULT TUPLE!\n");
            return;
        }
    }
}

void print_results(std::vector<ComputationResult> &vector) {
    for (EACH_MEASURED_TIME) {
        std::vector<double> t;
        for (auto &res : vector) {
            t.push_back(res[time]);
        }
        auto median = Math::median(t.begin(), t.end());
        printf("%s: %lums\n", MeasuredTimes::to_string(time).c_str(), static_cast<size_t>(median) / 1000000);
    }
    printf("Tuple count: %lu (%.2f%% of the probe tuples matched)\n", vector[0].get_result_count(), vector[0].get_result_count() / ((double) PROBE_TUPLES) * 100);
    printf("----------------\n");
    fflush(stdout);
}


int run(const Rankfile &rankfile, const std::string &result_path) {

    size_t rank = MPIHelper::get_rank();
    MPIHelper::print_node_info();

    BuildTable buildTable;
    ProbeTable probeTable;

    if (rank == 0) {
#ifdef DEBUG
        Timestamp start = TimeTools::now();
#endif
        QUICK_PRINT("Generating tables...\n");
        size_t max_key;
        buildTable = TableGenerator::generate_build_table(BUILD_TUPLES, SELECTIVITY, max_key);
        probeTable = TableGenerator::generate_probe_table_uniform(PROBE_TUPLES, max_key);
        QUICK_PRINT("Done generating tables!\n");


        double build_table_size_in_gb = (BUILD_TUPLES * sizeof(BuildTuple)) / 1024.0 / 1024.0 / 1024.0;
        double probe_table_size_in_gb = (PROBE_TUPLES * sizeof(ProbeTuple)) / 1024.0 / 1024.0 / 1024.0;

        QUICK_PRINT("Build-Table: %lu tuples (%fGB)\n", size_t(BUILD_TUPLES), build_table_size_in_gb);
        QUICK_PRINT("Probe-Table: %lu tuples (%fGB)\n", size_t(PROBE_TUPLES), probe_table_size_in_gb);
        double total_size_in_gb = build_table_size_in_gb + probe_table_size_in_gb;
        QUICK_PRINT("Total: %fGb\n", total_size_in_gb);
#ifdef DEBUG
        Timestamp end = TimeTools::now();
        double time_in_seconds = (end - start) / 1000000000.0;
        QUICK_PRINT("Generating took %f seconds at %fGB/s!\n", time_in_seconds, total_size_in_gb / time_in_seconds)
#endif
    } else {
        buildTable.resize(BUILD_TUPLES);
        probeTable.resize(PROBE_TUPLES);
    }


    QUICK_PRINT("Starting connections.\n");
    ConnectionCollection connections;

    for (size_t i = 0; i < MPIHelper::get_process_count(); i++) {
        if (rank == i)
            continue;

        RankfileInfoProvider r(rankfile, i);
        connections.add_connection_to(r);
    }
    QUICK_PRINT("All connections initialized.\n");

    QUICK_PRINT("Starting to broadcast tables\n");
    connections.bcast(buildTable.keys.data(), buildTable.size() * sizeof(typeof(buildTable.keys[0])), 0);
    connections.bcast(buildTable.payloads.data(), buildTable.size() * sizeof(typeof(buildTable.payloads[0])), 0);
    connections.bcast(probeTable.keys.data(), probeTable.size() * sizeof(typeof(probeTable.keys[0])), 0);
    connections.bcast(probeTable.payloads.data(), probeTable.size() * sizeof(typeof(probeTable.payloads[0])), 0);
    //connections.bcast(probeTable.selection_keys.data(), probeTable.size() * sizeof(typeof(probeTable.selection_keys[0])), 0);
    QUICK_PRINT("Broadcast ended\n");

    QUICK_PRINT("Done initializing\n");


    std::vector<AbstractJoin*> joinTypes = {
            JOIN_TYPES
    };

    for (auto &join : joinTypes) {
        auto parameters = join->get_parameters();
        set_additional_parameters(parameters);

        std::vector<ComputationResult> results(ITERATIONS);
        QUICK_PRINT("------%s-------\n", parameters.name.c_str());

        for (int i = 0; i < ITERATIONS; i++) {
            connections.barrier_all();
            QUICK_PRINT("------ITERATION %d------\n", i);
            results[i] = join->execute(buildTable, probeTable);
            if(MPIHelper::get_rank() == 0)
            {
                 randomized_result_tuple_check(results[i].result_tables);
                 results[i].clean();
            }
            connections.barrier_all();
            fflush(stdout);
        }

        print_to_file(result_path, parameters, results);
        print_results(results);
    }
    for (auto &join : joinTypes) {
        delete join;
    }
    return 0;
}

void print_to_file(const std::string &result_path, ComputationParameters &parameters, std::vector<ComputationResult> &results) {
    Json::object run;
    std::vector<Json> jsonResults;
    transform(results.begin(), results.end(), back_inserter(jsonResults),
                       [](auto &r) { return r.to_json(); });
    run["Results"] = jsonResults;
    run["Parameters"] = parameters.to_json();
    write_to_file(result_path, run);
}


int main(int argc, char** argv) {
    if (argc < 4) {
        QUICK_PRINT("Missing parameter!\n");
        QUICK_PRINT("Usage: %s <RANK> <RANKFILE> <Output path>\n", argv[0]);
        return -1;
    }

    size_t rank = static_cast<size_t>( std::stoi(argv[1]));

    std::string rankfile_path = std::string(argv[2]);
    QUICK_PRINT("Loading Rankfile.\n");
    fflush(stdout);

    auto rankfile = Rankfile::load(rankfile_path);
    QUICK_PRINT("Rankfile loaded successfully.\n");

    if (rankfile[rank].numa_node > 1) {
        QUICK_PRINT("Invalid numa node %d!\n", rankfile[rank].numa_node);
        exit(1);
    }

    if (rankfile[rank].host_name != MPIHelper::get_host_name()) {
        QUICK_PRINT("Something is wrong with the parameters/rankfile! hostname does not match!\n");
        exit(1);
    }

    pin_to_numa(rankfile[rank].numa_node);
    //mlockall(MCL_CURRENT | MCL_FUTURE);

    MPIHelper::set_rank(rank);
    MPIHelper::set_process_count(rankfile.process_count());
    std::string result_path = argv[3];
    run(rankfile, result_path);
}