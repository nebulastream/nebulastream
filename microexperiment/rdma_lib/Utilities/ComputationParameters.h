//
// Created by Toso, Lorenzo on 2018-11-13.
//

#ifndef MAMPI_COMPUTATIONPARAMETERS_H
#define MAMPI_COMPUTATIONPARAMETERS_H

#include <set>
#include <string>
#include <map>
#include "ConnectionCollection.h"
#include "NodeParameters.h"


class ComputationParameters {
public:
    std::string name;
    std::vector<NodeParameters> used_nodes;
    size_t process_count = 0;
    size_t this_rank = 0;

    size_t build_tuple_count = 0;
    size_t probe_tuple_count = 0;
    double join_selectivity = 0.0;
    size_t b_payload_size = 0;
    size_t p_payload_size = 0;
    size_t move_all_tuples_buffer_count = 0;
    size_t move_all_tuples_buffer_size = 0;
    size_t join_send_buffer_count = 0;
    size_t join_send_buffer_size = 0;
    size_t gpu_join_build_sub_iteration_count = 0;
    size_t gpu_join_build_block_count = 0;
    size_t gpu_join_build_thread_count = 0;
    size_t gpu_join_probe_sub_iteration_count = 0;
    size_t gpu_join_probe_block_count = 0;
    size_t gpu_join_probe_thread_count = 0;


    Json::object to_json();

    static ComputationParameters generate(ConnectionCollection & connections, const NodeParameters & node_parameters);
    static ComputationParameters generate(VerbsConnection & connection, const NodeParameters & node_parameters);
    static ComputationParameters generate(const NodeParameters & node_parameters);
private:
    static std::vector<NodeParameters> get_all_node_parameters(ConnectionCollection & connections, const NodeParameters & node_parameters);
    static std::vector<NodeParameters> get_all_node_parameters(VerbsConnection & connection, const NodeParameters & node_parameters);
};
#endif //MAMPI_COMPUTATIONPARAMETERS_H
