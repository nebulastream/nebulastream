//
// Created by Toso, Lorenzo on 2018-11-15.
//
#include "ComputationParameters.h"
#include "MPIHelper.h"
#include "ConnectionCollection.h"


ComputationParameters ComputationParameters::generate(ConnectionCollection & connections, const NodeParameters & node_parameters) {
    ComputationParameters parameters;
    parameters.used_nodes = get_all_node_parameters(connections, node_parameters);
    parameters.process_count = MPIHelper::get_process_count();
    return parameters;
}
ComputationParameters ComputationParameters::generate(VerbsConnection &connection, const NodeParameters &node_parameters) {
    ComputationParameters parameters;
    parameters.used_nodes = get_all_node_parameters(connection, node_parameters);
    parameters.process_count = MPIHelper::get_process_count();
    return parameters;
}


std::vector<NodeParameters> ComputationParameters::get_all_node_parameters(ConnectionCollection & connections, const NodeParameters & node_parameters) {
    size_t process_count = MPIHelper::get_process_count();

    if(process_count == 1)
        return {node_parameters};

    NodeParameters all_parameters[process_count];

    connections.all_gather(&node_parameters, sizeof(NodeParameters), all_parameters);

    std::vector<NodeParameters> vec(process_count);
    for(size_t i = 0; i < process_count; i++){
        vec[i] = all_parameters[i];
    }
    return vec;
}
std::vector<NodeParameters> ComputationParameters::get_all_node_parameters(VerbsConnection &connection, const NodeParameters &node_parameters) {
    size_t rank = MPIHelper::get_rank();
    size_t process_count = MPIHelper::get_process_count();


    NodeParameters all_parameters[process_count];

    if(rank < connection.get_target_rank()){
        memcpy(&(all_parameters[0]), &node_parameters, sizeof(NodeParameters));
        auto buffer = connection.register_buffer(&(all_parameters[1]), sizeof(NodeParameters));
        connection.post_and_receive_blocking(buffer);
        delete buffer;

        std::vector<NodeParameters> vec(2);
        vec[0] = all_parameters[0];
        vec[1] = all_parameters[1];
        return vec;
    }
    else {
        auto buffer = connection.register_buffer(const_cast<NodeParameters*>(&node_parameters), sizeof(NodeParameters));
        connection.send_blocking(buffer);
        delete buffer;
        return {};
    }
}

Json::object ComputationParameters::to_json() {
    Json::object j;
    j["name"] = name;

    Json::array nodes;
    for (auto &used_node : used_nodes)
        nodes.push_back(used_node.to_json());
    j["used_nodes"] = nodes;
    j["process_count"] = (int)process_count;

    j["this_rank"] = (int)this_rank;
    j["build_tuple_count"] = (int)build_tuple_count;
    j["probe_tuple_count"] = (int)probe_tuple_count;
    j["join_selectivity"] = (double)join_selectivity;
    j["b_payload_size"] = (int)b_payload_size;
    j["p_payload_size"] = (int)p_payload_size;
    j["move_all_tuples_buffer_count"] = (int)move_all_tuples_buffer_count;
    j["move_all_tuples_buffer_size"] = (int)move_all_tuples_buffer_size;
    j["join_send_buffer_count"] = (int)join_send_buffer_count;
    j["join_send_buffer_size"] = (int)join_send_buffer_size;

    j["gpu_join_build_sub_iteration_count"] =  (int)gpu_join_build_sub_iteration_count;
    j["gpu_join_build_block_count"] =  (int)gpu_join_build_block_count ;
    j["gpu_join_build_thread_count"] =  (int)gpu_join_build_thread_count ;
    j["gpu_join_probe_sub_iteration_count"] =  (int)gpu_join_probe_sub_iteration_count;
    j["gpu_join_probe_block_count"] =  (int)gpu_join_probe_block_count ;
    j["gpu_join_probe_thread_count"] =  (int)gpu_join_probe_thread_count ;

    return j;
}

ComputationParameters ComputationParameters::generate(const NodeParameters &node_parameters) {
    ComputationParameters parameters;
    parameters.used_nodes = {node_parameters};
    parameters.process_count = 1;
    return parameters;
}


