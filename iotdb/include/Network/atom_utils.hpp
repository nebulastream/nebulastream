//
// Created by xchatziliadis on 13.11.19.
//

#ifndef IOTDB_INCLUDE_NETWORK_ATOM_UTILS_HPP_
#define IOTDB_INCLUDE_NETWORK_ATOM_UTILS_HPP_

#include <string>

#include "caf/all.hpp"

using std::cout;
using std::cerr;
using std::endl;
using std::string;

namespace iotdb {
using namespace caf;

//coordinator stubs
using register_worker_atom = atom_constant<atom("r_worker")>;
using register_sensor_atom = atom_constant<atom("r_sensor")>;
using register_query_atom = atom_constant<atom("register")>;
using deploy_query_atom  = atom_constant<atom("deploy")>;

using topology_json_atom = atom_constant<atom("topology")>;
using show_registered_atom = atom_constant<atom("s_reg")>;
using show_running_atom = atom_constant<atom("s_run")>;
using show_running_operators_atom = atom_constant<atom("s_ops")>;

//worker stubs
using get_local_sink_port_atom  = atom_constant<atom("g_sinkp")>;
using execute_query_atom  = atom_constant<atom("execute")>;
using delete_query_atom  = atom_constant<atom("delete")>;
using get_operators_atom  = atom_constant<atom("g_ops")>;

using set_type_atom = atom_constant<atom("s_type")>;
using get_type_atom = atom_constant<atom("g_type")>;
}

#endif //IOTDB_INCLUDE_NETWORK_ATOM_UTILS_HPP_
