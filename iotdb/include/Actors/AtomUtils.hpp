#ifndef INCLUDE_ACTORS_ATOMUTILS_HPP_
#define INCLUDE_ACTORS_ATOMUTILS_HPP_

#include <string>

using std::cout;
using std::cerr;
using std::endl;
using std::string;

namespace NES {
using namespace caf;
constexpr auto task_timeout = std::chrono::seconds(10);
constexpr auto task_timeout_small = std::chrono::seconds(2);

using disconnect_atom = atom_constant<atom("disconnect")>;
using terminate_atom = atom_constant<atom("term")>;

//coordinator stubs
using register_worker_atom = atom_constant<atom("r_worker")>;
using register_sensor_atom = atom_constant<atom("r_sensor")>;
using deregister_sensor_atom = atom_constant<atom("dr_sensor")>;
using register_phy_stream_atom = atom_constant<atom("r_phy_str")>;
using register_log_stream_atom = atom_constant<atom("re_log_str")>;
using remove_log_stream_atom = atom_constant<atom("rm_log_str")>;
using remove_phy_stream_atom = atom_constant<atom("rm_phy_str")>;

using add_parent_atom = atom_constant<atom("ad_par_str")>;
using remove_parent_atom = atom_constant<atom("rm_par_str")>;
using get_own_id = atom_constant<atom("get_id")>;

using execute_query_atom = atom_constant<atom("exe_query")>;
using register_query_atom = atom_constant<atom("register")>;
using deregister_query_atom = atom_constant<atom("deregister")>;
using deploy_query_atom = atom_constant<atom("deploy")>;

//worker stubs
using get_local_sink_port_atom = atom_constant<atom("g_sinkp")>;
using execute_operators_atom = atom_constant<atom("exe_ops")>;
using delete_query_atom = atom_constant<atom("delete")>;
using get_operators_atom = atom_constant<atom("g_ops")>;

using set_type_atom = atom_constant<atom("s_type")>;
using get_type_atom = atom_constant<atom("g_type")>;
}

#endif //INCLUDE_ACTORS_ATOMUTILS_HPP_

