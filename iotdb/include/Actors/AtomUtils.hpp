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

using disconnect_atom = atom_constant<atom("disconnect")>;
using terminate_atom = atom_constant<atom("term")>;

//coordinator node handling
using register_node_atom = atom_constant<atom("r_node")>;
using deregister_node_atom = atom_constant<atom("dr_sensor")>;

//coordinator stream handling
using register_phy_stream_atom = atom_constant<atom("r_phy_str")>;
using register_log_stream_atom = atom_constant<atom("re_log_str")>;
using remove_log_stream_atom = atom_constant<atom("rm_log_str")>;
using remove_phy_stream_atom = atom_constant<atom("rm_phy_str")>;

//coordinator toplogy handling
using add_parent_atom = atom_constant<atom("ad_par_str")>;
using remove_parent_atom = atom_constant<atom("rm_par_str")>;

// Worker methods
using deploy_query_atom = atom_constant<atom("deploy_q")>;
using undeploy_query_atom = atom_constant<atom("undeploy_q")>;

using start_query_atom = atom_constant<atom("start_q")>;
using stop_query_atom = atom_constant<atom("stop_q")>;

}

#endif //INCLUDE_ACTORS_ATOMUTILS_HPP_

