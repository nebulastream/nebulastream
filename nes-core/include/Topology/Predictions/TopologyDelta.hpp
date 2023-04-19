#ifndef TOPOLOGYPREDICTION__DELTA_HPP_
#define TOPOLOGYPREDICTION__DELTA_HPP_
#include <cstdint>
#include <vector>
#include <memory>
#include <Topology/Predictions/Edge.hpp>

namespace NES::Experimental {

namespace TopologyPrediction {
struct TopologyDelta {
  TopologyDelta(std::vector<Edge> added, std::vector<Edge> removed);

  std::string toString();

  std::vector<Edge> added;
  std::vector<Edge> removed;
};
}
}

#endif //TOPOLOGYPREDICTION__DELTA_HPP_
