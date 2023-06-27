#ifndef TOPOLOGYPREDICTION__DELTA_HPP_
#define TOPOLOGYPREDICTION__DELTA_HPP_
#include <Topology/Prediction/Edge.hpp>
#include <cstdint>
#include <memory>
#include <vector>

namespace NES::Experimental::TopologyPrediction {
/**
 * @brief this class represents a set of changes to be applied to the topology
 */
class TopologyDelta {
  public:
    /**
   * @brief constructor
   * @param added a list of edges to be added to the topology
   * @param removed a list of edges to be removed from the topology
   */
    TopologyDelta(std::vector<Edge> added, std::vector<Edge> removed);

    /**
   * @brief this function returns a string visualizing the content of the changelog
   * @return a string in the format "added: {CHILD->PARENT, CHILD->PARENT, ...} removed: {CHILD->PARENT, CHILD->PARENT, ...}"
   */
    [[nodiscard]] std::string toString() const;

    /**
   * @brief return the list of added edges contained in this delta
   * @return a vector of edges
   */
    [[nodiscard]] std::vector<Edge> getAdded() const;

    /**
     * @brief return the list of removed edges contained in this delta
     * @return a vector of edges
     */
    [[nodiscard]] std::vector<Edge> getRemoved() const;

  private:
    /**
     * @brief obtain a string representation of a vector of edges
     * @param edges a vector of edges
     * @return a string in the format "CHILD->PARENT, CHILD->PARENT, ..."
     */
    static std::string edgeListToString(const std::vector<Edge>& edges);

    std::vector<Edge> added;
    std::vector<Edge> removed;
};
}// namespace NES::Experimental::TopologyPrediction

#endif//TOPOLOGYPREDICTION__DELTA_HPP_
