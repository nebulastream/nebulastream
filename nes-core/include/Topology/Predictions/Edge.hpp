#ifndef TOPOLOGYPREDICTION_PREDICTEDVIEWS_EDGE_HPP_
#define TOPOLOGYPREDICTION_PREDICTEDVIEWS_EDGE_HPP_
#include <cstdint>
#include <boost/functional/hash.hpp>
#include <functional>

namespace NES::Experimental {
namespace TopologyPrediction {
class Edge {
  public:
    [[nodiscard]] std::string toString() const {
        std::stringstream ss;
        ss << child << "->" << parent;
        return ss.str();
    }
    bool operator==(const Edge& other) const { return this->parent == other.parent && this->child == other.child; }
    uint64_t child;
    uint64_t parent;
};
}
}

template<>
struct std::hash<NES::Experimental::TopologyPrediction::Edge> {
    std::size_t operator()(NES::Experimental::TopologyPrediction::Edge const& e) const noexcept {
        std::size_t seed = 0;
        boost::hash_combine(seed, e.child);
        boost::hash_combine(seed, e.parent);
        return seed;
    }
};

template<>
struct std::equal_to<NES::Experimental::TopologyPrediction::Edge> {
    constexpr bool operator()(const NES::Experimental::TopologyPrediction::Edge& lhs, const NES::Experimental::TopologyPrediction::Edge& rhs) const {
        return lhs.child == rhs.child && lhs.parent == rhs.parent;
    }
};
#endif //TOPOLOGYPREDICTION_PREDICTEDVIEWS_EDGE_HPP_
