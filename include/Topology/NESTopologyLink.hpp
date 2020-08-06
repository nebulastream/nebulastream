#ifndef INCLUDE_TOPOLOGY_NESTOPOLOGYLINK_HPP_
#define INCLUDE_TOPOLOGY_NESTOPOLOGYLINK_HPP_

#include <memory>

#include "NESTopologyEntry.hpp"

namespace NES {
#define NOT_EXISTING_LINK_ID std::numeric_limits<size_t>::max()

enum LinkType {
    NodeToNode,
    NodeToSensor,
    SensorToNode,
    UnkownLinkType
};

/**
 * @brief This class is used for representing the links between two NES topology nodes. The direction is represented by
 * source and destination nodes.
 *
 * The link can be of type: node to node, node to sensor, and sensor to sensor.
 */
class NESTopologyLink {

  public:
    explicit NESTopologyLink(size_t pLinkId, NESTopologyEntryPtr pSourceNode, NESTopologyEntryPtr pDestNode,
                             size_t pLinkCapacity, size_t pLinkLatency) {
        linkId = pLinkId;
        sourceNode = pSourceNode;
        destNode = pDestNode;
        linkCapacity = pLinkCapacity;
        linkLatency = pLinkLatency;
    }

    /**
     * @brief method to get the id of the node
     * @return id as a size_t
     */
    size_t getId();

    /**
     * @brief get the source node of a link
     * @return NESTopologyEntryPtr to the source node
     */
    NESTopologyEntryPtr getSourceNode();

    /**
     * @brief method to get the id of the source of a lin
     * @return size_t id of the source node
     */
    size_t getSourceNodeId();

    /**
     * @brief get the destination node of a link
     * @return NESTopologyEntryPtr to the destination node
     */
    NESTopologyEntryPtr getDestNode();

    /**
     * @brief get the id of the destination node of the link
     * @return size_t of destination node
     */
    size_t getDestNodeId();

    /**
     * @brief get the type of the link
     * @return LinkType
     */
    LinkType getLinkType();

    /**
     * @brief get the type of the link as a string
     * @return link type as string
     */
    std::string getLinkTypeString();

    /**
     * @brief get link latency
     * @return link latency as size_t
     */
    size_t getLinkLatency() const;

    /**
     * @brief get link capacity
     * @return link capacity as size_t
     */
    size_t getLinkCapacity() const;

    /**
     * @brief update link latency
     * @param new linkLatency
     */
    void updateLinkLatency(size_t linkLatency);

    /**
     * @brief update link capacity
     * @param new linkCapacity
     */
    void updateLinkCapacity(size_t linkCapacity);

  private:
    /**
     * @brief unique identifier of the link.
     */
    size_t linkId;

    /**
     * @brief source node pointer
     */
    NESTopologyEntryPtr sourceNode;

    /**
     * @brief destination node pointer
     */
    NESTopologyEntryPtr destNode;

    /**
     * @brief link latency (lower is better)
     */
    size_t linkLatency;

    /**
     * @brief link capacity (higher is better)
     */
    size_t linkCapacity;
};

typedef std::shared_ptr<NESTopologyLink> NESTopologyLinkPtr;
}// namespace NES
#endif /* INCLUDE_TOPOLOGY_NESTOPOLOGYLINK_HPP_ */
