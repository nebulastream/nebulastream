/*
 * FogTopologyEntry.hpp
 *
 *  Created on: Jan 31, 2019
 *      Author: zeuchste
 */

#ifndef INCLUDE_TOPOLOGY_FOGTOPOLOGYENTRY_HPP_
#define INCLUDE_TOPOLOGY_FOGTOPOLOGYENTRY_HPP_
#include <string>
enum FogNodeType { Worker, Sensor };

class FogTopologyEntry
{
public:
//	FogTopologyEntry(){};

	virtual FogNodeType getEntryType() = 0;
	virtual std::string getEntryTypeString() = 0;

	virtual size_t getId() = 0;

};

typedef std::shared_ptr<FogTopologyEntry> FogTopologyEntryPtr;


#endif /* INCLUDE_TOPOLOGY_FOGTOPOLOGYENTRY_HPP_ */
