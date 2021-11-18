/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

#include <Mobility/Geo/Node/GeoNodeUtils.h>
#include <Mobility/Geo/Projection/GeoCalculator.h>
#include <Mobility/LocationCatalog.h>
#include <Mobility/Utils/MathUtils.h>
#include <Util/Logger.hpp>

namespace NES {

LocationCatalog::LocationCatalog(uint32_t defaultStorageSize) : defaultStorageSize(defaultStorageSize) {}

void LocationCatalog::addSink(const string& nodeId, const double movingRangeArea) {
    std::lock_guard lock(catalogLock);
    NES_DEBUG("LocationCatalog: adding sink " << nodeId);
    this->sinks.insert(
        std::pair<string, GeoSinkPtr>(nodeId, std::make_shared<GeoSink>(nodeId, movingRangeArea, defaultStorageSize)));
}

void LocationCatalog::addSink(const string& nodeId, double movingRangeArea, string streamName) {
    std::lock_guard lock(catalogLock);
    NES_DEBUG("LocationCatalog: adding sink " << nodeId);
    this->sinks.insert(
        std::pair<string, GeoSinkPtr>(nodeId, std::make_shared<GeoSink>(nodeId, movingRangeArea, defaultStorageSize, streamName)));
}

void LocationCatalog::addSource(const string& nodeId) {
    std::lock_guard lock(catalogLock);
    NES_DEBUG("LocationCatalog: adding source " << nodeId);
    this->sources.insert(std::pair<string, GeoSourcePtr>(nodeId, std::make_shared<GeoSource>(nodeId, defaultStorageSize)));
}

void LocationCatalog::addSource(const string& nodeId, double rangeArea) {
    std::lock_guard lock(catalogLock);
    NES_DEBUG("LocationCatalog: adding source " << nodeId << " with range " << rangeArea);
    this->sources.insert(
        std::pair<string, GeoSourcePtr>(nodeId, std::make_shared<GeoSource>(nodeId, rangeArea, defaultStorageSize)));
}

GeoSinkPtr LocationCatalog::getSink(const string& nodeId) {
    std::lock_guard lock(catalogLock);
    return sinks.at(nodeId);
}

GeoSourcePtr LocationCatalog::getSource(const string& nodeId) {
    std::lock_guard lock(catalogLock);
    return sources.at(nodeId);
}

std::vector<GeoSinkPtr> LocationCatalog::getSinkWithStream(const string& streamName) {
    std::unique_lock lock(catalogLock);
    std::vector<GeoSinkPtr> sinksWithStream;
    for (auto const& [nodeId, sink] : sinks) {
        if (sink->getStreamName() == streamName) {
            sinksWithStream.push_back(sink);
        }
    }
    return sinksWithStream;
}

void LocationCatalog::updateNodeLocation(const string& nodeId, const GeoPointPtr& location) {
    std::lock_guard lock(catalogLock);
    NES_DEBUG("LocationCatalog: update location from node " << nodeId);
    if (sinks.contains(nodeId)) {
        std::_Rb_tree_iterator<std::pair<const string, GeoSinkPtr>> it = this->sinks.find(nodeId);
        if (it != this->sinks.end()) {
            it->second->setCurrentLocation(location);
        }
    }

    if (sources.contains(nodeId)) {
        std::_Rb_tree_iterator<std::pair<const string, GeoSourcePtr>> it = this->sources.find(nodeId);
        if (it != this->sources.end()) {
            it->second->setCurrentLocation(location);
        }
    }
}

void LocationCatalog::updateSinks() {
    std::unique_lock lock(catalogLock);

    for (auto const& [nodeId, sink] : sinks) {
        if (sink->getLocationHistory()->size() > 1) {
            CartesianLinePtr trajectory = MathUtils::leastSquaresRegression(sink->getLocationHistory()->getCartesianPoints());
            const double allowedSlopeOffset = 0.5;
            if (sink->getTrajectory() == nullptr) {
                sink->setTrajectory(trajectory);
                sink->setPredictedSources(findSourcesOnRoute(trajectory));
            } else {
                double slope = sink->getTrajectory()->getSlope();
                if (trajectory->getSlope() > slope + allowedSlopeOffset && trajectory->getSlope() < slope - allowedSlopeOffset) {
                    // Update trajectory
                    sink->setTrajectory(trajectory);
                    sink->setPredictedSources(findSourcesOnRoute(trajectory));
                }
            }
        }

        bool filterEnabled = false;
        for (auto const& predictedSource: sink->getPredictedSources()) {
            if (predictedSource->getOverlaps().empty()) {
                continue;
            }

            if (sink->getRange()->contains(predictedSource->getSource()->getRange())) {
                predictedSource->setInRange(true);
                filterEnabled = true;
                break;
            }
        }

        if (sink->isFilterEnabled() != filterEnabled) {
            string currentStatus = (filterEnabled) ? "true" : "false";
            string oldStatus = (sink->isFilterEnabled()) ? "true" : "false";
            NES_DEBUG("LocationCatalog: Sink '" << sink->getId() << "' changed from '" << oldStatus << "' to '"
                                                  << currentStatus << "'!");
        }
        sink->setFilterEnabled(filterEnabled);
    }
}

void LocationCatalog::updateSources() {
    std::unique_lock lock(catalogLock);

    for (auto const& [nodeId, sink] : sinks) {
        if (sink->getRange() == nullptr) {
            continue;
        }

        for (auto const& [sourceId, source] : sources) {
            if (source->hasRange()) {
                bool enabled = sink->getRange()->contains(source->getRange());
                string status = (enabled) ? "true" : "false";

                NES_DEBUG("LocationCatalog: Sink '" << sink->getId() << "' contains source with range '" << source->getId()
                                                    << "': " << status);
                if (source->isEnabled() != enabled) {
                    string oldStatus = (source->isEnabled()) ? "true" : "false";
                    NES_DEBUG("LocationCatalog: Source '" << source->getId() << "' changed from '" << oldStatus << "' to '"
                                                          << status << "'!");
                }
                source->setEnabled(enabled);
            } else {
                bool enabled = sink->getRange()->contains(source->getCurrentLocation());
                string status = (enabled) ? "true" : "false";

                NES_DEBUG("LocationCatalog: Sink '" << sink->getId() << "' contains source '" << source->getId()
                                                    << "': " << status);
                if (source->isEnabled() != enabled) {
                    string oldStatus = (source->isEnabled()) ? "true" : "false";
                    NES_DEBUG("LocationCatalog: Source '" << source->getId() << "' changed from '" << oldStatus << "' to '"
                                                          << status << "'!");
                }
                source->setEnabled(enabled);
            }
        }
    }
}

bool LocationCatalog::contains(const string& nodeId) {
    std::unique_lock lock(catalogLock);
    return this->sinks.contains(nodeId) || this->sources.contains(nodeId);
}

uint64_t LocationCatalog::size() {
    std::lock_guard lock(catalogLock);
    return this->sinks.size() + this->sources.size();
}

web::json::value LocationCatalog::toJson() {
    std::lock_guard lock(catalogLock);

    std::vector<web::json::value> sinksJson = {};
    for (auto const& [nodeId, sink] : sinks) {
        web::json::value currentNodeJsonValue = GeoNodeUtils::generateJson(sink);
        sinksJson.push_back(currentNodeJsonValue);
    }

    std::vector<web::json::value> sourcesJson = {};
    for (auto const& [nodeId, source] : sources) {
        web::json::value currentNodeJsonValue = GeoNodeUtils::generateJson(source);
        sourcesJson.push_back(currentNodeJsonValue);
    }

    web::json::value responseJson{};
    responseJson["sinks"] = web::json::value::array(sinksJson);
    responseJson["sources"] = web::json::value::array(sourcesJson);

    return responseJson;
}

std::vector<PredictedSourcePtr> LocationCatalog::findSourcesOnRoute(const CartesianLinePtr& route) {
    std::vector<PredictedSourcePtr> predictedSources;

    for (auto const& [nodeId, source] : sources) {
        CartesianPointPtr center = GeoCalculator::geographicToCartesian(source->getCurrentLocation());
        CartesianCirclePtr cartesianCircle = std::make_shared<CartesianCircle>(center, source->getRange()->getDistanceToBound());
        if (MathUtils::intersect(route, cartesianCircle)) {
            PredictedSourcePtr predictedSource = std::make_shared<PredictedSource>(source);
            for (auto const& existingSource : predictedSources) {
                if (predictedSource->getSource()->getRange()->contains(existingSource->getSource()->getRange())) {
                    predictedSource->getOverlaps().push_back(existingSource->getSource());
                    existingSource->getOverlaps().push_back(predictedSource->getSource());
                }
            }
            predictedSources.push_back(predictedSource);
        }
    }

    return predictedSources;
}

}
// namespace NES
