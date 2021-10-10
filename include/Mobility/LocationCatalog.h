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

#ifndef NES_LOCATIONCATALOG_H
#define NES_LOCATIONCATALOG_H

#include <map>
#include <mutex>
#include <cpprest/json.h>
#include <Mobility/Geo/Node/GeoSink.h>
#include <Mobility/Geo/Node/GeoSource.h>


namespace NES {

using std::string;


class LocationCatalog {

  private:
    uint32_t defaultStorageSize;
    std::map<string, GeoSinkPtr> sinks;
    std::map<string, GeoSourcePtr> sources;
    std::mutex catalogLock;

    std::vector<PredictedSourcePtr> findSourcesOnRoute(const CartesianLinePtr& route);

  public:
    explicit LocationCatalog(uint32_t defaultStorageSize);
    void addSink(const string& nodeId, double movingRangeArea);
    void addSink(const string& nodeId, double movingRangeArea, string streamName);
    void addSource(const string& nodeId);
    void addSource(const string& nodeId, double rangeArea);
    GeoSinkPtr getSink(const string& nodeId);
    GeoSourcePtr getSource(const string& nodeId);
    std::vector<GeoSinkPtr> getSinkWithStream(const string& streamName);
    void updateNodeLocation(const string& nodeId, const GeoPointPtr& location);
    void updateSinks();
    void updateSources();
    web::json::value toJson();

    bool contains(const string& nodeId);
    uint64_t size();
};

using LocationCatalogPtr = std::shared_ptr<LocationCatalog>;

}

#endif//NES_LOCATIONCATALOG_H
