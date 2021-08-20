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

namespace NES {

web::json::value GeoNodeUtils::generateJson(const GeoSinkPtr& sink) {
    web::json::value sinkJsonValue{};
    sinkJsonValue["id"] = web::json::value::string(sink->getId());
    sinkJsonValue["latitude"] = web::json::value::number(sink->getCurrentLocation()->getLatitude());
    sinkJsonValue["longitude"] = web::json::value::number(sink->getCurrentLocation()->getLongitude());
    return sinkJsonValue;
}
web::json::value GeoNodeUtils::generateJson(const GeoSourcePtr& source) {
    web::json::value sourceJsonValue{};
    sourceJsonValue["id"] = web::json::value::string(source->getId());
    sourceJsonValue["latitude"] = web::json::value::number(source->getCurrentLocation()->getLatitude());
    sourceJsonValue["longitude"] = web::json::value::number(source->getCurrentLocation()->getLongitude());
    sourceJsonValue["enabled"] = web::json::value::number(source->isEnabled());
    return sourceJsonValue;
}

}
