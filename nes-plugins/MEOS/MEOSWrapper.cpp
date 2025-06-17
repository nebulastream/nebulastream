/*
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

#include "include/MEOSWrapper.hpp"

// Required includes for the implementation
#include <string>
#include <chrono>
#include <ctime>
#include <iomanip>
#include <sstream>
#include <iostream>



namespace MEOS {

    Meos::Meos() { 
        meos_initialize("UTC",nullptr); 
    }

    Meos::~Meos() { 

    }

    std::string Meos::convertSecondsToTimestamp(long long seconds) {
        std::chrono::seconds sec(seconds);
        std::chrono::time_point<std::chrono::system_clock> tp(sec);

        // Convert to time_t for formatting
        std::time_t time = std::chrono::system_clock::to_time_t(tp);

        // Convert to local time
        std::tm local_tm = *std::localtime(&time);

        // Format the time as a string
        std::ostringstream oss;
        oss << std::put_time(&local_tm, "%Y-%m-%d %H:%M:%S");
        return oss.str();
    }

    // TemporalInstant constructore
    Meos::TemporalInstant::TemporalInstant(double lon, double lat, long long ts, int srid) {

        Meos meos_instance;
        std::string ts_string = meos_instance.convertSecondsToTimestamp(ts);
        std::string str_pointbuffer = "SRID=" + std::to_string(srid) + ";POINT(" + std::to_string(lon) + " " + std::to_string(lat) + ")@" + ts_string;

        std::cout << "Creating MEOS TemporalInstant from: " << str_pointbuffer << std::endl;

        Temporal *temp = tgeompoint_in(str_pointbuffer.c_str());

        if (temp == nullptr) {
            std::cout << "Failed to parse temporal point with tgeompoint_in" << std::endl;
            // Try alternative format or set to null
            instant = nullptr;
        } else {
            instant = temp;
            std::cout << "Successfully created temporal point" << std::endl;
        }
    }

    Meos::TemporalInstant::~TemporalInstant() { 
        if (instant) {
            free(instant); 
        }
    }

    bool Meos::TemporalInstant::intersects(const TemporalInstant& point) const {  
        std::cout << "TemporalInstant::intersects called" << std::endl;
        // Use MEOS eintersects function for temporal points  
        bool result = eintersects_tpoint_tpoint((const Temporal *)this->instant, (const Temporal *)point.instant);
        if (result) {
            std::cout << "TemporalInstant intersects" << std::endl;
        }

        return result;
    }
 
    // TemporalSequence constructor - fix signature to match header
    Meos::TemporalSequence::TemporalSequence(double lon, double lat, int t_out) {
        // Placeholder implementation
        sequence = nullptr;
        std::cout << "TemporalSequence created with coordinates (" << lon << ", " << lat << ") at time " << t_out << std::endl;
    }

    Meos::TemporalSequence::~TemporalSequence() { 
        if (sequence) {
            free(sequence); 
        }
    }

    double Meos::TemporalSequence::length(const TemporalInstant& /* instant */) const {
        // Placeholder implementation
        // Using comment to avoid unused parameter warning
        return 0.0;
    }   

    // SpatioTemporalBox implementation
    Meos::SpatioTemporalBox::SpatioTemporalBox(const std::string& wkt_string) {
        // Use MEOS stbox_in function to parse the WKT string
        stbox_ptr = stbox_in(wkt_string.c_str());
    }

    Meos::SpatioTemporalBox::~SpatioTemporalBox() {
        if (stbox_ptr) {
            free(stbox_ptr);
            stbox_ptr = nullptr;
        }
    }


}// namespace MEOS

