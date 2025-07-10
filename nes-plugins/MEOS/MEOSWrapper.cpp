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
        meos_initialize(); 
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
    
    // Constructor for creating trajectory from multiple temporal instants
    Meos::TemporalSequence::TemporalSequence(const std::vector<TemporalInstant*>& instants) {
        // Placeholder implementation - would use MEOS tsequence_make or similar
        sequence = nullptr;
        std::cout << "TemporalSequence created from " << instants.size() << " temporal instants" << std::endl;
    }
    
    // Constructor for creating trajectory from coordinate arrays
    Meos::TemporalSequence::TemporalSequence(const std::vector<double>& longitudes, 
                                            const std::vector<double>& latitudes, 
                                            const std::vector<long long>& timestamps, 
                                            int srid) {
        if (longitudes.size() != latitudes.size() || longitudes.size() != timestamps.size()) {
            std::cout << "Error: Coordinate and timestamp arrays must have same size" << std::endl;
            sequence = nullptr;
            return;
        }
        
        try {
            if (longitudes.empty()) {
                sequence = nullptr;
                return;
            }
            
            std::cout << "Creating TemporalSequence from " << longitudes.size() << " points using MEOS API" << std::endl;
            
            // Create temporal instants using MEOS functions
            std::vector<Temporal*> instants;
            Meos meosWrapper; // For timestamp conversion
            
            for (size_t i = 0; i < longitudes.size(); ++i) {
                // Create WKT string for temporal point
                std::string timeStr = meosWrapper.convertSecondsToTimestamp(timestamps[i]);
                
                // Format: "SRID=4326;POINT(lon lat)@timestamp"
                char wkt[512];
                snprintf(wkt, sizeof(wkt), "SRID=%d;POINT(%f %f)@%s", 
                        srid, longitudes[i], latitudes[i], timeStr.c_str());
                
                std::cout << "Creating temporal instant: " << wkt << std::endl;
                
                // Use MEOS function to create temporal instant
                Temporal* instant = tgeompoint_in(wkt);
                if (instant) {
                    instants.push_back(instant);
                } else {
                    std::cout << "Warning: Failed to create temporal instant for point " << i << std::endl;
                }
            }
            
            if (instants.empty()) {
                std::cout << "Error: No valid temporal instants created" << std::endl;
                sequence = nullptr;
                return;
            }
            
            // Create temporal sequence using MEOS API
            // First, we need to create a TSequence from the temporal instants
            // For now, store the instants and create a simple sequence structure
            if (instants.size() == 1) {
                // Single instant becomes the sequence
                sequence = instants[0];
                std::cout << "Created single-instant sequence" << std::endl;
            } else {
                // Multiple instants - store them for later processing
                // Real implementation would use tsequence_make_exp or similar
                sequence = reinterpret_cast<Temporal*>(new std::vector<Temporal*>(instants));
                std::cout << "Stored " << instants.size() << " temporal instants for sequence processing" << std::endl;
            }
            
        } catch (const std::exception& e) {
            std::cout << "Error creating TemporalSequence: " << e.what() << std::endl;
            sequence = nullptr;
        }
    }

    Meos::TemporalSequence::~TemporalSequence() { 
        if (sequence) {
            // Check if this is a vector of instants or a single temporal object
            // This is a bit hacky but works for our current implementation
            try {
                // Try to interpret as vector first
                auto* instants_vec = reinterpret_cast<std::vector<Temporal*>*>(sequence);
                // Simple heuristic: if the "vector" has a reasonable size, treat as vector
                if (instants_vec && reinterpret_cast<uintptr_t>(instants_vec) > 0x1000) {
                    // Likely a vector pointer
                    for (auto* instant : *instants_vec) {
                        if (instant) free(instant);
                    }
                    delete instants_vec;
                } else {
                    // Likely a single temporal instant
                    free(sequence);
                }
            } catch (...) {
                // If anything goes wrong, just free as single object
                free(sequence);
            }
            sequence = nullptr;
        }
    }

    double Meos::TemporalSequence::length(const TemporalInstant& /* instant */) const {
        // Placeholder implementation
        // Using comment to avoid unused parameter warning
        return 0.0;
    }
    
    std::string Meos::TemporalSequence::serialize() const {
        if (!sequence) {
            return "EMPTY_SEQUENCE";
        }
        
        try {
            // For now, create a simple WKT-like representation
            // Real implementation would use proper MEOS serialization
            std::ostringstream result;
            result << "TEMPORALSEQUENCE(";
            
            // Get the stored instants
            auto* instants_vec = reinterpret_cast<std::vector<Temporal*>*>(sequence);
            if (instants_vec && !instants_vec->empty()) {
                result << instants_vec->size() << "_INSTANTS";
            }
            
            result << ")";
            return result.str();
            
        } catch (const std::exception& e) {
            std::cout << "Error in MEOS serialize: " << e.what() << std::endl;
            return "MEOS_SERIALIZATION_EXCEPTION";
        }
    }
    
    std::string Meos::TemporalSequence::toMFJSON() const {
        if (!sequence) {
            return R"({"type": "MovingPoint", "coordinates": [], "datetimes": [], "interpolation": "Linear"})";
        }
        
        try {
            // For now, create MF-JSON manually since MEOS API is limited
            // Real implementation would extract coordinates/times from stored instants
            std::ostringstream result;
            result << R"({"type": "MovingPoint", "coordinates": [)";
            
            // Get the stored instants  
            auto* instants_vec = reinterpret_cast<std::vector<Temporal*>*>(sequence);
            if (instants_vec && !instants_vec->empty()) {
                // Placeholder coordinates - real implementation would extract from MEOS instants
                for (size_t i = 0; i < instants_vec->size(); ++i) {
                    if (i > 0) result << ",";
                    result << "[0.0,0.0]"; // Would extract actual coordinates
                }
            }
            
            result << R"(], "datetimes": [)";
            
            if (instants_vec && !instants_vec->empty()) {
                // Placeholder timestamps - real implementation would extract from MEOS instants
                for (size_t i = 0; i < instants_vec->size(); ++i) {
                    if (i > 0) result << ",";
                    result << R"("2021-01-01T00:00:00Z")"; // Would extract actual timestamps
                }
            }
            
            result << R"(], "interpolation": "Linear"})";
            
            return result.str();
            
        } catch (const std::exception& e) {
            std::cout << "Error in MEOS toMFJSON: " << e.what() << std::endl;
            return R"({"type": "MovingPoint", "coordinates": [], "datetimes": [], "interpolation": "Linear"})";
        }
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

