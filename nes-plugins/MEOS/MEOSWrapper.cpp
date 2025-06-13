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
#include <iostream>

// Forward declare the MEOS C STBox type from the global scope
extern "C" {
    struct STBox;
}

namespace MEOS {

    // Constructor - no parameters according to header
    Meos::Meos() { 
        // MEOS initialize takes no parameters
        meos_initialize(); 
    }

    Meos::~Meos() { 
        meos_finalize(); 
    }

    std::string Meos::convertSecondsToTimestamp(long long seconds) {
        // Simple timestamp conversion for now
        return std::to_string(seconds);
    }

    // TemporalInstant constructor - matches header signature
    Meos::TemporalInstant::TemporalInstant(const std::string& mf_string) {
        // For now, try to parse as tfloat MF-JSON - would need to determine type dynamically
        Temporal *temp = tfloat_from_mfjson(mf_string.c_str());
        instant = temp;
    }

    Meos::TemporalInstant::~TemporalInstant() { 
        if (instant) {
            free(instant); 
        }
    }

    bool Meos::TemporalInstant::intersects(const TemporalInstant& point) const {  
        if (!instant || !point.instant) {
            return false;
        }
        
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
        // Placeholder implementation - would need proper MEOS spatial functions
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
            // Free the MEOS STBox using free (not pfree)
            free(stbox_ptr);
            stbox_ptr = nullptr;
        }
    }


}// namespace MEOS

