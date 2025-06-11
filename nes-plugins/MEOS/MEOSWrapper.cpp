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
        // Simple temporal overlap check
        if (!instant || !point.instant) {
            return false;
        }
        
        // Get time spans and check if they overlap - using correct function name
        Span *span1 = temporal_to_tstzspan(instant);
        Span *span2 = temporal_to_tstzspan(point.instant);
        
        bool result = overlaps_span_span(span1, span2);
        
        free(span1);
        free(span2);
        
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

}// namespace MEOS

