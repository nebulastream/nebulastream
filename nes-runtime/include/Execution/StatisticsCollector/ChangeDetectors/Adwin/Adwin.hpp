/*
 *      Adwin.h
 *      
 *      Copyright (C) 2008 Universitat Politècnica de Catalunya
 *      @author Albert Bifet - Ricard Gavaldà (abifet,gavalda@lsi.upc.edu)
 *      
 *      This program is free software; you can redistribute it and/or modify
 *      it under the terms of the GNU General Public License as published by
 *      the Free Software Foundation; either version 2 of the License, or
 *      (at your option) any later version.
 *      
 *      This program is distributed in the hope that it will be useful,
 *      but WITHOUT ANY WARRANTY; without even the implied warranty of
 *      MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *      GNU General Public License for more details.
 *      
 *      You should have received a copy of the GNU General Public License
 *      along with this program; if not, write to the Free Software
 *      Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 *      MA 02110-1301, USA.
 */

#ifndef NES_RUNTIME_INCLUDE_EXECUTION_ADWIN_HPP_
#define NES_RUNTIME_INCLUDE_EXECUTION_ADWIN_HPP_

#include <Execution/StatisticsCollector/ChangeDetectors/Adwin/List.hpp>
#include <Execution/StatisticsCollector/ChangeDetectors/ChangeDetector.hpp>

namespace NES::Runtime::Execution {

class Adwin : public ChangeDetector {
  public:
    /**
    * @brief Initialize ADWIN.
    * @param delta confidence value (0,1) controls sensitivity of detecting concept drift
    * @param M number of buckets
    */
    Adwin(double delta, int M);

    /**
     * @brief Update ADWIN with new element.
     * @param value
     * @return
     */
    bool insertValue(double& value) override;

    /**
     * @brief Get estimated mean.
     * @return estimated mean
     */
    double getEstimation() const;
    void print() const;
    int length() const { return windowSize; }

  private:
    /**
     * @brief Create new bucket for new element.
     * @param value
     */
    void insertElement(const double& value);

    /**
     * @brief Compress buckets: when there are MAXBUCKETS+1 buckets of size 2^i, merge the two oldest buckets.
     */
    void compressBuckets();

    /**
     * @brief Checks whether concept drift has occurred.
     * @return true, if ADWIN detects change
     */
    bool checkDrift();

    /**
     * @brief
     * @param N0 length of subwindow W0
     * @param N1 length n1 of subwindow W1
     * @param u0 average of W0
     * @param u1 average of W1
     * @return true, if observed average in both subwindows differs more than threshold
     */
    bool cutExpression(int n0, int n1, const double& u0, const double& u1);

    /**
     * @brief Delete buckets to reduce window size.
     */
    void deleteElement();

    int numberOfBuckets;
    List bucketList;
    int mintTime;
    int mintClock;
    double mdblError;
    double mdblWidth;
    int windowSize; // width
    int lastBucketIndex;
    double sum; // running sum
    double variance; // running variance

    const int MINTCLOCK;
    const int MINLENGTHWINDOW;
    const double DELTA;
    const int MAXBUCKETS;
};
} // namespace NES::Runtime::Execution
#endif// NES_RUNTIME_INCLUDE_EXECUTION_ADWIN_HPP_