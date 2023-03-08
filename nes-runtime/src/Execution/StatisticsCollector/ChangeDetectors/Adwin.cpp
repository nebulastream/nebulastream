/*
 *      Adwin.cpp
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

#include <cstdio>
#include <cmath>
#include <Execution/StatisticsCollector/ChangeDetectors/Adwin.hpp>

namespace NES::Runtime::Execution {

static int bucketSize(int Row){
    return (int) pow(2,Row);
}

Adwin::Adwin(double delta, int M) : bucketList(M),MINTCLOCK(1),MINLENGTHWINDOW(16),DELTA(delta),MAXBUCKETS(M){
    mintTime = 0;
    mintClock = MINTCLOCK;
    mdblError = 0;
    mdblWidth = 0;
    // initialize buckets
    lastBucketIndex = 0;
    sum = 0;
    variance = 0;
    windowSize = 0;
    numberOfBuckets = 0;
}

bool Adwin::insertValue(double& value){
    insertElement(value);
    compressBuckets();
    return checkDrift();
}

void Adwin::insertElement(const double& value){
    windowSize++;
    bucketList.head->addBack(value,0.0);
    numberOfBuckets++;

    // update statistics
    if (windowSize > 1) {
        variance += (windowSize - 1) * (value - sum / (windowSize - 1)) * (value - sum / (windowSize - 1)) / windowSize;
    }
    sum += value;
}

void Adwin::compressBuckets(){
    // traverse the list of buckets in increasing order
    ListNode* cursor = bucketList.head;
    ListNode* nextNode;

    int i = 0;
       		   
    do {
        // find the number of buckets in a row
        int k = cursor->size;
        // if the row is full, merge buckets
        if (k == MAXBUCKETS + 1) {
            nextNode = cursor->next;
            if (nextNode == nullptr) { // if no bucket at the end of list, add new tail
                bucketList.addToTail();
                nextNode = cursor->next;
                lastBucketIndex++;
            }
            int n1 = bucketSize(i);
            int n2 = bucketSize(i);
            double u1 = cursor->sum[0] / n1;
            double u2 = cursor->sum[1] / n2;
            double incVariance = n1 * n2 * (u1 - u2) * (u1 - u2) / (n1 + n2);

            nextNode->addBack(cursor->sum[0] + cursor->sum[1], cursor->variance[0] + cursor->variance[1] + incVariance); // insert sum of first two values into next node
            numberOfBuckets--;
            cursor->dropFront(2); // drop first n elements
            if (nextNode->size <= MAXBUCKETS) break;
        } else {
            break;
        }
        cursor = cursor->next;
        i++;

    } while (cursor != nullptr);
}

bool Adwin::checkDrift(){
    bool change = false;
    bool quit = false;
    ListNode* nodeCursor;
    mintTime++;

    if((mintTime % mintClock == 0) && (windowSize > MINLENGTHWINDOW) ){
        bool reduceWidth = true;
        while (reduceWidth) {
            reduceWidth = false;
            quit = false;
            int n0 = 0; // length n0 of subwindow W0
            int n1 = windowSize; // length n0 of subwindow W1
            double u0 = 0; // sum of values in W0
            double u1 = sum; // sum of values in W1

            nodeCursor = bucketList.tail;
            int i = lastBucketIndex;
            do {
                for (int k = 0; k < nodeCursor->size; k++) {

                    if (i == 0 && k == nodeCursor->size - 1) {
                        quit = true;
                        break;
                    }

                    n0 += bucketSize(i); // length n0 of subwindow W0
                    n1 -= bucketSize(i); // length n0 of subwindow W1
                    u0 += nodeCursor->sum[k]; // sum of values in W0
                    u1 -= nodeCursor->sum[k]; // sum of values in W1

                    int mintMinWinLength = 5;
                    if(n0 >= mintMinWinLength && n1 >= mintMinWinLength && cutExpression(n0,n1,u0,u1)) {
                        reduceWidth  = true;
                        change = true;

                        if (windowSize > 0) {
                            deleteElement(); // when change is detected, delete last bucket and reduce windowSize
                            quit = true;
                            break;
                        }
                    }
                }

                nodeCursor = nodeCursor->prev;
                i--;
            } while (!quit && nodeCursor != nullptr);
        }//End While
    }//End if
			   
    return change;
}

bool Adwin::cutExpression(int N0, int N1, const double& u0, const double& u1){
    auto n0 = double(N0);
    auto n1 = double(N1);
    auto n  = double(windowSize); // length of window windowSize
    double diff = u0/n0 - u1/n1; // difference of the average of values in W0 and W1

    double var = variance / windowSize; // variance of window
    double deltaDash = log(2.0 * log(n) / DELTA); // delta

    int mintMinWinLength = 5;
    double harmonicMean = ((double) 1 / ((n0 - mintMinWinLength + 1))) + ((double) 1 / ((n1 - mintMinWinLength + 1))); // harmonic mean of n0 and n1

    double epsilon = sqrt(2 * harmonicMean * var * deltaDash) + (double) 2 / 3 * deltaDash * harmonicMean; // threshold epsilon cut

    if (fabs(diff) > epsilon) // when observed average in both subwindows differs more than threshold epsilon cut, change occurred
        return true;
    else
        return false;
}

void Adwin::deleteElement(){
    // update statistics
    ListNode* Node;
    Node = bucketList.tail;
    int n1 = bucketSize(lastBucketIndex);
    windowSize -= n1;
    sum -= Node->sum[0];
    double u1 = Node->sum[0]/n1;
    double incVariance = Node->variance[0]+ n1 * windowSize * (u1 - sum / windowSize) * (u1 - sum / windowSize) / (n1 + windowSize);
    variance -= incVariance;

    // delete Bucket
    Node->dropFront();
    numberOfBuckets--;
    if (Node->size == 0){
        bucketList.removeFromTail();
        lastBucketIndex--;
    }
}

double Adwin::getEstimation() const{
    if (windowSize > 0) {
        return sum / double(windowSize);
    } else {
        return 0;
    }
}    

void Adwin::print() const{
    ListNode* it;
    it = bucketList.tail;
    if (it == nullptr) printf(" It NULL");

    int i = lastBucketIndex;
    do {
        for (int k = it->size-1; k >= 0; k--) {
            printf(" %d [%1.4f de %d],", i, it->sum[k], bucketSize(i));
        }
        printf("\n");
        it = it->prev;
        i--;
    } while (it != nullptr);
}

} // namespace NES::Runtime::Execution