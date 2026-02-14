--------------------------- MODULE DeltaHandshake ---------------------------
(*
 * Model of the three-way handshake protocol used by the DeltaOperatorHandler
 * to maintain cross-buffer state in a streaming query engine.
 *
 * PROBLEM: The delta operator computes current - previous for each record.
 * Records arrive in multiple buffers (e.g. buffer 1 has records 1,2,3 and
 * buffer 2 has records 4,5,6). Buffers are processed by different threads
 * concurrently, so buffer 2 might start before buffer 1 finishes.
 * The first record of each buffer needs the LAST value from the previous
 * buffer to compute its delta, but that value might not be available yet.
 *
 * SOLUTION: A shared handler with two maps, and three code paths that cover
 * every possible ordering of concurrent buffer processing:
 *
 *   Case A (Open):  Buffer N checks in open() if buffer N-1 already stored
 *                   its last value. If yes, use it directly.
 *
 *   Case B (Close): Buffer N-1 checks in close() if buffer N already stored
 *                   a pending first record. If yes, compute and emit the
 *                   deferred delta on behalf of buffer N.
 *
 *   Case C (Execute): Buffer N checks in execute() — when processing its
 *                   first record — if buffer N-1 has stored its last value
 *                   SINCE open() ran (the predecessor closed between our
 *                   open and our first execute). If yes, resolve immediately.
 *                   Otherwise, store our first record as "pending" so the
 *                   predecessor can resolve it in Case B.
 *
 * DATA MODEL: Each buffer contains RecordsPerBuffer records with sequential
 * integer values. Buffer 1 = (1,2,3), buffer 2 = (4,5,6), buffer 3 = (7,8,9).
 * A delta is represented as a <<previous, current>> pair. The expected output
 * is all consecutive pairs: <<1,2>>, <<2,3>>, <<3,4>>, ..., <<8,9>>.
 * Only the very first record of the entire stream (record 1) has no delta.
 *
 * LABELS: Each label is an atomic step. Labels are annotated with:
 *   [GLOBAL - handler lock]  — accesses shared handler state under the lock.
 *                               Other threads cannot interleave within this step.
 *   [THREAD-LOCAL]           — only reads/writes per-buffer local state.
 *                               No lock held; other threads can run before/after.
 *   [GHOST]                  — updates verification-only state (emittedPairs).
 *
 * WHAT TLC CHECKS:
 *   OnlyCorrectDeltas (invariant):  No wrong pair is ever emitted.
 *   EventuallyComplete (liveness):  All expected pairs are eventually emitted.
 *   Termination (liveness):         All buffers eventually finish.
 *)
EXTENDS Integers, Sequences, FiniteSets, TLC

CONSTANTS NumBuffers, RecordsPerBuffer

(* --algorithm DeltaHandshake

variables
    (*
     * GLOBAL handler state — shared across all threads.
     * In C++ this is folly::Synchronized<HandlerState> with two maps.
     * All access is under the handler lock (one label = one lock scope).
     *)
    lastTuplePresent = {},
    lastTupleVal = [b \in 1..NumBuffers |-> 0],
    pendingPresent = {},
    pendingVal = [b \in 1..NumBuffers |-> 0],

    \* Ghost variable for verification only — not part of real implementation.
    emittedPairs = {};

define
    Val(b, j) == (b - 1) * RecordsPerBuffer + j
    Total == NumBuffers * RecordsPerBuffer
    ExpectedPairs == { <<i, i + 1>> : i \in 1..(Total - 1) }

    OnlyCorrectDeltas == emittedPairs \subseteq ExpectedPairs
    EventuallyComplete == <>(emittedPairs = ExpectedPairs)
end define;

fair process buffer \in 1..NumBuffers
variables
    (*
     * THREAD-LOCAL state — per-buffer, no synchronization needed.
     * In C++ these live in DeltaState (operator-local state) and stack variables.
     *)
    prev = 0,               \* DeltaState.previousRecord — last seen value
    hasPred = FALSE,         \* did open() find predecessor's last value?
    firstVal = 0,            \* value of this buffer's first record
    handshakeFound = FALSE,  \* did storePendingAndCheckPredecessor find predecessor?
    predVal = 0,             \* predecessor value returned by handler
    pendingFound = FALSE,    \* did storeLastAndCheckPending find successor's pending?
    pendingFirstVal = 0,     \* successor's first record value from handler
    j = 2;                   \* loop counter for records 2..RecordsPerBuffer
begin

    (* ================================================================
     * OPEN PHASE — DeltaPhysicalOperator::open()
     * ================================================================ *)

    Open_LoadPredecessor:
        (* [GLOBAL - handler lock] loadPredecessor(seqNum)
         * Under the handler lock: check if the previous buffer already
         * stored its last value in lastTuples. If found, copy it to our
         * local scratch buffer and erase the entry (consumed). *)
        if self > 1 /\ (self - 1) \in lastTuplePresent then
            prev := lastTupleVal[self - 1];
            lastTuplePresent := lastTuplePresent \ {self - 1};
            hasPred := TRUE;
        end if;

    Open_InitState:
        (* [THREAD-LOCAL] Initialize DeltaState.
         * In real code: create DeltaState, init previousRecord with min values,
         * then if loadPredecessor succeeded, deserialize predecessor values from
         * scratch buffer into previousRecord. Set isFirstRecord accordingly.
         * In the model: prev and hasPred are already set by the handler result. *)
        skip;

    (* ================================================================
     * EXECUTE PHASE — DeltaPhysicalOperator::execute() for first record
     * ================================================================ *)

    ExecuteFirst_ReadRecord:
        (* [THREAD-LOCAL] Read the first record's field values.
         * In real code: expr.readFunction.execute(record, arena) reads the
         * current value from the input record. Store it locally. *)
        firstVal := Val(self, 1);

    ExecuteFirst_Decide:
        (* [THREAD-LOCAL] Branch based on local state.
         * If predecessor was found in open (hasPred=TRUE), we can compute the
         * delta purely locally — no handler call needed. Same for buffer 1
         * which has no predecessor at all. *)
        if hasPred then
            \* Case A: predecessor found in open — keep prev (holds predecessor value)
            \* for the emit step, then update after emitting.
            goto ExecuteFirst_EmitLocal;
        elsif self = 1 then
            \* Buffer 1: drop first record (no predecessor in stream)
            prev := firstVal;
            goto ExecuteLoop;
        else
            \* Need handler — predecessor was not available in open.
            \* Update prev now for subsequent records regardless of handshake outcome.
            prev := firstVal;
        end if;

    ExecuteFirst_Handshake:
        (* [GLOBAL - handler lock] storePendingAndCheckPredecessor(seqNum, fullRecord, predDest)
         * Under the handler lock, atomically:
         *   1. Check if lastTuples[self-1] exists (predecessor closed since our open)
         *   2. If YES: copy predecessor values to predDest, erase entry, return true
         *   3. If NO:  store our first record in pendingFirstRecords[self], return false
         * This covers the gap between open() and execute() — Case C. *)
        if (self - 1) \in lastTuplePresent then
            \* Case C: predecessor closed between our open and first execute
            predVal := lastTupleVal[self - 1];
            lastTuplePresent := lastTuplePresent \ {self - 1};
            handshakeFound := TRUE;
        else
            \* Predecessor still not done — store our first record as pending (setup for Case B)
            pendingPresent := pendingPresent \union {self};
            pendingVal := [pendingVal EXCEPT ![self] = firstVal];
            handshakeFound := FALSE;
        end if;

    ExecuteFirst_ResolveHandshake:
        (* [THREAD-LOCAL + GHOST] Use the handler's return value.
         * If the handler found the predecessor (Case C), compute the delta
         * from the returned predecessor value and emit it.
         * If not found, the record is stored as pending — nothing to emit now. *)
        if handshakeFound then
            emittedPairs := emittedPairs \union {<<predVal, firstVal>>};
        end if;
        goto ExecuteLoop;

    ExecuteFirst_EmitLocal:
        (* [THREAD-LOCAL + GHOST] Emit the delta for Case A (predecessor found in open).
         * prev still holds the predecessor's last value; firstVal is our first record.
         * After emitting, update prev to the current record for subsequent deltas. *)
        emittedPairs := emittedPairs \union {<<prev, firstVal>>};
        prev := firstVal;

    (* ================================================================
     * EXECUTE PHASE — DeltaPhysicalOperator::execute() for records 2..N
     * ================================================================ *)

    ExecuteLoop:
        (* [THREAD-LOCAL + GHOST] Process remaining records.
         * Simple delta: current - previous. No handler interaction at all.
         * Each iteration is one atomic step — other threads can interleave
         * between records, which matches real concurrent execution. *)
        while j <= RecordsPerBuffer do
            emittedPairs := emittedPairs \union {<<prev, Val(self, j)>>};
            prev := Val(self, j);
            j := j + 1;
        end while;

    (* ================================================================
     * CLOSE PHASE — DeltaPhysicalOperator::close()
     * ================================================================ *)

    Close_StoreAndCheck:
        (* [GLOBAL - handler lock] storeLastAndCheckPending(seqNum, lastScratch, pendingDest)
         * Under the handler lock, atomically:
         *   1. Store our last delta-field values in lastTuples[self]
         *   2. Check if pendingFirstRecords[self+1] exists (successor waiting)
         *   3. If YES: copy successor's first record to pendingDest, erase, return true
         *   4. If NO:  return false *)
        lastTuplePresent := lastTuplePresent \union {self};
        lastTupleVal := [lastTupleVal EXCEPT ![self] = prev];
        if (self + 1) \in pendingPresent then
            \* Case B: successor stored its first record before we closed
            pendingFirstVal := pendingVal[self + 1];
            pendingPresent := pendingPresent \ {self + 1};
            pendingFound := TRUE;
        else
            pendingFound := FALSE;
        end if;

    Close_EmitDeferred:
        (* [THREAD-LOCAL + GHOST] If the handler found a pending successor record,
         * compute and emit the deferred delta on behalf of the successor.
         * In real code: deserialize all fields from pendingDest into a Record,
         * compute delta = firstValue - lastValue for each delta expression,
         * call executeChild to emit the record downstream. *)
        if pendingFound then
            emittedPairs := emittedPairs \union {<<prev, pendingFirstVal>>};
        end if;

end process;

end algorithm; *)

\* BEGIN TRANSLATION - the hash of the PlusCal algorithm is irrelevant
VARIABLES lastTuplePresent, lastTupleVal, pendingPresent, pendingVal, 
          emittedPairs, pc

(* define statement *)
Val(b, j) == (b - 1) * RecordsPerBuffer + j
Total == NumBuffers * RecordsPerBuffer
ExpectedPairs == { <<i, i + 1>> : i \in 1..(Total - 1) }

OnlyCorrectDeltas == emittedPairs \subseteq ExpectedPairs
EventuallyComplete == <>(emittedPairs = ExpectedPairs)

VARIABLES prev, hasPred, firstVal, handshakeFound, predVal, pendingFound, 
          pendingFirstVal, j

vars == << lastTuplePresent, lastTupleVal, pendingPresent, pendingVal, 
           emittedPairs, pc, prev, hasPred, firstVal, handshakeFound, predVal, 
           pendingFound, pendingFirstVal, j >>

ProcSet == (1..NumBuffers)

Init == (* Global variables *)
        /\ lastTuplePresent = {}
        /\ lastTupleVal = [b \in 1..NumBuffers |-> 0]
        /\ pendingPresent = {}
        /\ pendingVal = [b \in 1..NumBuffers |-> 0]
        /\ emittedPairs = {}
        (* Process buffer *)
        /\ prev = [self \in 1..NumBuffers |-> 0]
        /\ hasPred = [self \in 1..NumBuffers |-> FALSE]
        /\ firstVal = [self \in 1..NumBuffers |-> 0]
        /\ handshakeFound = [self \in 1..NumBuffers |-> FALSE]
        /\ predVal = [self \in 1..NumBuffers |-> 0]
        /\ pendingFound = [self \in 1..NumBuffers |-> FALSE]
        /\ pendingFirstVal = [self \in 1..NumBuffers |-> 0]
        /\ j = [self \in 1..NumBuffers |-> 2]
        /\ pc = [self \in ProcSet |-> "Open_LoadPredecessor"]

Open_LoadPredecessor(self) == /\ pc[self] = "Open_LoadPredecessor"
                              /\ IF self > 1 /\ (self - 1) \in lastTuplePresent
                                    THEN /\ prev' = [prev EXCEPT ![self] = lastTupleVal[self - 1]]
                                         /\ lastTuplePresent' = lastTuplePresent \ {self - 1}
                                         /\ hasPred' = [hasPred EXCEPT ![self] = TRUE]
                                    ELSE /\ TRUE
                                         /\ UNCHANGED << lastTuplePresent, 
                                                         prev, hasPred >>
                              /\ pc' = [pc EXCEPT ![self] = "Open_InitState"]
                              /\ UNCHANGED << lastTupleVal, pendingPresent, 
                                              pendingVal, emittedPairs, 
                                              firstVal, handshakeFound, 
                                              predVal, pendingFound, 
                                              pendingFirstVal, j >>

Open_InitState(self) == /\ pc[self] = "Open_InitState"
                        /\ TRUE
                        /\ pc' = [pc EXCEPT ![self] = "ExecuteFirst_ReadRecord"]
                        /\ UNCHANGED << lastTuplePresent, lastTupleVal, 
                                        pendingPresent, pendingVal, 
                                        emittedPairs, prev, hasPred, firstVal, 
                                        handshakeFound, predVal, pendingFound, 
                                        pendingFirstVal, j >>

ExecuteFirst_ReadRecord(self) == /\ pc[self] = "ExecuteFirst_ReadRecord"
                                 /\ firstVal' = [firstVal EXCEPT ![self] = Val(self, 1)]
                                 /\ pc' = [pc EXCEPT ![self] = "ExecuteFirst_Decide"]
                                 /\ UNCHANGED << lastTuplePresent, 
                                                 lastTupleVal, pendingPresent, 
                                                 pendingVal, emittedPairs, 
                                                 prev, hasPred, handshakeFound, 
                                                 predVal, pendingFound, 
                                                 pendingFirstVal, j >>

ExecuteFirst_Decide(self) == /\ pc[self] = "ExecuteFirst_Decide"
                             /\ IF hasPred[self]
                                   THEN /\ pc' = [pc EXCEPT ![self] = "ExecuteFirst_EmitLocal"]
                                        /\ prev' = prev
                                   ELSE /\ IF self = 1
                                              THEN /\ prev' = [prev EXCEPT ![self] = firstVal[self]]
                                                   /\ pc' = [pc EXCEPT ![self] = "ExecuteLoop"]
                                              ELSE /\ prev' = [prev EXCEPT ![self] = firstVal[self]]
                                                   /\ pc' = [pc EXCEPT ![self] = "ExecuteFirst_Handshake"]
                             /\ UNCHANGED << lastTuplePresent, lastTupleVal, 
                                             pendingPresent, pendingVal, 
                                             emittedPairs, hasPred, firstVal, 
                                             handshakeFound, predVal, 
                                             pendingFound, pendingFirstVal, j >>

ExecuteFirst_Handshake(self) == /\ pc[self] = "ExecuteFirst_Handshake"
                                /\ IF (self - 1) \in lastTuplePresent
                                      THEN /\ predVal' = [predVal EXCEPT ![self] = lastTupleVal[self - 1]]
                                           /\ lastTuplePresent' = lastTuplePresent \ {self - 1}
                                           /\ handshakeFound' = [handshakeFound EXCEPT ![self] = TRUE]
                                           /\ UNCHANGED << pendingPresent, 
                                                           pendingVal >>
                                      ELSE /\ pendingPresent' = (pendingPresent \union {self})
                                           /\ pendingVal' = [pendingVal EXCEPT ![self] = firstVal[self]]
                                           /\ handshakeFound' = [handshakeFound EXCEPT ![self] = FALSE]
                                           /\ UNCHANGED << lastTuplePresent, 
                                                           predVal >>
                                /\ pc' = [pc EXCEPT ![self] = "ExecuteFirst_ResolveHandshake"]
                                /\ UNCHANGED << lastTupleVal, emittedPairs, 
                                                prev, hasPred, firstVal, 
                                                pendingFound, pendingFirstVal, 
                                                j >>

ExecuteFirst_ResolveHandshake(self) == /\ pc[self] = "ExecuteFirst_ResolveHandshake"
                                       /\ IF handshakeFound[self]
                                             THEN /\ emittedPairs' = (emittedPairs \union {<<predVal[self], firstVal[self]>>})
                                             ELSE /\ TRUE
                                                  /\ UNCHANGED emittedPairs
                                       /\ pc' = [pc EXCEPT ![self] = "ExecuteLoop"]
                                       /\ UNCHANGED << lastTuplePresent, 
                                                       lastTupleVal, 
                                                       pendingPresent, 
                                                       pendingVal, prev, 
                                                       hasPred, firstVal, 
                                                       handshakeFound, predVal, 
                                                       pendingFound, 
                                                       pendingFirstVal, j >>

ExecuteFirst_EmitLocal(self) == /\ pc[self] = "ExecuteFirst_EmitLocal"
                                /\ emittedPairs' = (emittedPairs \union {<<prev[self], firstVal[self]>>})
                                /\ prev' = [prev EXCEPT ![self] = firstVal[self]]
                                /\ pc' = [pc EXCEPT ![self] = "ExecuteLoop"]
                                /\ UNCHANGED << lastTuplePresent, lastTupleVal, 
                                                pendingPresent, pendingVal, 
                                                hasPred, firstVal, 
                                                handshakeFound, predVal, 
                                                pendingFound, pendingFirstVal, 
                                                j >>

ExecuteLoop(self) == /\ pc[self] = "ExecuteLoop"
                     /\ IF j[self] <= RecordsPerBuffer
                           THEN /\ emittedPairs' = (emittedPairs \union {<<prev[self], Val(self, j[self])>>})
                                /\ prev' = [prev EXCEPT ![self] = Val(self, j[self])]
                                /\ j' = [j EXCEPT ![self] = j[self] + 1]
                                /\ pc' = [pc EXCEPT ![self] = "ExecuteLoop"]
                           ELSE /\ pc' = [pc EXCEPT ![self] = "Close_StoreAndCheck"]
                                /\ UNCHANGED << emittedPairs, prev, j >>
                     /\ UNCHANGED << lastTuplePresent, lastTupleVal, 
                                     pendingPresent, pendingVal, hasPred, 
                                     firstVal, handshakeFound, predVal, 
                                     pendingFound, pendingFirstVal >>

Close_StoreAndCheck(self) == /\ pc[self] = "Close_StoreAndCheck"
                             /\ lastTuplePresent' = (lastTuplePresent \union {self})
                             /\ lastTupleVal' = [lastTupleVal EXCEPT ![self] = prev[self]]
                             /\ IF (self + 1) \in pendingPresent
                                   THEN /\ pendingFirstVal' = [pendingFirstVal EXCEPT ![self] = pendingVal[self + 1]]
                                        /\ pendingPresent' = pendingPresent \ {self + 1}
                                        /\ pendingFound' = [pendingFound EXCEPT ![self] = TRUE]
                                   ELSE /\ pendingFound' = [pendingFound EXCEPT ![self] = FALSE]
                                        /\ UNCHANGED << pendingPresent, 
                                                        pendingFirstVal >>
                             /\ pc' = [pc EXCEPT ![self] = "Close_EmitDeferred"]
                             /\ UNCHANGED << pendingVal, emittedPairs, prev, 
                                             hasPred, firstVal, handshakeFound, 
                                             predVal, j >>

Close_EmitDeferred(self) == /\ pc[self] = "Close_EmitDeferred"
                            /\ IF pendingFound[self]
                                  THEN /\ emittedPairs' = (emittedPairs \union {<<prev[self], pendingFirstVal[self]>>})
                                  ELSE /\ TRUE
                                       /\ UNCHANGED emittedPairs
                            /\ pc' = [pc EXCEPT ![self] = "Done"]
                            /\ UNCHANGED << lastTuplePresent, lastTupleVal, 
                                            pendingPresent, pendingVal, prev, 
                                            hasPred, firstVal, handshakeFound, 
                                            predVal, pendingFound, 
                                            pendingFirstVal, j >>

buffer(self) == Open_LoadPredecessor(self) \/ Open_InitState(self)
                   \/ ExecuteFirst_ReadRecord(self)
                   \/ ExecuteFirst_Decide(self)
                   \/ ExecuteFirst_Handshake(self)
                   \/ ExecuteFirst_ResolveHandshake(self)
                   \/ ExecuteFirst_EmitLocal(self) \/ ExecuteLoop(self)
                   \/ Close_StoreAndCheck(self) \/ Close_EmitDeferred(self)

(* Allow infinite stuttering to prevent deadlock on termination. *)
Terminating == /\ \A self \in ProcSet: pc[self] = "Done"
               /\ UNCHANGED vars

Next == (\E self \in 1..NumBuffers: buffer(self))
           \/ Terminating

Spec == /\ Init /\ [][Next]_vars
        /\ \A self \in 1..NumBuffers : WF_vars(buffer(self))

Termination == <>(\A self \in ProcSet: pc[self] = "Done")

\* END TRANSLATION

=============================================================================
