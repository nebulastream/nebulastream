----------------------- MODULE SequenceShredderCompleted -----------------------
(*
  MODEL OVERVIEW
  ==============
  Models the concurrent sequence range assignment with "completed buffer" concept.

  BUFFERS
  Buffer 0 is a sentinel (always available, completed, has delimiter).
  Buffers 1..NumBuffers are real buffers processed by worker threads.
  The last buffer is always completed (source flushes cleanly at the end).

  Each buffer has static properties (known from content):
    - HasDelimiter:      contains at least one record delimiter
    - HasCompleteTuples: has >= 1 record fully within it (>= 2 delimiters)
    - IsCompleted:       last written byte is a delimiter (no trailing ST)

  WORKERS
  NumWorkers worker threads pick unprocessed buffers from a queue and process
  them. Each worker repeatedly: picks a buffer, inserts it into the spanning
  tuple buffer, tries to claim spanning tuples (CAS), computes its range,
  then picks the next buffer. This decouples thread count from buffer count.

  SPANNING TUPLES & CLAIMING
  A spanning tuple forms between consecutive delimiter buffers.
  Workers race to claim STs via CAS. Each ST is claimed by exactly one worker.

  RANGE ASSIGNMENT (the "completed" strategy)
  - Uncompleted buffer: emits fractured sub-range [N.0, N.1) for its complete
    tuples only. Leaves [N.1, ...) for whoever claims the trailing ST.
  - Completed buffer: no trailing ST. Range ends at clean boundary S(N+1).
  - Leading ST claimant: starts range at F(leftEndpoint) — after the left
    endpoint's fractured emission.
  - Trailing ST claimant: extends range through middles to the right endpoint.

  INVARIANTS (checked when all buffers are done)
  1. NoOverlap:     No two emitting buffers have overlapping ranges
  2. NoCoverageGap: Every buffer position is covered by some emitting range
*)

EXTENDS Integers, FiniteSets, TLC

Min(S) == CHOOSE x \in S : \A y \in S : x <= y
Max(S) == CHOOSE x \in S : \A y \in S : x >= y

CONSTANTS
    NumBuffers,         \* Real buffers: 1..NumBuffers (buffer 0 is sentinel)
    NumWorkers,         \* Number of worker threads
    HasDelimiter,       \* [0..NumBuffers -> BOOLEAN]
    HasCompleteTuples,  \* [0..NumBuffers -> BOOLEAN]
    IsCompleted         \* [0..NumBuffers -> BOOLEAN]

ASSUME NumBuffers \in Nat \ {0}
ASSUME NumWorkers \in Nat \ {0}
ASSUME HasDelimiter \in [0..NumBuffers -> BOOLEAN]
ASSUME HasCompleteTuples \in [0..NumBuffers -> BOOLEAN]
ASSUME IsCompleted \in [0..NumBuffers -> BOOLEAN]

\* Buffer 0 (sentinel): always completed, has delimiter, no complete tuples
ASSUME HasDelimiter[0] = TRUE
ASSUME HasCompleteTuples[0] = FALSE
ASSUME IsCompleted[0] = TRUE

\* Last buffer is always completed
ASSUME IsCompleted[NumBuffers] = TRUE
ASSUME HasDelimiter[NumBuffers] = TRUE

\* Structural constraints
ASSUME \A i \in 0..NumBuffers : HasCompleteTuples[i] => HasDelimiter[i]
ASSUME \A i \in 0..NumBuffers : IsCompleted[i] => HasDelimiter[i]

\* ---------------------------------------------------------------------------
\* SCALING: fractured ranges as integers scaled by 10.
\* Buffer i's range [i, i+1) becomes [i*10, (i+1)*10).
\* Fractured boundary i.1 = i*10 + 1.
\* ---------------------------------------------------------------------------
S(n) == n * 10
F(n) == n * 10 + 1

\* ---------------------------------------------------------------------------
\* STATIC TOPOLOGY (independent of availability)
\* ---------------------------------------------------------------------------
DelimPos == {i \in 0..NumBuffers : HasDelimiter[i]}

StaticNextDelim(p) == LET after == {d \in DelimPos : d > p}
                      IN IF after # {} THEN Min(after) ELSE 0

AllPossibleSTs == {<<d, StaticNextDelim(d)>> : d \in {d2 \in DelimPos : StaticNextDelim(d2) # 0}}

MiddleBuffers(st) == {m \in 1..NumBuffers : st[1] < m /\ m < st[2]}

(* --algorithm SequenceShredderCompleted

variables
    \* Buffer 0 (sentinel) is always available
    available = [i \in 0..NumBuffers |-> i = 0];
    stClaimed = [st \in AllPossibleSTs |-> 0];
    eStart = [b \in 0..NumBuffers |-> 0];
    eEnd = [b \in 0..NumBuffers |-> 0];
    emitting = [b \in 0..NumBuffers |-> FALSE];
    processed = [b \in 0..NumBuffers |-> b = 0];  \* sentinel already processed

define
    \* Dynamic topology: only considers available buffers
    AllAvailable(p, q) == \A k \in Min({p,q})..Max({p,q}) : available[k]

    NextDelim(p) == LET after == {d \in DelimPos : d > p /\ AllAvailable(p, d)}
                    IN IF after # {} THEN Min(after) ELSE 0

    PrevDelim(p) == LET before == {d \in DelimPos : d < p /\ AllAvailable(p, d)}
                    IN IF before # {} THEN Max(before) ELSE 0

    SpanningTuples == {<<d, NextDelim(d)>> : d \in {d2 \in DelimPos : NextDelim(d2) # 0}}

    LeftST(b) == IF PrevDelim(b) # 0 \/ (PrevDelim(b) = 0 /\ available[0])
                 THEN <<PrevDelim(b), b>>
                 ELSE <<-1, -1>>
    RightST(b) == IF NextDelim(b) # 0 THEN <<b, NextDelim(b)>> ELSE <<-1, -1>>
    ContainingST(b) == IF (PrevDelim(b) # 0 \/ (PrevDelim(b) = 0 /\ available[0])) /\ NextDelim(b) # 0
                       THEN <<PrevDelim(b), NextDelim(b)>>
                       ELSE <<-1, -1>>

    \* Effective range computation
    EffRange(b, leading, trailing, nodelim) ==
        LET lst == LeftST(b)
            rst == RightST(b)
            cst == ContainingST(b)

            \* START: if claimed leading ST [A, ..., self], start at F(A)
            \* (A is uncompleted — otherwise no trailing ST would exist from A)
            \* Include middles of leading ST
            myStart ==
                IF leading /\ lst \in AllPossibleSTs
                THEN LET midStarts == {S(m) : m \in MiddleBuffers(lst)}
                     IN Min({F(lst[1])} \cup midStarts \cup {S(b)})
                ELSE IF nodelim /\ cst \in AllPossibleSTs
                THEN Min({S(m) : m \in MiddleBuffers(cst)} \cup {S(b)})
                ELSE S(b)

            \* END: depends on completed status and trailing ST
            \* - Claimed trailing [self, ..., B]: extend through middles to S(B)
            \* - Completed, no trailing claimed: clean boundary S(self+1)
            \* - Uncompleted, no trailing claimed: fractured F(self)
            myEnd ==
                IF trailing /\ rst \in AllPossibleSTs
                THEN LET midEnds == {S(m) + 10 : m \in MiddleBuffers(rst)}
                     IN Max({S(b) + 10} \cup midEnds \cup {S(rst[2])})
                ELSE IF nodelim /\ cst \in AllPossibleSTs
                THEN Max({S(m) + 10 : m \in MiddleBuffers(cst)} \cup {S(b) + 10})
                ELSE IF IsCompleted[b]
                THEN S(b) + 10
                ELSE F(b)
        IN <<myStart, myEnd>>

    \* --- INVARIANTS ---
    EmittingSet == {b \in 0..NumBuffers : emitting[b]}
    AllDone == \A b \in 1..NumBuffers : processed[b]

    NoOverlap == \A b1, b2 \in EmittingSet :
        b1 # b2 => (eEnd[b1] <= eStart[b2] \/ eEnd[b2] <= eStart[b1])

    \* Every real buffer position (1..NumBuffers) must be covered
    NoCoverageGap == \A i \in 1..NumBuffers :
        \E b \in EmittingSet : eStart[b] <= S(i) /\ S(i) < eEnd[b]

    RangeInvariant == AllDone => (NoOverlap /\ NoCoverageGap)
end define;

\* Worker threads pick unprocessed buffers and process them
fair process worker \in 1..NumWorkers
variables
    buf = 0;      \* current buffer being processed
    cl = FALSE;
    ct = FALSE;
    cn = FALSE;
begin
    PickBuffer:
        \* Pick any unprocessed buffer (non-deterministic choice)
        either
            with b \in {b2 \in 1..NumBuffers : ~processed[b2]} do
                buf := b;
            end with;
        or
            \* All buffers processed — worker is done
            if \A b \in 1..NumBuffers : processed[b] then
                goto WorkerDone;
            else
                goto PickBuffer;
            end if;
        end either;

    InsertBuffer:
        available[buf] := TRUE;

    TryLeading:
        if HasDelimiter[buf] /\ LeftST(buf) \in AllPossibleSTs /\ stClaimed[LeftST(buf)] = 0 then
            stClaimed[LeftST(buf)] := buf ||
            cl := TRUE;
        end if;

    TryTrailing:
        if HasDelimiter[buf] /\ RightST(buf) \in AllPossibleSTs /\ stClaimed[RightST(buf)] = 0 then
            stClaimed[RightST(buf)] := buf ||
            ct := TRUE;
        end if;

    TryNoDelim:
        if ~HasDelimiter[buf] /\ ContainingST(buf) \in AllPossibleSTs /\ stClaimed[ContainingST(buf)] = 0 then
            stClaimed[ContainingST(buf)] := buf ||
            cn := TRUE;
        end if;

    Finish:
        if HasCompleteTuples[buf] \/ cl \/ ct \/ cn then
            eStart[buf] := EffRange(buf, cl, ct, cn)[1] ||
            eEnd[buf] := EffRange(buf, cl, ct, cn)[2] ||
            emitting[buf] := TRUE ||
            processed[buf] := TRUE;
        else
            eStart[buf] := 0 ||
            eEnd[buf] := 0 ||
            emitting[buf] := FALSE ||
            processed[buf] := TRUE;
        end if;

    Reset:
        cl := FALSE ||
        ct := FALSE ||
        cn := FALSE ||
        buf := 0;
        goto PickBuffer;

    WorkerDone:
        skip;
end process;

end algorithm; *)

\* BEGIN TRANSLATION
VARIABLES available, stClaimed, eStart, eEnd, emitting, processed, pc

(* define statement *)
AllAvailable(p, q) == \A k \in Min({p,q})..Max({p,q}) : available[k]

NextDelim(p) == LET after == {d \in DelimPos : d > p /\ AllAvailable(p, d)}
                IN IF after # {} THEN Min(after) ELSE 0

PrevDelim(p) == LET before == {d \in DelimPos : d < p /\ AllAvailable(p, d)}
                IN IF before # {} THEN Max(before) ELSE 0

SpanningTuples == {<<d, NextDelim(d)>> : d \in {d2 \in DelimPos : NextDelim(d2) # 0}}

LeftST(b) == IF PrevDelim(b) # 0 \/ (PrevDelim(b) = 0 /\ available[0])
             THEN <<PrevDelim(b), b>>
             ELSE <<-1, -1>>
RightST(b) == IF NextDelim(b) # 0 THEN <<b, NextDelim(b)>> ELSE <<-1, -1>>
ContainingST(b) == IF (PrevDelim(b) # 0 \/ (PrevDelim(b) = 0 /\ available[0])) /\ NextDelim(b) # 0
                   THEN <<PrevDelim(b), NextDelim(b)>>
                   ELSE <<-1, -1>>


EffRange(b, leading, trailing, nodelim) ==
    LET lst == LeftST(b)
        rst == RightST(b)
        cst == ContainingST(b)




        myStart ==
            IF leading /\ lst \in AllPossibleSTs
            THEN LET midStarts == {S(m) : m \in MiddleBuffers(lst)}
                 IN Min({F(lst[1])} \cup midStarts \cup {S(b)})
            ELSE IF nodelim /\ cst \in AllPossibleSTs
            THEN Min({S(m) : m \in MiddleBuffers(cst)} \cup {S(b)})
            ELSE S(b)





        myEnd ==
            IF trailing /\ rst \in AllPossibleSTs
            THEN LET midEnds == {S(m) + 10 : m \in MiddleBuffers(rst)}
                 IN Max({S(b) + 10} \cup midEnds \cup {S(rst[2])})
            ELSE IF nodelim /\ cst \in AllPossibleSTs
            THEN Max({S(m) + 10 : m \in MiddleBuffers(cst)} \cup {S(b) + 10})
            ELSE IF IsCompleted[b]
            THEN S(b) + 10
            ELSE F(b)
    IN <<myStart, myEnd>>


EmittingSet == {b \in 0..NumBuffers : emitting[b]}
AllDone == \A b \in 1..NumBuffers : processed[b]

NoOverlap == \A b1, b2 \in EmittingSet :
    b1 # b2 => (eEnd[b1] <= eStart[b2] \/ eEnd[b2] <= eStart[b1])


NoCoverageGap == \A i \in 1..NumBuffers :
    \E b \in EmittingSet : eStart[b] <= S(i) /\ S(i) < eEnd[b]

RangeInvariant == AllDone => (NoOverlap /\ NoCoverageGap)

VARIABLES buf, cl, ct, cn

vars == << available, stClaimed, eStart, eEnd, emitting, processed, pc, buf, 
           cl, ct, cn >>

ProcSet == (1..NumWorkers)

Init == (* Global variables *)
        /\ available = [i \in 0..NumBuffers |-> i = 0]
        /\ stClaimed = [st \in AllPossibleSTs |-> 0]
        /\ eStart = [b \in 0..NumBuffers |-> 0]
        /\ eEnd = [b \in 0..NumBuffers |-> 0]
        /\ emitting = [b \in 0..NumBuffers |-> FALSE]
        /\ processed = [b \in 0..NumBuffers |-> b = 0]
        (* Process worker *)
        /\ buf = [self \in 1..NumWorkers |-> 0]
        /\ cl = [self \in 1..NumWorkers |-> FALSE]
        /\ ct = [self \in 1..NumWorkers |-> FALSE]
        /\ cn = [self \in 1..NumWorkers |-> FALSE]
        /\ pc = [self \in ProcSet |-> "PickBuffer"]

PickBuffer(self) == /\ pc[self] = "PickBuffer"
                    /\ \/ /\ \E b \in {b2 \in 1..NumBuffers : ~processed[b2]}:
                               buf' = [buf EXCEPT ![self] = b]
                          /\ pc' = [pc EXCEPT ![self] = "InsertBuffer"]
                       \/ /\ IF \A b \in 1..NumBuffers : processed[b]
                                THEN /\ pc' = [pc EXCEPT ![self] = "WorkerDone"]
                                ELSE /\ pc' = [pc EXCEPT ![self] = "PickBuffer"]
                          /\ buf' = buf
                    /\ UNCHANGED << available, stClaimed, eStart, eEnd, 
                                    emitting, processed, cl, ct, cn >>

InsertBuffer(self) == /\ pc[self] = "InsertBuffer"
                      /\ available' = [available EXCEPT ![buf[self]] = TRUE]
                      /\ pc' = [pc EXCEPT ![self] = "TryLeading"]
                      /\ UNCHANGED << stClaimed, eStart, eEnd, emitting, 
                                      processed, buf, cl, ct, cn >>

TryLeading(self) == /\ pc[self] = "TryLeading"
                    /\ IF HasDelimiter[buf[self]] /\ LeftST(buf[self]) \in AllPossibleSTs /\ stClaimed[LeftST(buf[self])] = 0
                          THEN /\ /\ cl' = [cl EXCEPT ![self] = TRUE]
                                  /\ stClaimed' = [stClaimed EXCEPT ![LeftST(buf[self])] = buf[self]]
                          ELSE /\ TRUE
                               /\ UNCHANGED << stClaimed, cl >>
                    /\ pc' = [pc EXCEPT ![self] = "TryTrailing"]
                    /\ UNCHANGED << available, eStart, eEnd, emitting, 
                                    processed, buf, ct, cn >>

TryTrailing(self) == /\ pc[self] = "TryTrailing"
                     /\ IF HasDelimiter[buf[self]] /\ RightST(buf[self]) \in AllPossibleSTs /\ stClaimed[RightST(buf[self])] = 0
                           THEN /\ /\ ct' = [ct EXCEPT ![self] = TRUE]
                                   /\ stClaimed' = [stClaimed EXCEPT ![RightST(buf[self])] = buf[self]]
                           ELSE /\ TRUE
                                /\ UNCHANGED << stClaimed, ct >>
                     /\ pc' = [pc EXCEPT ![self] = "TryNoDelim"]
                     /\ UNCHANGED << available, eStart, eEnd, emitting, 
                                     processed, buf, cl, cn >>

TryNoDelim(self) == /\ pc[self] = "TryNoDelim"
                    /\ IF ~HasDelimiter[buf[self]] /\ ContainingST(buf[self]) \in AllPossibleSTs /\ stClaimed[ContainingST(buf[self])] = 0
                          THEN /\ /\ cn' = [cn EXCEPT ![self] = TRUE]
                                  /\ stClaimed' = [stClaimed EXCEPT ![ContainingST(buf[self])] = buf[self]]
                          ELSE /\ TRUE
                               /\ UNCHANGED << stClaimed, cn >>
                    /\ pc' = [pc EXCEPT ![self] = "Finish"]
                    /\ UNCHANGED << available, eStart, eEnd, emitting, 
                                    processed, buf, cl, ct >>

Finish(self) == /\ pc[self] = "Finish"
                /\ IF HasCompleteTuples[buf[self]] \/ cl[self] \/ ct[self] \/ cn[self]
                      THEN /\ /\ eEnd' = [eEnd EXCEPT ![buf[self]] = EffRange(buf[self], cl[self], ct[self], cn[self])[2]]
                              /\ eStart' = [eStart EXCEPT ![buf[self]] = EffRange(buf[self], cl[self], ct[self], cn[self])[1]]
                              /\ emitting' = [emitting EXCEPT ![buf[self]] = TRUE]
                              /\ processed' = [processed EXCEPT ![buf[self]] = TRUE]
                      ELSE /\ /\ eEnd' = [eEnd EXCEPT ![buf[self]] = 0]
                              /\ eStart' = [eStart EXCEPT ![buf[self]] = 0]
                              /\ emitting' = [emitting EXCEPT ![buf[self]] = FALSE]
                              /\ processed' = [processed EXCEPT ![buf[self]] = TRUE]
                /\ pc' = [pc EXCEPT ![self] = "Reset"]
                /\ UNCHANGED << available, stClaimed, buf, cl, ct, cn >>

Reset(self) == /\ pc[self] = "Reset"
               /\ /\ buf' = [buf EXCEPT ![self] = 0]
                  /\ cl' = [cl EXCEPT ![self] = FALSE]
                  /\ cn' = [cn EXCEPT ![self] = FALSE]
                  /\ ct' = [ct EXCEPT ![self] = FALSE]
               /\ pc' = [pc EXCEPT ![self] = "PickBuffer"]
               /\ UNCHANGED << available, stClaimed, eStart, eEnd, emitting, 
                               processed >>

WorkerDone(self) == /\ pc[self] = "WorkerDone"
                    /\ TRUE
                    /\ pc' = [pc EXCEPT ![self] = "Done"]
                    /\ UNCHANGED << available, stClaimed, eStart, eEnd, 
                                    emitting, processed, buf, cl, ct, cn >>

worker(self) == PickBuffer(self) \/ InsertBuffer(self) \/ TryLeading(self)
                   \/ TryTrailing(self) \/ TryNoDelim(self) \/ Finish(self)
                   \/ Reset(self) \/ WorkerDone(self)

(* Allow infinite stuttering to prevent deadlock on termination. *)
Terminating == /\ \A self \in ProcSet: pc[self] = "Done"
               /\ UNCHANGED vars

Next == (\E self \in 1..NumWorkers: worker(self))
           \/ Terminating

Spec == /\ Init /\ [][Next]_vars
        /\ \A self \in 1..NumWorkers : WF_vars(worker(self))

Termination == <>(\A self \in ProcSet: pc[self] = "Done")

\* END TRANSLATION

\* ===========================================================================
\* MODEL CONFIGURATIONS (to be explored by TLC)
\* ===========================================================================

\* Example: D=delimiter, N=no-delim, C=completed, U=uncompleted
\*   Buf 0: sentinel (D, C, always available)
\*   Buf 1: D, complete tuples, uncompleted
\*   Buf 2: D, complete tuples, completed
\*   Buf 3: D, 0 complete tuples, uncompleted
\*   Buf 4: N
\*   Buf 5: D, complete tuples, completed
HasDelimiterDef == [i \in 0..5 |-> i \in {0, 1, 2, 3, 5}]
HasCompleteTuplesDef == [i \in 0..5 |-> i \in {1, 2, 5}]
IsCompletedDef == [i \in 0..5 |-> i \in {0, 2, 5}]

=============================================================================
