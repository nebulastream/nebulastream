----------------------- MODULE SequenceShredderFracture -----------------------
(*
  MODEL: Always-Fracture Strategy
  ================================
  Every buffer fractures into three zones at its delimiters:
    [N.0, N.1)  = leading zone:   bytes before the first delimiter
    [N.1, N.2)  = complete zone:  complete tuples between first and last delimiter
    [N.2, (N+1).0) = trailing zone: bytes after the last delimiter

  A buffer can always emit its complete zone [N.1, N.2).
  No-delimiter buffers have no zones — their entire content [N.0, (N+1).0)
  belongs to a spanning tuple.

  Spanning tuples bridge from one delimiter buffer's trailing zone through
  any middle (no-delimiter) buffers to the next delimiter buffer's leading zone.
  ST <<A, B>> covers [A.2, B.1) — from A's trailing zone to B's complete zone.

  SCALING: N.0 = N*3, N.1 = N*3+1, N.2 = N*3+2

  Buffer 0 is a sentinel (always available, has delimiter).
  The last buffer always has a delimiter.
*)

EXTENDS Integers, FiniteSets, TLC

Min(S) == CHOOSE x \in S : \A y \in S : x <= y
Max(S) == CHOOSE x \in S : \A y \in S : x >= y

CONSTANTS
    NumBuffers,
    NumWorkers

ASSUME NumBuffers \in Nat \ {0}
ASSUME NumWorkers \in Nat \ {0}

\* HasDelimiter[i] \in {0, 1, 2}: 0=none, 1=exactly one delimiter, 2=two or more
\* Buffer 0 (sentinel) and last buffer always have a delimiter (2 = has complete tuples)
ValidDelimiterConfigs == {f \in [0..NumBuffers -> {0, 1, 2}] : f[0] = 2 /\ f[NumBuffers] = 2}

\* ---------------------------------------------------------------------------
\* SCALING
\* ---------------------------------------------------------------------------
Z0(n) == n * 3       \* leading zone start
Z1(n) == n * 3 + 1   \* complete zone start (first delimiter)
Z2(n) == n * 3 + 2   \* trailing zone start (after last delimiter)

MiddleBuffers(st) == {m \in 1..NumBuffers : st[1] < m /\ m < st[2]}

(* --algorithm SequenceShredderFracture

variables
    HasDelimiter \in ValidDelimiterConfigs;
    available = [i \in 0..NumBuffers |-> i = 0];
    stClaimed = [st \in AllPossibleSTs |-> 0];
    eStart = [b \in 0..NumBuffers |-> 0];
    eEnd = [b \in 0..NumBuffers |-> 0];
    emitting = [b \in 0..NumBuffers |-> FALSE];
    processed = [b \in 0..NumBuffers |-> b = 0];  \* picked from queue
    finished = [b \in 0..NumBuffers |-> b = 0];   \* completed Finish step

define
    \* Static topology (depends on HasDelimiter which is now a variable, but frozen after Init)
    DelimPos == {i \in 0..NumBuffers : HasDelimiter[i] > 0}

    StaticNextDelim(p) == LET after == {d \in DelimPos : d > p}
                          IN IF after # {} THEN Min(after) ELSE 0

    AllPossibleSTs == {<<d, StaticNextDelim(d)>> : d \in {d2 \in DelimPos : StaticNextDelim(d2) # 0}}

    \* Dynamic topology (depends on available)
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

    \* --- EFFECTIVE RANGE ---
    \*
    \* Delimiter buffer (default):     [Z1(self), Z2(self))     = complete zone
    \* + claimed leading ST <<A,self>>:  extend start to Z2(A)  = pick up A's trailing zone + middles
    \* + claimed trailing ST <<self,B>>: extend end to Z1(B)    = cover middles + B's leading zone
    \*
    \* No-delimiter buffer claiming ST <<A,B>>:
    \*   covers [Z2(A), Z1(B)) — the full span between the two endpoints' complete zones
    \*   (A's trailing zone, all middles including self, B's leading zone)

    \* When the ST endpoint has only 1 delimiter, its complete zone is empty.
    \* The claimant absorbs it: use Z1 instead of Z2 (or Z2 instead of Z1).
    \* When the endpoint has 2+ delimiters, leave its complete zone for it.
    \* For leading ST [A, ..., self] claimed by self:
    \* If A has 1 delimiter, its complete zone is empty — extend to Z1(A) to absorb it.
    \* If A has 2+ delimiters, leave its complete zone — extend to Z2(A).
    LeadingStart(endpoint) ==
        IF HasDelimiter[endpoint] = 1
        THEN Z1(endpoint)
        ELSE Z2(endpoint)

    EffRange(b, leading, trailing, nodelim) ==
        LET lst == LeftST(b)
            rst == RightST(b)
            cst == ContainingST(b)

            myStart ==
                IF leading /\ lst \in AllPossibleSTs
                THEN LeadingStart(lst[1])
                ELSE IF nodelim /\ cst \in AllPossibleSTs
                THEN LeadingStart(cst[1])
                ELSE Z1(b)

            \* A 1-delimiter buffer that didn't claim trailing yields its empty
            \* complete zone to the right neighbor (who absorbs via LeadingStart).
            defaultEnd == IF HasDelimiter[b] = 1 /\ ~trailing
                          THEN Z1(b)    \* yield empty complete zone
                          ELSE Z2(b)    \* normal complete zone end

            myEnd ==
                IF trailing /\ rst \in AllPossibleSTs
                THEN Z1(rst[2])
                ELSE IF nodelim /\ cst \in AllPossibleSTs
                THEN Z1(cst[2])
                ELSE defaultEnd

        IN <<myStart, myEnd>>

    \* --- INVARIANTS ---
    EmittingSet == {b \in 0..NumBuffers : emitting[b]}
    AllDone == \A b \in 1..NumBuffers : finished[b]

    NoOverlap == \A b1, b2 \in EmittingSet :
        b1 # b2 => (eEnd[b1] <= eStart[b2] \/ eEnd[b2] <= eStart[b1])

    \* Every scaled point from Z0(1) to Z2(NumBuffers)-1 must be covered
    \* Z2(last buffer) is the end of the last buffer's complete zone.
    \* The trailing zone of the last buffer has no data (no ST after it).
    NoCoverageGap == \A p \in Z0(1)..(Z2(NumBuffers) - 1) :
        \E b \in EmittingSet : eStart[b] <= p /\ p < eEnd[b]

    \* Every emitting buffer must produce at least 1 tuple
    AllEmittingNonEmpty == \A b \in EmittingSet :
        \/ HasDelimiter[b] >= 2                           \* has complete tuples
        \/ \E st \in AllPossibleSTs : stClaimed[st] = b   \* claimed a spanning tuple

    RangeInvariant == AllDone => (NoOverlap /\ NoCoverageGap /\ AllEmittingNonEmpty)
end define;

fair process worker \in 1..NumWorkers
variables
    buf = 0;
    cl = FALSE;
    ct = FALSE;
    cn = FALSE;
begin
    PickBuffer:
        either
            \* Atomically pick AND mark as processed (models taking from a queue)
            with b \in {b2 \in 1..NumBuffers : ~processed[b2]} do
                buf := b ||
                processed[b] := TRUE;
            end with;
        or
            if \A b \in 1..NumBuffers : processed[b] then
                goto WorkerDone;
            else
                goto PickBuffer;
            end if;
        end either;

    InsertBuffer:
        available[buf] := TRUE;

    TryLeading:
        if HasDelimiter[buf] > 0 /\ LeftST(buf) \in AllPossibleSTs /\ stClaimed[LeftST(buf)] = 0 then
            stClaimed[LeftST(buf)] := buf ||
            cl := TRUE;
        end if;

    TryTrailing:
        if HasDelimiter[buf] > 0 /\ RightST(buf) \in AllPossibleSTs /\ stClaimed[RightST(buf)] = 0 then
            stClaimed[RightST(buf)] := buf ||
            ct := TRUE;
        end if;

    TryNoDelim:
        if HasDelimiter[buf] = 0 /\ ContainingST(buf) \in AllPossibleSTs /\ stClaimed[ContainingST(buf)] = 0 then
            stClaimed[ContainingST(buf)] := buf ||
            cn := TRUE;
        end if;

    Finish:
        if HasDelimiter[buf] >= 2 \/ cl \/ ct \/ cn then
            eStart[buf] := EffRange(buf, cl, ct, cn)[1] ||
            eEnd[buf] := EffRange(buf, cl, ct, cn)[2] ||
            emitting[buf] := TRUE ||
            finished[buf] := TRUE;
        else
            eStart[buf] := 0 ||
            eEnd[buf] := 0 ||
            emitting[buf] := FALSE ||
            finished[buf] := TRUE;
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
VARIABLES HasDelimiter, available, stClaimed, eStart, eEnd, emitting, 
          processed, finished, pc

(* define statement *)
DelimPos == {i \in 0..NumBuffers : HasDelimiter[i] > 0}

StaticNextDelim(p) == LET after == {d \in DelimPos : d > p}
                      IN IF after # {} THEN Min(after) ELSE 0

AllPossibleSTs == {<<d, StaticNextDelim(d)>> : d \in {d2 \in DelimPos : StaticNextDelim(d2) # 0}}


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

















LeadingStart(endpoint) ==
    IF HasDelimiter[endpoint] = 1
    THEN Z1(endpoint)
    ELSE Z2(endpoint)

EffRange(b, leading, trailing, nodelim) ==
    LET lst == LeftST(b)
        rst == RightST(b)
        cst == ContainingST(b)

        myStart ==
            IF leading /\ lst \in AllPossibleSTs
            THEN LeadingStart(lst[1])
            ELSE IF nodelim /\ cst \in AllPossibleSTs
            THEN LeadingStart(cst[1])
            ELSE Z1(b)



        defaultEnd == IF HasDelimiter[b] = 1 /\ ~trailing
                      THEN Z1(b)
                      ELSE Z2(b)

        myEnd ==
            IF trailing /\ rst \in AllPossibleSTs
            THEN Z1(rst[2])
            ELSE IF nodelim /\ cst \in AllPossibleSTs
            THEN Z1(cst[2])
            ELSE defaultEnd

    IN <<myStart, myEnd>>


EmittingSet == {b \in 0..NumBuffers : emitting[b]}
AllDone == \A b \in 1..NumBuffers : finished[b]

NoOverlap == \A b1, b2 \in EmittingSet :
    b1 # b2 => (eEnd[b1] <= eStart[b2] \/ eEnd[b2] <= eStart[b1])




NoCoverageGap == \A p \in Z0(1)..(Z2(NumBuffers) - 1) :
    \E b \in EmittingSet : eStart[b] <= p /\ p < eEnd[b]


AllEmittingNonEmpty == \A b \in EmittingSet :
    \/ HasDelimiter[b] >= 2
    \/ \E st \in AllPossibleSTs : stClaimed[st] = b

RangeInvariant == AllDone => (NoOverlap /\ NoCoverageGap /\ AllEmittingNonEmpty)

VARIABLES buf, cl, ct, cn

vars == << HasDelimiter, available, stClaimed, eStart, eEnd, emitting, 
           processed, finished, pc, buf, cl, ct, cn >>

ProcSet == (1..NumWorkers)

Init == (* Global variables *)
        /\ HasDelimiter \in ValidDelimiterConfigs
        /\ available = [i \in 0..NumBuffers |-> i = 0]
        /\ stClaimed = [st \in AllPossibleSTs |-> 0]
        /\ eStart = [b \in 0..NumBuffers |-> 0]
        /\ eEnd = [b \in 0..NumBuffers |-> 0]
        /\ emitting = [b \in 0..NumBuffers |-> FALSE]
        /\ processed = [b \in 0..NumBuffers |-> b = 0]
        /\ finished = [b \in 0..NumBuffers |-> b = 0]
        (* Process worker *)
        /\ buf = [self \in 1..NumWorkers |-> 0]
        /\ cl = [self \in 1..NumWorkers |-> FALSE]
        /\ ct = [self \in 1..NumWorkers |-> FALSE]
        /\ cn = [self \in 1..NumWorkers |-> FALSE]
        /\ pc = [self \in ProcSet |-> "PickBuffer"]

PickBuffer(self) == /\ pc[self] = "PickBuffer"
                    /\ \/ /\ \E b \in {b2 \in 1..NumBuffers : ~processed[b2]}:
                               /\ buf' = [buf EXCEPT ![self] = b]
                               /\ processed' = [processed EXCEPT ![b] = TRUE]
                          /\ pc' = [pc EXCEPT ![self] = "InsertBuffer"]
                       \/ /\ IF \A b \in 1..NumBuffers : processed[b]
                                THEN /\ pc' = [pc EXCEPT ![self] = "WorkerDone"]
                                ELSE /\ pc' = [pc EXCEPT ![self] = "PickBuffer"]
                          /\ UNCHANGED <<processed, buf>>
                    /\ UNCHANGED << HasDelimiter, available, stClaimed, eStart, 
                                    eEnd, emitting, finished, cl, ct, cn >>

InsertBuffer(self) == /\ pc[self] = "InsertBuffer"
                      /\ available' = [available EXCEPT ![buf[self]] = TRUE]
                      /\ pc' = [pc EXCEPT ![self] = "TryLeading"]
                      /\ UNCHANGED << HasDelimiter, stClaimed, eStart, eEnd, 
                                      emitting, processed, finished, buf, cl, 
                                      ct, cn >>

TryLeading(self) == /\ pc[self] = "TryLeading"
                    /\ IF HasDelimiter[buf[self]] > 0 /\ LeftST(buf[self]) \in AllPossibleSTs /\ stClaimed[LeftST(buf[self])] = 0
                          THEN /\ /\ cl' = [cl EXCEPT ![self] = TRUE]
                                  /\ stClaimed' = [stClaimed EXCEPT ![LeftST(buf[self])] = buf[self]]
                          ELSE /\ TRUE
                               /\ UNCHANGED << stClaimed, cl >>
                    /\ pc' = [pc EXCEPT ![self] = "TryTrailing"]
                    /\ UNCHANGED << HasDelimiter, available, eStart, eEnd, 
                                    emitting, processed, finished, buf, ct, cn >>

TryTrailing(self) == /\ pc[self] = "TryTrailing"
                     /\ IF HasDelimiter[buf[self]] > 0 /\ RightST(buf[self]) \in AllPossibleSTs /\ stClaimed[RightST(buf[self])] = 0
                           THEN /\ /\ ct' = [ct EXCEPT ![self] = TRUE]
                                   /\ stClaimed' = [stClaimed EXCEPT ![RightST(buf[self])] = buf[self]]
                           ELSE /\ TRUE
                                /\ UNCHANGED << stClaimed, ct >>
                     /\ pc' = [pc EXCEPT ![self] = "TryNoDelim"]
                     /\ UNCHANGED << HasDelimiter, available, eStart, eEnd, 
                                     emitting, processed, finished, buf, cl, 
                                     cn >>

TryNoDelim(self) == /\ pc[self] = "TryNoDelim"
                    /\ IF HasDelimiter[buf[self]] = 0 /\ ContainingST(buf[self]) \in AllPossibleSTs /\ stClaimed[ContainingST(buf[self])] = 0
                          THEN /\ /\ cn' = [cn EXCEPT ![self] = TRUE]
                                  /\ stClaimed' = [stClaimed EXCEPT ![ContainingST(buf[self])] = buf[self]]
                          ELSE /\ TRUE
                               /\ UNCHANGED << stClaimed, cn >>
                    /\ pc' = [pc EXCEPT ![self] = "Finish"]
                    /\ UNCHANGED << HasDelimiter, available, eStart, eEnd, 
                                    emitting, processed, finished, buf, cl, ct >>

Finish(self) == /\ pc[self] = "Finish"
                /\ IF HasDelimiter[buf[self]] >= 2 \/ cl[self] \/ ct[self] \/ cn[self]
                      THEN /\ /\ eEnd' = [eEnd EXCEPT ![buf[self]] = EffRange(buf[self], cl[self], ct[self], cn[self])[2]]
                              /\ eStart' = [eStart EXCEPT ![buf[self]] = EffRange(buf[self], cl[self], ct[self], cn[self])[1]]
                              /\ emitting' = [emitting EXCEPT ![buf[self]] = TRUE]
                              /\ finished' = [finished EXCEPT ![buf[self]] = TRUE]
                      ELSE /\ /\ eEnd' = [eEnd EXCEPT ![buf[self]] = 0]
                              /\ eStart' = [eStart EXCEPT ![buf[self]] = 0]
                              /\ emitting' = [emitting EXCEPT ![buf[self]] = FALSE]
                              /\ finished' = [finished EXCEPT ![buf[self]] = TRUE]
                /\ pc' = [pc EXCEPT ![self] = "Reset"]
                /\ UNCHANGED << HasDelimiter, available, stClaimed, processed, 
                                buf, cl, ct, cn >>

Reset(self) == /\ pc[self] = "Reset"
               /\ /\ buf' = [buf EXCEPT ![self] = 0]
                  /\ cl' = [cl EXCEPT ![self] = FALSE]
                  /\ cn' = [cn EXCEPT ![self] = FALSE]
                  /\ ct' = [ct EXCEPT ![self] = FALSE]
               /\ pc' = [pc EXCEPT ![self] = "PickBuffer"]
               /\ UNCHANGED << HasDelimiter, available, stClaimed, eStart, 
                               eEnd, emitting, processed, finished >>

WorkerDone(self) == /\ pc[self] = "WorkerDone"
                    /\ TRUE
                    /\ pc' = [pc EXCEPT ![self] = "Done"]
                    /\ UNCHANGED << HasDelimiter, available, stClaimed, eStart, 
                                    eEnd, emitting, processed, finished, buf, 
                                    cl, ct, cn >>

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

=============================================================================
