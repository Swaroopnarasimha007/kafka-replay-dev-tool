package dev.replay;

import java.util.*;

/** Worker contract only. No step-2 endpoint can advance an approved job. */
final class JobStates {
    enum State { APPROVED, CANARY_RUNNING, AWAITING_RELEASE, RUNNING, COMPLETED,
        COMPLETED_WITH_EXCEPTIONS, PAUSED, BLOCKED, FAILED }
    static final Map<State,Set<State>> TRANSITIONS = Map.of(
        State.APPROVED, Set.of(State.CANARY_RUNNING,State.RUNNING,State.BLOCKED,State.FAILED),
        State.CANARY_RUNNING, Set.of(State.AWAITING_RELEASE,State.BLOCKED,State.FAILED),
        State.AWAITING_RELEASE, Set.of(State.RUNNING,State.BLOCKED,State.FAILED),
        State.RUNNING, Set.of(State.PAUSED,State.BLOCKED,State.FAILED,State.COMPLETED,State.COMPLETED_WITH_EXCEPTIONS),
        State.PAUSED, Set.of(State.RUNNING,State.BLOCKED,State.FAILED),
        State.BLOCKED, Set.of(State.PAUSED,State.AWAITING_RELEASE,State.FAILED));

    static void require(State from, State to) {
        if (!TRANSITIONS.getOrDefault(from,Set.of()).contains(to)) throw new Planner.Conflict("Invalid job state transition");
    }
    // Step 3 must call this under the job row lock: repeating a successful release is a no-op.
    static State release(State current) {
        if (current == State.RUNNING || current == State.COMPLETED || current == State.COMPLETED_WITH_EXCEPTIONS) return current;
        require(current, State.RUNNING);
        if (current != State.AWAITING_RELEASE) throw new Planner.Conflict("Canary must await release");
        return State.RUNNING;
    }
    static State release(State current, boolean releaseAlreadyRecorded) {
        // A retry after the worker later pauses/blocks must never resume it as a side effect.
        return releaseAlreadyRecorded ? current : release(current);
    }
}
