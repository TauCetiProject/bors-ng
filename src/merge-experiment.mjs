// This timer selects a backend; the existing observational gates still decide
// when work can start and merge. Durable state survives Worker/Container restarts.
const HOUR = 3_600_000;
const TERMINAL = new Set(["complete", "aborted", "refused"]);
const iso = now => new Date(now).toISOString();

function parsePlan(variable) {
  if (!variable) return null;
  const plan = JSON.parse(variable.value);
  if (plan.enabled === false) return null;
  const schema = plan.mode === "bors_only" ? "tauceti-merge.experiment/v2" : "tauceti-merge.experiment/v1";
  if (plan.schema !== schema || !/^[A-Za-z0-9_-]{1,80}$/.test(plan.id || "") ||
      !["bors_then_queue", "bors_only"].includes(plan.mode ?? "bors_then_queue") ||
      plan.duration_hours !== 24 || !Number.isFinite(Date.parse(plan.created_at))) {
    throw new Error("Invalid experiment plan");
  }
  return plan;
}

export async function readExperimentState(storage) {
  // Keep the bors-only state separate: a rollback to the v1 controller must
  // see its old terminal state, not an active state it would restore to queue.
  const states = (await Promise.all([storage.get("merge-experiment"),
    storage.get("merge-experiment-bors-only")])).filter(Boolean);
  return states.sort((a, b) => Date.parse(b.requested_at) - Date.parse(a.requested_at))[0];
}

function usable(observation, backend, now) {
  if (!observation || observation.schema !== "tauceti-merge.observation/v1" ||
      observation.backend !== backend.value || observation.updated_at !== backend.updated_at) return false;
  const age = now - Date.parse(observation.observed_at);
  return age >= -10_000 && age <= 180_000 &&
    Number.isInteger(observation.github_count) && observation.github_count >= 0 &&
    Number.isInteger(observation.bors_count) && observation.bors_count >= 0;
}

export async function tickExperiment(storage, variables, now = Date.now()) {
  let state = await readExperimentState(storage);
  let plan;
  const planVariable = await variables.get("MERGE_EXPERIMENT");
  try { plan = parsePlan(planVariable); }
  catch (error) {
    if (!state || TERMINAL.has(state.phase)) throw error;
    // A malformed operator edit cancels an active experiment; it must not
    // disable the restore deadline by failing before the state is inspected.
    plan = null;
  }
  if (!plan && (!state || TERMINAL.has(state.phase))) return state || { phase: "inactive" };
  if (plan?.id === state?.id && TERMINAL.has(state.phase)) return state;
  let backend = await variables.get("MERGE_BACKEND");
  if (!backend || !["queue", "bors"].includes(backend.value) || !backend.updated_at) {
    throw new Error("Live merge backend unavailable");
  }
  const save = async () => {
    const key = state.plan.mode === "bors_only" ? "merge-experiment-bors-only" : "merge-experiment";
    await storage.put(key, state); return state;
  };

  // Persist the transition intent before sending PATCH. If its response is lost,
  // adopt the verified result next tick, or retry the unchanged prior value.
  const select = async value => {
    state.intent = { value, previous: { value: backend.value, updated_at: backend.updated_at }, at: iso(now) };
    await save();
    backend = await variables.select(value, state.intent.previous);
    if (!backend || backend.value !== value || !backend.updated_at) throw new Error("Backend write not verified");
    state.expected = { value, updated_at: backend.updated_at };
    delete state.intent;
    await save();
  };
  const owned = () => state.expected?.value === backend.value && state.expected.updated_at === backend.updated_at;
  const abort = async reason => {
    // A bors-only measurement does not own a return-to-queue transition, even
    // when measurement aborts. Live drain/admission guards still govern work.
    if (owned() && backend.value !== "queue" && state.plan.mode !== "bors_only") {
      state.abort_reason = reason;
      await save();
      await select("queue");
    }
    state.phase = "aborted";
    state.reason = reason;
    state.finished_at = iso(now);
    if (state.plan.mode === "bors_only" && state.bors_started_at) {
      state.bors_ended_at = iso(Math.min(now, Date.parse(state.bors_started_at) + 24 * HOUR));
    }
    console.error("merge experiment aborted", state.id, reason);
    delete state.abort_reason;
    return save();
  };

  if (state && !TERMINAL.has(state.phase)) {
    if (state.intent) {
      const intent = state.intent;
      if (backend.value === intent.value && Date.parse(backend.updated_at) >= Date.parse(intent.at) - 1000 &&
          backend.updated_at !== intent.previous.updated_at) {
        state.expected = { value: backend.value, updated_at: backend.updated_at };
        delete state.intent;
        await save();
      } else if (backend.value === intent.previous.value && backend.updated_at === intent.previous.updated_at) {
        await select(intent.value);
      } else {
        return abort("manual backend change during transition; selection preserved");
      }
    }
    if (!owned()) return abort("manual MERGE_BACKEND change; selection preserved");
    if (state.abort_reason) return abort(state.abort_reason);
    if (!plan || plan.id !== state.id || JSON.stringify(plan) !== JSON.stringify(state.plan)) {
      return abort("experiment plan disabled or changed");
    }
  } else {
    if (!plan) return state;
    state = { schema: "tauceti-merge.experiment-state/v1", id: plan.id, plan,
      phase: "draining_to_bors", requested_at: iso(now), expected: { value: backend.value, updated_at: backend.updated_at } };
    if (backend.value !== "queue" || now - Date.parse(plan.created_at) > 15 * 60_000 ||
        Date.parse(plan.created_at) > now + 60_000) {
      state.phase = "refused"; state.reason = "new experiment requires queue and a fresh plan";
      return save();
    }
    await select("bors");
  }

  // Absolute fail-safe is independent of bors health or observation availability.
  // Handoffs get at most six hours each; active windows get 24 hours each.
  if (now - Date.parse(state.plan.created_at) >= 72 * HOUR) return abort("absolute experiment deadline");
  if (["draining_to_bors", "draining_to_queue"].includes(state.phase)) {
    const requested = state.phase === "draining_to_bors" ? state.requested_at : state.queue_requested_at;
    if (now - Date.parse(requested) >= 6 * HOUR) return abort("handoff exceeded six hours");
  }
  const observation = await storage.get("merge-experiment-observation");
  const fresh = usable(observation, backend, now);
  if (fresh && observation.github_count > 0 && observation.bors_count > 0) {
    state.overlap_samples = (state.overlap_samples || 0) + (state.last_overlap_at !== observation.observed_at ? 1 : 0);
    state.last_overlap_at = observation.observed_at;
    if (state.overlap_samples >= 2) return abort("two fresh observations of queue overlap");
  } else if (fresh) { state.overlap_samples = 0; }
  if (state.phase === "draining_to_bors" && fresh && observation.github_count === 0) {
    state.phase = "bors"; state.bors_started_at = iso(now);
  } else if (state.phase === "bors" && now - Date.parse(state.bors_started_at) >= 24 * HOUR) {
    state.bors_ended_at = iso(now);
    if (state.plan.mode === "bors_only") {
      state.bors_ended_at = iso(Date.parse(state.bors_started_at) + 24 * HOUR);
      state.phase = "complete"; state.finished_at = iso(now);
    } else {
      state.phase = "draining_to_queue"; state.queue_requested_at = iso(now);
      await select("queue");
    }
  } else if (state.phase === "draining_to_queue" && fresh && observation.bors_count === 0) {
    state.phase = "queue"; state.queue_started_at = iso(now);
  } else if (state.phase === "queue" && now - Date.parse(state.queue_started_at) >= 24 * HOUR) {
    state.phase = "complete"; state.queue_ended_at = iso(now); state.finished_at = iso(now);
  }
  return save();
}
