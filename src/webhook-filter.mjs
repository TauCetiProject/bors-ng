// Bors only updates active batches and attempts from checks on its own
// staging and trying commits. Other checks are already available from GitHub
// when bors fetches PR-head status during preflight.
function borsBranch(branch) {
  return typeof branch === "string" &&
    (branch.startsWith("staging") || branch.startsWith("trying"));
}

export function needsBors(event, body) {
  if (event !== "check_run" && event !== "check_suite") return true;

  let payload;
  try {
    payload = JSON.parse(new TextDecoder().decode(body));
  } catch {
    // Let bors report a malformed payload rather than silently discarding it.
    return true;
  }

  if (event === "check_run") {
    return payload.check_run?.status === "completed" &&
      borsBranch(payload.check_run?.check_suite?.head_branch);
  }
  return payload.action === "completed" &&
    borsBranch(payload.check_suite?.head_branch);
}
