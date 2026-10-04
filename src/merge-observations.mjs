// Immutable measurements in the existing R2 bucket; these records never grant
// either queue ownership or permission to merge.
export async function readObservations(request, bucket) {
  const url = new URL(request.url);
  const day = url.searchParams.get("day");
  if (request.method !== "GET" || !/^\d{4}-\d{2}-\d{2}$/.test(day || "")) {
    return Response.json({ error: "GET with day=YYYY-MM-DD required" }, { status: 400 });
  }
  const page = await bucket.list({ prefix: `merge-observations/${day}/`, limit: 100,
    cursor: url.searchParams.get("cursor") || undefined });
  const records = await Promise.all(page.objects.map(async (o) => {
    const object = await bucket.get(o.key);
    if (!object) throw new Error("Observation disappeared");
    return object.json();
  }));
  return Response.json({ observations: records, truncated: page.truncated,
    cursor: page.truncated ? page.cursor : null }, { headers: { "Cache-Control": "no-store" } });
}

export async function archiveObservation(observation, bucket) {
  const when = observation.observed_at;
  if (observation.schema !== "tauceti-merge.observation/v1" ||
      typeof when !== "string" || !/^\d{4}-\d{2}-\d{2}T[\d:.]+(?:Z|\+00:00)$/.test(when)) {
    throw new Error("Invalid merge observation");
  }
  await bucket.put(`merge-observations/${when.slice(0, 10)}/${when}.json`, JSON.stringify(observation),
    { httpMetadata: { contentType: "application/json" } });
}
