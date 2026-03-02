import { getRuns } from '$lib/server/queries/runs';

export function load() {
	return { runs: getRuns() };
}
