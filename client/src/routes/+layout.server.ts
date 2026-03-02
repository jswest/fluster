import { projectName, dbPath } from '$lib/server/env';

export function load() {
	return { projectName, dbPath };
}
