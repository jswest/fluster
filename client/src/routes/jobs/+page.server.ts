import { getJobs } from '$lib/server/queries/jobs';

export function load() {
	return { jobs: getJobs() };
}
