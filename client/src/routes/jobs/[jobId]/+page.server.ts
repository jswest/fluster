import { error } from '@sveltejs/kit';
import { getJob, getJobLogs } from '$lib/server/queries/jobs';

export function load({ params }) {
	const jobId = Number(params.jobId);
	if (Number.isNaN(jobId)) throw error(400, 'Invalid job ID');

	const job = getJob(jobId);
	if (!job) throw error(404, 'Job not found');

	return {
		job,
		logs: getJobLogs(jobId)
	};
}
