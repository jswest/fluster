import { desc, eq } from 'drizzle-orm';
import { db } from '$lib/server/db';
import { jobs, jobLogs } from '$lib/server/db/schema';

export function getJobs() {
	return db.select().from(jobs).orderBy(desc(jobs.createdAt)).all();
}

export function getJob(jobId: number) {
	return db.select().from(jobs).where(eq(jobs.jobId, jobId)).get();
}

export function getJobLogs(jobId: number, limit = 200) {
	return db
		.select()
		.from(jobLogs)
		.where(eq(jobLogs.jobId, jobId))
		.orderBy(desc(jobLogs.jobLogId))
		.limit(limit)
		.all();
}
