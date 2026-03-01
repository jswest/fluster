import { desc, eq, sql } from 'drizzle-orm';
import { db } from '$lib/server/db';
import {
	clusterRuns,
	clusterAssignments,
	clusterSummaries,
	clusterRunCritiques
} from '$lib/server/db/schema';

export function getRuns() {
	return db
		.select({
			clusterRunId: clusterRuns.clusterRunId,
			reductionId: clusterRuns.reductionId,
			method: clusterRuns.method,
			paramsJson: clusterRuns.paramsJson,
			createdAt: clusterRuns.createdAt,
			nClusters: sql<number>`(
				SELECT COUNT(DISTINCT cluster_id)
				FROM cluster_assignments
				WHERE cluster_run_id = ${clusterRuns.clusterRunId} AND cluster_id >= 0
			)`,
			nItems: sql<number>`(
				SELECT COUNT(*)
				FROM cluster_assignments
				WHERE cluster_run_id = ${clusterRuns.clusterRunId}
			)`
		})
		.from(clusterRuns)
		.orderBy(desc(clusterRuns.clusterRunId))
		.all();
}

export function getRun(clusterRunId: number) {
	return db
		.select()
		.from(clusterRuns)
		.where(eq(clusterRuns.clusterRunId, clusterRunId))
		.get();
}

export function getClusterLabels(clusterRunId: number) {
	return db
		.select()
		.from(clusterSummaries)
		.where(eq(clusterSummaries.clusterRunId, clusterRunId))
		.orderBy(clusterSummaries.clusterId)
		.all();
}

export function getCritique(clusterRunId: number) {
	const row = db
		.select({ critiqueJson: clusterRunCritiques.critiqueJson })
		.from(clusterRunCritiques)
		.where(eq(clusterRunCritiques.clusterRunId, clusterRunId))
		.get();

	if (!row) return undefined;
	try {
		return JSON.parse(row.critiqueJson) as Record<string, unknown>;
	} catch {
		return undefined;
	}
}
