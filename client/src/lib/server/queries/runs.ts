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
			)`,
			nNoise: sql<number>`(
				SELECT COUNT(*)
				FROM cluster_assignments
				WHERE cluster_run_id = ${clusterRuns.clusterRunId} AND cluster_id < 0
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

export function getClusterDetails(clusterRunId: number) {
	const sizes = db
		.select({
			clusterId: clusterAssignments.clusterId,
			size: sql<number>`COUNT(*)`
		})
		.from(clusterAssignments)
		.where(eq(clusterAssignments.clusterRunId, clusterRunId))
		.groupBy(clusterAssignments.clusterId)
		.all();

	const sizeMap = new Map(sizes.map((s) => [s.clusterId, s.size]));

	const labels = db
		.select()
		.from(clusterSummaries)
		.where(eq(clusterSummaries.clusterRunId, clusterRunId))
		.orderBy(clusterSummaries.clusterId)
		.all();

	return labels.map((l) => ({
		clusterId: l.clusterId,
		label: l.label,
		size: sizeMap.get(l.clusterId) ?? 0
	}));
}

export type CritiqueData = {
	verdict?: string;
	quality_score?: number;
	recommendations?: string[];
	metrics?: Record<string, unknown>;
};

export function getCritique(clusterRunId: number): CritiqueData | undefined {
	const row = db
		.select({ critiqueJson: clusterRunCritiques.critiqueJson })
		.from(clusterRunCritiques)
		.where(eq(clusterRunCritiques.clusterRunId, clusterRunId))
		.get();

	if (!row) return undefined;
	try {
		return JSON.parse(row.critiqueJson) as CritiqueData;
	} catch {
		return undefined;
	}
}
