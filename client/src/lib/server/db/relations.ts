import { relations } from "drizzle-orm/relations";
import { rows, items, artifacts, itemArtifacts, representations, jobs, jobLogs, embeddings, reductionCoordinates, reductions, clusterRuns, clusterAssignments, clusterExemplars, llmCalls, clusterSummaries, clusterRunCritiques } from "./schema";

export const itemsRelations = relations(items, ({one, many}) => ({
	row: one(rows, {
		fields: [items.rowId],
		references: [rows.rowId]
	}),
	itemArtifacts: many(itemArtifacts),
	representations: many(representations),
	reductionCoordinates: many(reductionCoordinates),
	clusterAssignments: many(clusterAssignments),
	clusterExemplars: many(clusterExemplars),
}));

export const rowsRelations = relations(rows, ({many}) => ({
	items: many(items),
}));

export const itemArtifactsRelations = relations(itemArtifacts, ({one}) => ({
	artifact: one(artifacts, {
		fields: [itemArtifacts.artifactId],
		references: [artifacts.artifactId]
	}),
	item: one(items, {
		fields: [itemArtifacts.itemId],
		references: [items.itemId]
	}),
}));

export const artifactsRelations = relations(artifacts, ({many}) => ({
	itemArtifacts: many(itemArtifacts),
}));

export const representationsRelations = relations(representations, ({one, many}) => ({
	item: one(items, {
		fields: [representations.itemId],
		references: [items.itemId]
	}),
	embeddings: many(embeddings),
}));

export const jobLogsRelations = relations(jobLogs, ({one}) => ({
	job: one(jobs, {
		fields: [jobLogs.jobId],
		references: [jobs.jobId]
	}),
}));

export const jobsRelations = relations(jobs, ({many}) => ({
	jobLogs: many(jobLogs),
	llmCalls: many(llmCalls),
}));

export const embeddingsRelations = relations(embeddings, ({one}) => ({
	representation: one(representations, {
		fields: [embeddings.representationId],
		references: [representations.representationId]
	}),
}));

export const reductionCoordinatesRelations = relations(reductionCoordinates, ({one}) => ({
	item: one(items, {
		fields: [reductionCoordinates.itemId],
		references: [items.itemId]
	}),
	reduction: one(reductions, {
		fields: [reductionCoordinates.reductionId],
		references: [reductions.reductionId]
	}),
}));

export const reductionsRelations = relations(reductions, ({many}) => ({
	reductionCoordinates: many(reductionCoordinates),
	clusterRuns: many(clusterRuns),
}));

export const clusterRunsRelations = relations(clusterRuns, ({one, many}) => ({
	reduction: one(reductions, {
		fields: [clusterRuns.reductionId],
		references: [reductions.reductionId]
	}),
	clusterAssignments: many(clusterAssignments),
	clusterExemplars: many(clusterExemplars),
	clusterSummaries: many(clusterSummaries),
	clusterRunCritiques: many(clusterRunCritiques),
}));

export const clusterAssignmentsRelations = relations(clusterAssignments, ({one}) => ({
	item: one(items, {
		fields: [clusterAssignments.itemId],
		references: [items.itemId]
	}),
	clusterRun: one(clusterRuns, {
		fields: [clusterAssignments.clusterRunId],
		references: [clusterRuns.clusterRunId]
	}),
}));

export const clusterExemplarsRelations = relations(clusterExemplars, ({one}) => ({
	item: one(items, {
		fields: [clusterExemplars.itemId],
		references: [items.itemId]
	}),
	clusterRun: one(clusterRuns, {
		fields: [clusterExemplars.clusterRunId],
		references: [clusterRuns.clusterRunId]
	}),
}));

export const llmCallsRelations = relations(llmCalls, ({one}) => ({
	job: one(jobs, {
		fields: [llmCalls.jobId],
		references: [jobs.jobId]
	}),
}));

export const clusterSummariesRelations = relations(clusterSummaries, ({one}) => ({
	clusterRun: one(clusterRuns, {
		fields: [clusterSummaries.clusterRunId],
		references: [clusterRuns.clusterRunId]
	}),
}));

export const clusterRunCritiquesRelations = relations(clusterRunCritiques, ({one}) => ({
	clusterRun: one(clusterRuns, {
		fields: [clusterRunCritiques.clusterRunId],
		references: [clusterRuns.clusterRunId]
	}),
}));