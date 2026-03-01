import { sqliteTable, integer, text, foreignKey, primaryKey, real } from "drizzle-orm/sqlite-core"
import { sql } from "drizzle-orm"

export const schemaVersion = sqliteTable("schema_version", {
	version: integer().primaryKey(),
	appliedAt: text("applied_at").default(sql`(datetime('now'))`).notNull(),
});

export const rows = sqliteTable("rows", {
	rowId: integer("row_id").primaryKey({ autoIncrement: true }),
	rowName: text("row_name"),
	rowMetadataJson: text("row_metadata_json").default("{}").notNull(),
	sourceRowNumber: integer("source_row_number"),
	createdAt: text("created_at").default(sql`(datetime('now'))`).notNull(),
});

export const artifacts = sqliteTable("artifacts", {
	artifactId: text("artifact_id").primaryKey(),
	originalPath: text("original_path").notNull(),
	storedPath: text("stored_path").notNull(),
	mimeType: text("mime_type"),
	bytes: integer().notNull(),
	createdAt: text("created_at").default(sql`(datetime('now'))`).notNull(),
});

export const items = sqliteTable("items", {
	itemId: integer("item_id").primaryKey({ autoIncrement: true }),
	rowId: integer("row_id").notNull().unique().references(() => rows.rowId),
	createdAt: text("created_at").default(sql`(datetime('now'))`).notNull(),
});

export const itemArtifacts = sqliteTable("item_artifacts", {
	itemId: integer("item_id").notNull().references(() => items.itemId),
	artifactId: text("artifact_id").notNull().references(() => artifacts.artifactId),
	role: text().default("source").notNull(),
}, (table) => [
	primaryKey({ columns: [table.itemId, table.artifactId], name: "item_artifacts_item_id_artifact_id_pk"}),
]);

export const representations = sqliteTable("representations", {
	representationId: integer("representation_id").primaryKey({ autoIncrement: true }),
	itemId: integer("item_id").notNull().references(() => items.itemId),
	representationType: text("representation_type").notNull(),
	text: text().notNull(),
	textHash: text("text_hash").notNull(),
	modelName: text("model_name"),
	createdAt: text("created_at").default(sql`(datetime('now'))`).notNull(),
});

export const jobs = sqliteTable("jobs", {
	jobId: integer("job_id").primaryKey({ autoIncrement: true }),
	jobType: text("job_type").notNull(),
	status: text().default("queued").notNull(),
	inputParamsJson: text("input_params_json").default("{}").notNull(),
	progressJson: text("progress_json").default("{}").notNull(),
	cancelRequestedAt: text("cancel_requested_at"),
	startedAt: text("started_at"),
	finishedAt: text("finished_at"),
	errorMessage: text("error_message"),
	createdAt: text("created_at").default(sql`(datetime('now'))`).notNull(),
});

export const jobLogs = sqliteTable("job_logs", {
	jobLogId: integer("job_log_id").primaryKey({ autoIncrement: true }),
	jobId: integer("job_id").notNull().references(() => jobs.jobId),
	createdAt: text("created_at").default(sql`(datetime('now'))`).notNull(),
	level: text().default("info").notNull(),
	message: text().notNull(),
	payloadJson: text("payload_json"),
});

export const embeddings = sqliteTable("embeddings", {
	embeddingId: integer("embedding_id").primaryKey({ autoIncrement: true }),
	representationId: integer("representation_id").notNull().references(() => representations.representationId),
	modelName: text("model_name").notNull(),
	dimensions: integer().notNull(),
	createdAt: text("created_at").default(sql`(datetime('now'))`).notNull(),
});

export const reductions = sqliteTable("reductions", {
	reductionId: integer("reduction_id").primaryKey({ autoIncrement: true }),
	embeddingReference: text("embedding_reference").notNull(),
	method: text().notNull(),
	targetDimensions: integer("target_dimensions").notNull(),
	paramsJson: text("params_json").default("{}").notNull(),
	createdAt: text("created_at").default(sql`(datetime('now'))`).notNull(),
});

export const reductionCoordinates = sqliteTable("reduction_coordinates", {
	reductionId: integer("reduction_id").notNull().references(() => reductions.reductionId),
	itemId: integer("item_id").notNull().references(() => items.itemId),
	coordinatesJson: text("coordinates_json").notNull(),
}, (table) => [
	primaryKey({ columns: [table.reductionId, table.itemId], name: "reduction_coordinates_reduction_id_item_id_pk"}),
]);

export const clusterRuns = sqliteTable("cluster_runs", {
	clusterRunId: integer("cluster_run_id").primaryKey({ autoIncrement: true }),
	reductionId: integer("reduction_id").notNull().references(() => reductions.reductionId),
	method: text().notNull(),
	paramsJson: text("params_json").default("{}").notNull(),
	createdAt: text("created_at").default(sql`(datetime('now'))`).notNull(),
});

export const clusterAssignments = sqliteTable("cluster_assignments", {
	clusterRunId: integer("cluster_run_id").notNull().references(() => clusterRuns.clusterRunId),
	itemId: integer("item_id").notNull().references(() => items.itemId),
	clusterId: integer("cluster_id").notNull(),
	membershipProbability: real("membership_probability").notNull(),
}, (table) => [
	primaryKey({ columns: [table.clusterRunId, table.itemId], name: "cluster_assignments_cluster_run_id_item_id_pk"}),
]);

export const clusterExemplars = sqliteTable("cluster_exemplars", {
	clusterRunId: integer("cluster_run_id").notNull().references(() => clusterRuns.clusterRunId),
	clusterId: integer("cluster_id").notNull(),
	itemId: integer("item_id").notNull().references(() => items.itemId),
	rank: integer().notNull(),
	score: real().notNull(),
}, (table) => [
	primaryKey({ columns: [table.clusterRunId, table.clusterId, table.itemId], name: "cluster_exemplars_cluster_run_id_cluster_id_item_id_pk"}),
]);

export const llmCalls = sqliteTable("llm_calls", {
	llmCallId: integer("llm_call_id").primaryKey({ autoIncrement: true }),
	jobId: integer("job_id").references(() => jobs.jobId),
	taskName: text("task_name").notNull(),
	provider: text().notNull(),
	model: text().notNull(),
	inputJson: text("input_json").notNull(),
	outputRawText: text("output_raw_text"),
	outputParsedJson: text("output_parsed_json"),
	createdAt: text("created_at").default(sql`(datetime('now'))`).notNull(),
});

export const clusterSummaries = sqliteTable("cluster_summaries", {
	clusterSummaryId: integer("cluster_summary_id").primaryKey({ autoIncrement: true }),
	clusterRunId: integer("cluster_run_id").notNull().references(() => clusterRuns.clusterRunId),
	clusterId: integer("cluster_id").notNull(),
	label: text().notNull(),
	labelJson: text("label_json").notNull(),
	createdAt: text("created_at").default(sql`(datetime('now'))`).notNull(),
});

export const clusterRunCritiques = sqliteTable("cluster_run_critiques", {
	clusterRunId: integer("cluster_run_id").primaryKey().references(() => clusterRuns.clusterRunId),
	critiqueJson: text("critique_json").notNull(),
	createdAt: text("created_at").default(sql`(datetime('now'))`).notNull(),
});
