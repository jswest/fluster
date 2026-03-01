import { defineConfig } from 'drizzle-kit';

export default defineConfig({
	dialect: 'sqlite',
	dbCredentials: {
		url: process.env.FLUSTER_DB_PATH!
	},
	out: './src/lib/server/db',
	tablesFilter: [
		'schema_version',
		'rows',
		'artifacts',
		'items',
		'item_artifacts',
		'representations',
		'jobs',
		'job_logs',
		'embeddings',
		'reductions',
		'reduction_coordinates',
		'cluster_runs',
		'cluster_assignments',
		'cluster_exemplars',
		'cluster_summaries',
		'cluster_run_critiques',
		'llm_calls'
	]
});
