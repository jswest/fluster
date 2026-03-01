import { defineConfig } from 'drizzle-kit';

export default defineConfig({
	dialect: 'sqlite',
	dbCredentials: {
		url: process.env.FLUSTER_DB_PATH!
	},
	out: './src/lib/server/db'
});
