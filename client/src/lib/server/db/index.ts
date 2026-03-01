import Database from 'better-sqlite3';
import { drizzle } from 'drizzle-orm/better-sqlite3';
import { building } from '$app/environment';
import { dbPath } from '$lib/server/env';

const sqlite = building ? undefined : new Database(dbPath, { readonly: true });
export const db = drizzle(sqlite!);
