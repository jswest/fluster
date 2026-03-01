import { json } from '@sveltejs/kit';
import { sql } from 'drizzle-orm';
import { db } from '$lib/server/db';

export function GET() {
	try {
		db.run(sql`SELECT 1`);
		return json({ ok: true });
	} catch (e) {
		const message = e instanceof Error ? e.message : 'Unknown error';
		return json({ ok: false, error: message }, { status: 500 });
	}
}
