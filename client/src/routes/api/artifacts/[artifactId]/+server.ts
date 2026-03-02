import { error } from '@sveltejs/kit';
import { db } from '$lib/server/db';
import { artifacts } from '$lib/server/db/schema';
import { eq } from 'drizzle-orm';
import { projectDir } from '$lib/server/env';
import fs from 'fs';
import path from 'path';

export function GET({ params }) {
	const artifactId = params.artifactId;

	const artifact = db
		.select({ storedPath: artifacts.storedPath, mimeType: artifacts.mimeType })
		.from(artifacts)
		.where(eq(artifacts.artifactId, artifactId))
		.get();

	if (!artifact) throw error(404, 'Artifact not found');

	const filePath = path.resolve(path.join(projectDir, 'artifacts', artifact.storedPath));
	const artifactsRoot = path.resolve(path.join(projectDir, 'artifacts'));
	if (!filePath.startsWith(artifactsRoot + path.sep) && filePath !== artifactsRoot) {
		throw error(403, 'Invalid artifact path');
	}
	if (!fs.existsSync(filePath)) throw error(404, 'Artifact file not found');

	const fileBuffer = fs.readFileSync(filePath);
	return new Response(fileBuffer, {
		headers: {
			'Content-Type': artifact.mimeType || 'application/octet-stream',
			'Cache-Control': 'public, max-age=31536000, immutable'
		}
	});
}
