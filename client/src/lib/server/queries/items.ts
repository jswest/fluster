import { eq, and, like, inArray } from 'drizzle-orm';
import { db } from '$lib/server/db';
import {
	items,
	rows,
	representations,
	reductionCoordinates,
	clusterAssignments,
	clusterRuns,
	itemArtifacts,
	artifacts
} from '$lib/server/db/schema';

const PREVIEW_LENGTH = 200;

function getImageArtifactIds(itemIds: number[]): Map<number, string> {
	if (itemIds.length === 0) return new Map();

	const results = db
		.select({
			itemId: itemArtifacts.itemId,
			artifactId: itemArtifacts.artifactId
		})
		.from(itemArtifacts)
		.innerJoin(artifacts, eq(itemArtifacts.artifactId, artifacts.artifactId))
		.where(and(
			like(artifacts.mimeType, 'image/%'),
			inArray(itemArtifacts.itemId, itemIds)
		))
		.all();

	const map = new Map<number, string>();
	for (const r of results) {
		if (!map.has(r.itemId)) {
			map.set(r.itemId, r.artifactId);
		}
	}
	return map;
}

export function getScatterPlotData(clusterRunId: number) {
	const run = db
		.select({ reductionId: clusterRuns.reductionId })
		.from(clusterRuns)
		.where(eq(clusterRuns.clusterRunId, clusterRunId))
		.get();

	if (!run) return [];

	const rawRows = db
		.select({
			itemId: items.itemId,
			recordName: rows.rowName,
			embeddingText: representations.text,
			coordinatesJson: reductionCoordinates.coordinatesJson,
			clusterId: clusterAssignments.clusterId
		})
		.from(items)
		.innerJoin(rows, eq(items.rowId, rows.rowId))
		.innerJoin(
			representations,
			and(
				eq(representations.itemId, items.itemId),
				eq(representations.representationType, 'embedding_text')
			)
		)
		.innerJoin(
			reductionCoordinates,
			and(
				eq(reductionCoordinates.itemId, items.itemId),
				eq(reductionCoordinates.reductionId, run.reductionId)
			)
		)
		.innerJoin(
			clusterAssignments,
			and(
				eq(clusterAssignments.itemId, items.itemId),
				eq(clusterAssignments.clusterRunId, clusterRunId)
			)
		)
		.all();

	const imageMap = getImageArtifactIds(rawRows.map((r) => r.itemId));

	return rawRows.map((r) => {
		const coords = JSON.parse(r.coordinatesJson) as number[];
		return {
			itemId: r.itemId,
			x: coords[0],
			y: coords[1],
			clusterId: r.clusterId,
			recordName: r.recordName ?? '',
			embeddingTextPreview:
				r.embeddingText.length > PREVIEW_LENGTH
					? r.embeddingText.slice(0, PREVIEW_LENGTH) + '…'
					: r.embeddingText,
			imageArtifactId: imageMap.get(r.itemId) ?? null
		};
	});
}

export function getItemDetail(itemId: number) {
	const row = db
		.select({
			itemId: items.itemId,
			rowName: rows.rowName,
			rowMetadataJson: rows.rowMetadataJson,
			embeddingText: representations.text
		})
		.from(items)
		.innerJoin(rows, eq(items.rowId, rows.rowId))
		.innerJoin(
			representations,
			and(
				eq(representations.itemId, items.itemId),
				eq(representations.representationType, 'embedding_text')
			)
		)
		.where(eq(items.itemId, itemId))
		.get();

	if (!row) return undefined;

	let metadata: Record<string, unknown> = {};
	try {
		metadata = JSON.parse(row.rowMetadataJson);
	} catch {}

	const imageRow = db
		.select({ artifactId: itemArtifacts.artifactId })
		.from(itemArtifacts)
		.innerJoin(artifacts, eq(itemArtifacts.artifactId, artifacts.artifactId))
		.where(
			and(
				eq(itemArtifacts.itemId, itemId),
				like(artifacts.mimeType, 'image/%')
			)
		)
		.limit(1)
		.get();

	return {
		itemId: row.itemId,
		recordName: row.rowName ?? '',
		metadata,
		embeddingText: row.embeddingText,
		imageArtifactId: imageRow?.artifactId ?? null
	};
}
