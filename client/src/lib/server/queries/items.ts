import { eq, and } from 'drizzle-orm';
import { db } from '$lib/server/db';
import {
	items,
	rows,
	representations,
	reductionCoordinates,
	clusterAssignments,
	clusterRuns
} from '$lib/server/db/schema';

const PREVIEW_LENGTH = 200;

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
					: r.embeddingText
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

	return {
		itemId: row.itemId,
		recordName: row.rowName ?? '',
		metadata,
		embeddingText: row.embeddingText
	};
}
