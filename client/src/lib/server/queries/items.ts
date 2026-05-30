import { eq, and, like, inArray } from 'drizzle-orm';
import { db } from '$lib/server/db';
import {
	items,
	rows,
	representations,
	reductions,
	reductionCoordinates,
	clusterAssignments,
	clusterRuns,
	itemArtifacts,
	artifacts
} from '$lib/server/db/schema';


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

export type Layout = {
	reductionId: number;
	method: string;
	dims: number;
	label: string;
};

function layoutLabel(method: string, dims: number): string {
	if (method === 'som') return 'SOM grid';
	if (method === 'umap') return 'UMAP';
	if (method === 'pca') return 'PCA';
	return `${method} ${dims}d`;
}

/**
 * The 2D reductions a run can be laid out on, plus which one to show by default.
 *
 * A layout is decoupled from the reduction the clustering was computed on: any
 * 2D reduction (umap_2d, som_2d, ...) can display any run's clusters. The
 * default prefers the run's own reduction when it is itself 2D (so a SOM
 * codebook run opens on the SOM grid), then a UMAP, then any 2D reduction.
 *
 * When no 2D reduction exists, `layouts` instead holds the run's own (non-2D)
 * reduction so the view still renders from its first two dimensions — so
 * callers must treat `dims` as load-bearing rather than assuming every Layout
 * is 2D.
 */
export function getLayouts(clusterRunId: number): {
	layouts: Layout[];
	defaultReductionId: number | null;
} {
	const run = db
		.select({ reductionId: clusterRuns.reductionId })
		.from(clusterRuns)
		.where(eq(clusterRuns.clusterRunId, clusterRunId))
		.get();

	if (!run) return { layouts: [], defaultReductionId: null };

	const cols = {
		reductionId: reductions.reductionId,
		method: reductions.method,
		dims: reductions.targetDimensions
	};

	const twoD = db
		.select(cols)
		.from(reductions)
		.where(eq(reductions.targetDimensions, 2))
		.orderBy(reductions.reductionId)
		.all();

	const chosen = twoD.length > 0
		? twoD
		: db.select(cols).from(reductions).where(eq(reductions.reductionId, run.reductionId)).all();

	if (chosen.length === 0) return { layouts: [], defaultReductionId: null };

	const layouts = chosen.map((r) => ({ ...r, label: layoutLabel(r.method, r.dims) }));
	const ownIs2D = twoD.some((r) => r.reductionId === run.reductionId);
	const defaultReductionId =
		twoD.length === 0
			? chosen[0].reductionId
			: ownIs2D
				? run.reductionId
				: (twoD.find((r) => r.method === 'umap') ?? twoD[0]).reductionId;

	return { layouts, defaultReductionId };
}

export function getLayoutData(clusterRunId: number, reductionId: number) {
	const rawRows = db
		.select({
			itemId: items.itemId,
			recordName: rows.rowName,
			rowMetadataJson: rows.rowMetadataJson,
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
				eq(reductionCoordinates.reductionId, reductionId)
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
		let metadata: Record<string, unknown> = {};
		try {
			metadata = JSON.parse(r.rowMetadataJson);
		} catch {}
		return {
			itemId: r.itemId,
			x: coords[0],
			y: coords[1],
			clusterId: r.clusterId,
			recordName: r.recordName ?? '',
			embeddingText: r.embeddingText,
			metadata,
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
