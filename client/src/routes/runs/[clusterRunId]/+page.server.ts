import { error } from '@sveltejs/kit';
import { getRun, getClusterDetails, getCritique } from '$lib/server/queries/runs';
import { getLayouts, getLayoutData, getSomGrid } from '$lib/server/queries/items';

export function load({ params, url }) {
	const clusterRunId = Number(params.clusterRunId);
	if (Number.isNaN(clusterRunId)) throw error(400, 'Invalid cluster run ID');

	const run = getRun(clusterRunId);
	if (!run) throw error(404, 'Cluster run not found');

	const { layouts, defaultReductionId } = getLayouts(clusterRunId);

	// ?layout=<reductionId> selects a layout; fall back to the default.
	const requested = Number(url.searchParams.get('layout'));
	const active = layouts.find((l) => l.reductionId === requested) ??
		layouts.find((l) => l.reductionId === defaultReductionId) ?? null;

	return {
		run,
		clusters: getClusterDetails(clusterRunId),
		critique: getCritique(clusterRunId),
		layouts,
		activeReductionId: active?.reductionId ?? null,
		points: active == null ? [] : getLayoutData(clusterRunId, active.reductionId),
		somGrid: active?.method === 'som' ? getSomGrid(active.reductionId) : null
	};
}
