import { error } from '@sveltejs/kit';
import { getRun, getClusterDetails, getCritique } from '$lib/server/queries/runs';
import { getScatterPlotData } from '$lib/server/queries/items';

export function load({ params }) {
	const clusterRunId = Number(params.clusterRunId);
	if (Number.isNaN(clusterRunId)) throw error(400, 'Invalid cluster run ID');

	const run = getRun(clusterRunId);
	if (!run) throw error(404, 'Cluster run not found');

	return {
		run,
		clusters: getClusterDetails(clusterRunId),
		critique: getCritique(clusterRunId),
		points: getScatterPlotData(clusterRunId)
	};
}
