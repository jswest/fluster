import { scaleOrdinal } from 'd3-scale';

const PALETTE = [
	'#4e79a7', '#f28e2b', '#e15759', '#76b7b2', '#59a14f',
	'#edc948', '#b07aa1', '#ff9da7', '#9c755f', '#bab0ac'
];

const NOISE_COLOR = '#999999';

export function createClusterColorScale(clusterIds: number[]): (clusterId: number) => string {
	const ids = [...new Set(clusterIds)]
		.filter((id) => id >= 0)
		.sort((a, b) => a - b);
	const scale = scaleOrdinal<number, string>().domain(ids).range(PALETTE);
	return (clusterId: number) => (clusterId < 0 ? NOISE_COLOR : scale(clusterId));
}
