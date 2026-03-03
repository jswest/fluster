import { scaleOrdinal } from 'd3-scale';

const PALETTE = [
	'#6B9BD2', '#F5A623', '#E8636B', '#7DD3CC', '#6BBF59',
	'#F7D96E', '#C98DB8', '#FF9DAF', '#B8917A', '#C8BEB8'
];

const NOISE_COLOR = '#555555';

export function createClusterColorScale(clusterIds: number[]): (clusterId: number) => string {
	const ids = [...new Set(clusterIds)]
		.filter((id) => id >= 0)
		.sort((a, b) => a - b);
	const scale = scaleOrdinal<number, string>().domain(ids).range(PALETTE);
	return (clusterId: number) => (clusterId < 0 ? NOISE_COLOR : scale(clusterId));
}
