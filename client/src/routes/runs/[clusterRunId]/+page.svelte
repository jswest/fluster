<script lang="ts">
	import Card from '$lib/components/Card.svelte';
	import Pill from '$lib/components/Pill.svelte';
	import EmptyState from '$lib/components/EmptyState.svelte';
	import Input from '$lib/components/Input.svelte';
	import ScatterPlot from '$lib/components/ScatterPlot.svelte';
	import ItemDrawer from '$lib/components/ItemDrawer.svelte';
	import { createClusterColorScale } from '$lib/cluster-colors';
	import { formatTime, formatPercent } from '$lib/format';

	let { data } = $props();

	let search = $state('');
	let showCritique = $state(false);
	let focusClusterId: number | null = $state(null);
	let inspectItemId: number | null = $state(null);

	const getClusterColor = $derived.by(() =>
		createClusterColorScale(data.points.map((p) => p.clusterId))
	);

	const filteredClusters = $derived(
		data.clusters.filter((c) => {
			if (!search.trim()) return true;
			return c.label.toLowerCase().includes(search.trim().toLowerCase());
		})
	);

	function scoreVariant(score: number): 'success' | 'warn' | 'danger' {
		if (score >= 0.7) return 'success';
		if (score >= 0.4) return 'warn';
		return 'danger';
	}

	function formatMetricLabel(key: string): string {
		return key.replace(/_/g, ' ');
	}

	function formatMetricValue(value: unknown): string {
		if (value === null || value === undefined) return '\u2014';
		if (typeof value === 'number') {
			if (Number.isInteger(value)) return String(value);
			return value.toFixed(3);
		}
		return String(value);
	}

	function handlePointSelect(itemId: number) {
		inspectItemId = itemId;
	}
</script>

<div class="fullscreen">
	{#if data.points.length === 0}
		<div class="container stack">
			<EmptyState message="No UMAP data available for this run." />
		</div>
	{:else}
		<ScatterPlot points={data.points} getColor={getClusterColor} {focusClusterId} onSelect={handlePointSelect} />
	{/if}

	<div class="left-rail">
		<div class="rail-header">
			<a href="/runs" class="muted">&larr; Runs</a>
			<h2>Run #{data.run.clusterRunId}</h2>
		</div>

		<Card>
			<div class="metadata">
				<div class="meta-row">
					<span class="muted">Method</span>
					<span>{data.run.method}</span>
				</div>
				<div class="meta-row">
					<span class="muted">Params</span>
					<span>{data.run.paramsJson}</span>
				</div>
				<div class="meta-row">
					<span class="muted">Clusters</span>
					<span>{data.clusters.length}</span>
				</div>
				<div class="meta-row">
					<span class="muted">Created</span>
					<span>{formatTime(data.run.createdAt)}</span>
				</div>
			</div>
		</Card>

		<h3>Clusters</h3>

		{#if data.clusters.length === 0}
			<EmptyState message="No cluster labels found." />
		{:else}
			<Input placeholder="Search clusters..." bind:value={search} />

			{#if filteredClusters.length === 0}
				<p class="muted">No clusters match.</p>
			{:else}
				<table>
					<thead>
						<tr>
							<th></th>
							<th>ID</th>
							<th>Label</th>
							<th>Size</th>
						</tr>
					</thead>
					<tbody>
						{#each filteredClusters as cluster}
							<tr
								class="cluster-row"
								class:active={focusClusterId === cluster.clusterId}
								onclick={() => focusClusterId = focusClusterId === cluster.clusterId ? null : cluster.clusterId}
							>
								<td><span class="swatch" style="background: {getClusterColor(cluster.clusterId)}"></span></td>
								<td>{cluster.clusterId}</td>
								<td>{cluster.label}</td>
								<td>{cluster.size}</td>
							</tr>
						{/each}
					</tbody>
				</table>
			{/if}
		{/if}

		{#if data.critique}
			<button class="critique-btn" onclick={() => showCritique = !showCritique}>
				{showCritique ? 'Hide Critique' : 'Show Critique'}
			</button>
		{/if}
	</div>

	{#if inspectItemId != null}
		<ItemDrawer itemId={inspectItemId} onClose={() => inspectItemId = null} />
	{/if}

	{#if showCritique && data.critique}
		<div class="critique-overlay">
			<div class="critique-header">
				<h2>Critique</h2>
				<button onclick={() => showCritique = false}>&times;</button>
			</div>

			<div class="critique-cards stack">
				{#if data.critique.verdict}
					<Card>
						<h3>Verdict</h3>
						<p>{data.critique.verdict}</p>
					</Card>
				{/if}

				{#if data.critique.quality_score != null}
					<Card>
						<h3>Quality Score</h3>
						<Pill variant={scoreVariant(data.critique.quality_score)}>
							{formatPercent(data.critique.quality_score)}
						</Pill>
					</Card>
				{/if}

				{#if data.critique.metrics}
					<Card>
						<h3>Metrics</h3>
						<div class="metadata">
							{#each Object.entries(data.critique.metrics) as [key, value]}
								<div class="meta-row">
									<span class="muted">{formatMetricLabel(key)}</span>
									<span>{formatMetricValue(value)}</span>
								</div>
							{/each}
						</div>
					</Card>
				{/if}

				{#if data.critique.recommendations && data.critique.recommendations.length > 0}
					<Card>
						<h3>Recommendations</h3>
						<ul>
							{#each data.critique.recommendations as rec}
								<li>{rec}</li>
							{/each}
						</ul>
					</Card>
				{/if}
			</div>
		</div>
	{/if}
</div>

<style>
	.fullscreen {
		height: calc(100vh - 2.75rem);
		position: relative;
		overflow: hidden;
	}

	.left-rail {
		position: absolute;
		top: 0;
		left: 0;
		width: 22rem;
		height: 100%;
		overflow-y: auto;
		padding: 1rem;
		background: rgba(255, 255, 255, 0.85);
		backdrop-filter: blur(8px);
		-webkit-backdrop-filter: blur(8px);
		border-right: 1px solid var(--color-secondary-dark);
		display: flex;
		flex-direction: column;
		gap: 0.75rem;
		z-index: 5;
	}

	.rail-header {
		display: flex;
		align-items: center;
		gap: 0.75rem;
	}

	.rail-header h2 {
		margin: 0;
		font-size: 1.25rem;
	}

	.metadata {
		display: flex;
		flex-direction: column;
		gap: 0.25rem;
		font-size: 0.875rem;
	}

	.meta-row {
		display: flex;
		gap: 0.75rem;
	}

	.meta-row .muted {
		min-width: 7ch;
	}

	.critique-btn {
		align-self: flex-start;
		font-size: 0.8125rem;
		padding: 0.25rem 0.75rem;
	}

	.critique-overlay {
		position: absolute;
		top: 0;
		right: 0;
		width: 28rem;
		height: 100%;
		overflow-y: auto;
		padding: 1rem;
		z-index: 6;
		background: rgba(255, 255, 255, 0.9);
		backdrop-filter: blur(8px);
		-webkit-backdrop-filter: blur(8px);
		border-left: 1px solid var(--color-secondary-dark);
	}

	.critique-header {
		display: flex;
		justify-content: space-between;
		align-items: center;
		margin-bottom: 0.75rem;
	}

	.critique-header h2 {
		margin: 0;
	}

	.critique-header button {
		font-size: 1.25rem;
		padding: 0.25rem 0.5rem;
		line-height: 1;
	}

	ul {
		margin: 0.5rem 0 0 0;
		padding-left: 1.25rem;
	}

	li {
		margin-bottom: 0.25rem;
	}

	table {
		font-size: 0.8125rem;
	}

	.cluster-row {
		cursor: pointer;
	}

	.cluster-row:hover {
		background: rgba(0, 0, 0, 0.04);
	}

	.cluster-row.active {
		background: rgba(0, 0, 0, 0.08);
	}

	.swatch {
		display: inline-block;
		width: 0.75rem;
		height: 0.75rem;
		border-radius: 2px;
		vertical-align: middle;
	}
</style>
