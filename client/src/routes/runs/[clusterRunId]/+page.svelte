<script lang="ts">
	import Card from '$lib/components/Card.svelte';
	import Pill from '$lib/components/Pill.svelte';
	import EmptyState from '$lib/components/EmptyState.svelte';
	import Input from '$lib/components/Input.svelte';
	import { formatTime, formatPercent } from '$lib/format';

	let { data } = $props();

	let search = $state('');

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
</script>

<div class="container stack">
	<div class="header row">
		<a href="/runs" class="muted">&larr; Runs</a>
		<h1>Run #{data.run.clusterRunId}</h1>
	</div>

	<Card>
		<div class="metadata">
			<div class="meta-row">
				<span class="muted">Method</span>
				<span>{data.run.method}</span>
			</div>
			<div class="meta-row">
				<span class="muted">Parameters</span>
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

	<h2>Clusters</h2>

	{#if data.clusters.length === 0}
		<EmptyState message="No cluster labels found." />
	{:else}
		<Input placeholder="Search clusters by label..." bind:value={search} />

		{#if filteredClusters.length === 0}
			<EmptyState message="No clusters match your search." />
		{:else}
			<table>
				<thead>
					<tr>
						<th>ID</th>
						<th>Label</th>
						<th>Size</th>
					</tr>
				</thead>
				<tbody>
					{#each filteredClusters as cluster}
						<tr>
							<td>{cluster.clusterId}</td>
							<td>{cluster.label}</td>
							<td>{cluster.size}</td>
						</tr>
					{/each}
				</tbody>
			</table>
		{/if}
	{/if}

	<h2>Critique</h2>

	{#if !data.critique}
		<EmptyState message="No critique available for this run." />
	{:else}
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
	{/if}
</div>

<style>
	.header {
		align-items: center;
		gap: 1rem;
	}

	.header h1 {
		margin: 0;
	}

	.metadata {
		display: flex;
		flex-direction: column;
		gap: 0.5rem;
	}

	.meta-row {
		display: flex;
		gap: 1rem;
	}

	.meta-row .muted {
		min-width: 10ch;
	}

	ul {
		margin: 0.5rem 0 0 0;
		padding-left: 1.25rem;
	}

	li {
		margin-bottom: 0.25rem;
	}
</style>
