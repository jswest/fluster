<script lang="ts">
	import EmptyState from '$lib/components/EmptyState.svelte';
	import { formatTime, formatPercent } from '$lib/format';

	let { data } = $props();
</script>

<div class="container">
	<h1>Cluster Runs</h1>

	{#if data.runs.length === 0}
		<EmptyState message="No cluster runs found." />
	{:else}
		<table>
			<thead>
				<tr>
					<th>Method</th>
					<th>Clusters</th>
					<th>Noise</th>
					<th>Created</th>
				</tr>
			</thead>
			<tbody>
				{#each data.runs as run}
					<tr>
						<td><a href="/runs/{run.clusterRunId}">{run.method}</a></td>
						<td>{run.nClusters}</td>
						<td>{run.nItems > 0 ? formatPercent(run.nNoise / run.nItems) : '\u2014'}</td>
						<td class="muted">{formatTime(run.createdAt)}</td>
					</tr>
				{/each}
			</tbody>
		</table>
	{/if}
</div>

<style>
	a {
		color: inherit;
		text-decoration: none;
		border-bottom: 1px solid var(--color-fg-secondary);
	}

	a:hover {
		border-bottom-color: var(--color-fg);
	}
</style>
