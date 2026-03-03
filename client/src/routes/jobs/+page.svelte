<script lang="ts">
	import Pill from '$lib/components/Pill.svelte';
	import EmptyState from '$lib/components/EmptyState.svelte';
	import { statusVariant, formatTime } from '$lib/format';

	let { data } = $props();
</script>

<div class="container">
	<h1>Jobs</h1>

	{#if data.jobs.length === 0}
		<EmptyState message="No jobs found." />
	{:else}
		<table>
			<thead>
				<tr>
					<th>Type</th>
					<th>Status</th>
					<th>Started</th>
					<th>Finished</th>
				</tr>
			</thead>
			<tbody>
				{#each data.jobs as job}
					<tr>
						<td><a href="/jobs/{job.jobId}">{job.jobType}</a></td>
						<td>
							<Pill variant={statusVariant[job.status] ?? 'neutral'} size="sm">
								{job.status}
							</Pill>
						</td>
						<td class="muted">{formatTime(job.startedAt)}</td>
						<td class="muted">{formatTime(job.finishedAt)}</td>
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
