<script lang="ts">
	import { invalidateAll } from '$app/navigation';
	import Pill from '$lib/components/Pill.svelte';
	import Card from '$lib/components/Card.svelte';
	import EmptyState from '$lib/components/EmptyState.svelte';
	import Spinner from '$lib/components/Spinner.svelte';
	import { statusVariant, formatTime } from '$lib/format';

	let { data } = $props();

	const logLevelVariant: Record<string, 'neutral' | 'info' | 'success' | 'warn' | 'danger'> = {
		debug: 'neutral',
		info: 'info',
		warning: 'warn',
		error: 'danger'
	};

	const isActive = $derived(data.job.status === 'running' || data.job.status === 'queued');

	$effect(() => {
		if (!isActive) return;
		const interval = setInterval(() => {
			invalidateAll();
		}, 2000);
		return () => clearInterval(interval);
	});
</script>

<div class="container stack">
	<div class="header row">
		<a href="/jobs" class="muted">&larr; Jobs</a>
		<h1>Job #{data.job.jobId}</h1>
		{#if isActive}
			<Spinner size="sm" />
		{/if}
	</div>

	<Card>
		<div class="metadata">
			<div class="meta-row">
				<span class="muted">Type</span>
				<span>{data.job.jobType}</span>
			</div>
			<div class="meta-row">
				<span class="muted">Status</span>
				<Pill variant={statusVariant[data.job.status] ?? 'neutral'}>
					{data.job.status}
				</Pill>
			</div>
			<div class="meta-row">
				<span class="muted">Created</span>
				<span>{formatTime(data.job.createdAt)}</span>
			</div>
			<div class="meta-row">
				<span class="muted">Started</span>
				<span>{formatTime(data.job.startedAt)}</span>
			</div>
			<div class="meta-row">
				<span class="muted">Finished</span>
				<span>{formatTime(data.job.finishedAt)}</span>
			</div>
			{#if data.job.errorMessage}
				<div class="meta-row">
					<span class="muted">Error</span>
					<span class="error-message">{data.job.errorMessage}</span>
				</div>
			{/if}
		</div>
	</Card>

	<h2>Logs</h2>

	{#if data.logs.length === 0}
		<EmptyState message="No logs yet." />
	{:else}
		<div class="log-list">
			{#each data.logs as log}
				<div class="log-entry">
					<span class="log-time muted">{formatTime(log.createdAt)}</span>
					<Pill variant={logLevelVariant[log.level] ?? 'neutral'} size="sm">
						{log.level}
					</Pill>
					<span class="log-message">{log.message}</span>
				</div>
			{/each}
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

	.error-message {
		color: var(--color-tertiary-invert);
	}

	.log-list {
		display: flex;
		flex-direction: column;
		gap: 0.25rem;
	}

	.log-entry {
		display: flex;
		align-items: baseline;
		gap: 0.75rem;
		padding: 0.25rem 0;
		border-bottom: 1px solid var(--color-bg-secondary);
	}

	.log-time {
		flex-shrink: 0;
		font-size: 0.8125rem;
	}

	.log-message {
		word-break: break-word;
	}
</style>
