<script lang="ts">
	import Pill from './Pill.svelte';

	type Cluster = {
		clusterId: number;
		label: string;
		size: number;
		shortLabel: string;
		rationale: string;
		keywords: string[];
		provider: string;
		model: string;
	};

	interface Props {
		cluster: Cluster;
		color: string;
		onClose: () => void;
	}

	let { cluster, color, onClose }: Props = $props();
</script>

<div class="drawer">
	<div class="drawer-header">
		<h3>Cluster Detail</h3>
		<button onclick={onClose}>&times;</button>
	</div>

	<div class="drawer-body stack">
		<div class="title-row">
			<span class="swatch" style="background: {color}"></span>
			<span class="title-text">
				<span class="muted">#{cluster.clusterId}</span>
				{cluster.label}
			</span>
		</div>

		{#if cluster.shortLabel}
			<div class="field">
				<span class="muted">Short Label</span>
				<span class="field-value">{cluster.shortLabel}</span>
			</div>
		{/if}

		<div class="field">
			<span class="muted">Size</span>
			<span class="field-value">{cluster.size}</span>
		</div>

		{#if cluster.rationale}
			<div class="field">
				<span class="muted">Rationale</span>
				<p class="rationale">{cluster.rationale}</p>
			</div>
		{/if}

		{#if cluster.keywords.length > 0}
			<div class="field">
				<span class="muted">Keywords</span>
				<div class="keywords">
					{#each cluster.keywords as keyword}
						<Pill size="sm" variant="info">{keyword}</Pill>
					{/each}
				</div>
			</div>
		{/if}

		<div class="field">
			<span class="muted">Provider</span>
			<span class="field-value">{cluster.provider}</span>
		</div>

		<div class="field">
			<span class="muted">Model</span>
			<span class="field-value">{cluster.model}</span>
		</div>
	</div>
</div>

<style>
	.drawer {
		position: absolute;
		top: 0;
		right: 0;
		width: 28rem;
		height: 100%;
		overflow-y: auto;
		padding: 1rem;
		z-index: 7;
		background: rgba(10, 10, 15, 0.92);
		backdrop-filter: blur(8px);
		-webkit-backdrop-filter: blur(8px);
		border-left: 1px solid var(--color-fg-secondary);
	}

	.drawer-header {
		display: flex;
		justify-content: space-between;
		align-items: center;
		margin-bottom: 0.75rem;
	}

	.drawer-header h3 {
		margin: 0;
	}

	.drawer-header button {
		font-size: 1.25rem;
		padding: 0.25rem 0.5rem;
		line-height: 1;
	}

	.drawer-body {
		font-size: 0.875rem;
	}

	.title-row {
		display: flex;
		align-items: center;
		gap: 0.5rem;
	}

	.title-text {
		font-weight: 600;
	}

	.swatch {
		display: inline-block;
		width: 0.875rem;
		height: 0.875rem;
		border-radius: 2px;
		flex-shrink: 0;
	}

	.field {
		display: flex;
		flex-direction: column;
		gap: 0.25rem;
	}

	.field-value {
		font-weight: 600;
	}

	.rationale {
		margin: 0;
		line-height: 1.5;
	}

	.keywords {
		display: flex;
		flex-wrap: wrap;
		gap: 0.375rem;
	}
</style>
