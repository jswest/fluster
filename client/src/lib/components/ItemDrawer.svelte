<script lang="ts">
	import Spinner from './Spinner.svelte';

	type ItemDetail = {
		itemId: number;
		recordName: string;
		metadata: Record<string, unknown>;
		embeddingText: string;
		imageArtifactId: string | null;
	};

	interface Props {
		itemId: number;
		onClose: () => void;
	}

	let { itemId, onClose }: Props = $props();

	let item: ItemDetail | null = $state(null);
	let loading = $state(true);
	let fetchError = $state('');
	let expanded = $state(false);

	const TRUNCATE_LENGTH = 500;

	$effect(() => {
		const currentId = itemId;
		loading = true;
		fetchError = '';
		item = null;
		expanded = false;

		fetch(`/api/items/${currentId}`)
			.then((r) => {
				if (!r.ok) throw new Error(`HTTP ${r.status}`);
				return r.json();
			})
			.then((data) => {
				if (itemId === currentId) item = data;
			})
			.catch((e) => {
				if (itemId === currentId) fetchError = e.message;
			})
			.finally(() => {
				if (itemId === currentId) loading = false;
			});
	});

	const displayText = $derived.by(() => {
		if (!item) return '';
		if (expanded || item.embeddingText.length <= TRUNCATE_LENGTH) return item.embeddingText;
		return item.embeddingText.slice(0, TRUNCATE_LENGTH) + '...';
	});

	const canExpand = $derived(item ? item.embeddingText.length > TRUNCATE_LENGTH : false);
</script>

<div class="drawer">
	<div class="drawer-header">
		<h3>Item Detail</h3>
		<button onclick={onClose}>&times;</button>
	</div>

	{#if loading}
		<div class="center">
			<Spinner size="md" />
		</div>
	{:else if fetchError}
		<p class="muted">Failed to load item: {fetchError}</p>
	{:else if item}
		<div class="drawer-body stack">
			<div class="field">
				<span class="muted">Name</span>
				<span class="field-value">{item.recordName || 'unnamed'}</span>
			</div>

				{#if item.imageArtifactId}
				<div class="field">
					<span class="muted">Image</span>
					<img
						src="/api/artifacts/{item.imageArtifactId}"
						alt={item.recordName || 'Image'}
						class="item-image"
					/>
				</div>
			{/if}

			{#if Object.keys(item.metadata).length > 0}
				<div class="field">
					<span class="muted">Metadata</span>
					<div class="meta-entries">
						{#each Object.entries(item.metadata) as [key, value]}
							<div class="meta-entry">
								<span class="muted">{key}</span>
								<span>{String(value ?? '\u2014')}</span>
							</div>
						{/each}
					</div>
				</div>
			{/if}

			<div class="field">
				<span class="muted">{item.imageArtifactId ? 'Caption' : 'Embedding Text'}</span>
				<pre class="embedding-text">{displayText}</pre>
				{#if canExpand}
					<button class="expand-btn" onclick={() => expanded = !expanded}>
						{expanded ? 'Collapse' : 'Expand full text'}
					</button>
				{/if}
			</div>
		</div>
	{/if}
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
		background: rgba(255, 255, 255, 0.92);
		backdrop-filter: blur(8px);
		-webkit-backdrop-filter: blur(8px);
		border-left: 1px solid var(--color-secondary-dark);
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

	.center {
		display: flex;
		justify-content: center;
		padding: 2rem 0;
	}

	.drawer-body {
		font-size: 0.875rem;
	}

	.field {
		display: flex;
		flex-direction: column;
		gap: 0.25rem;
	}

	.field-value {
		font-weight: 600;
	}

	.meta-entries {
		display: flex;
		flex-direction: column;
		gap: 0.125rem;
		padding-left: 0.5rem;
	}

	.meta-entry {
		display: flex;
		gap: 0.75rem;
	}

	.meta-entry .muted {
		min-width: 10ch;
	}

	.item-image {
		max-width: 100%;
		height: auto;
		border: 1px solid var(--color-secondary-dark);
	}

	.embedding-text {
		white-space: pre-wrap;
		word-break: break-word;
		font-size: 0.8125rem;
		line-height: 1.5;
		margin: 0;
		padding: 0.5rem;
		background: rgba(0, 0, 0, 0.03);
		border: 1px solid var(--color-secondary-dark);
	}

	.expand-btn {
		align-self: flex-start;
		font-size: 0.75rem;
		padding: 0.2rem 0.5rem;
	}
</style>
