<script lang="ts">
	import type { Snippet } from 'svelte';

	interface Props {
		children: Snippet;
	}

	let { children }: Props = $props();
</script>

<div class="card">
	{@render children()}
</div>

<style>
	.card {
		border: 1px solid var(--color-primary-dark);
		padding: 1rem;
		background: var(--color-primary-light);
	}
</style>
