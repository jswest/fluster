<script lang="ts">
	import type { Snippet } from 'svelte';
	import type { HTMLButtonAttributes } from 'svelte/elements';

	interface Props extends HTMLButtonAttributes {
		children: Snippet;
	}

	let { children, ...rest }: Props = $props();
</script>

<button {...rest}>
	{@render children()}
</button>

<style>
	button {
		display: inline-flex;
		align-items: center;
		gap: 0.5rem;
	}
</style>
