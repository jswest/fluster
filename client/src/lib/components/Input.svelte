<script lang="ts">
	import type { HTMLInputAttributes } from 'svelte/elements';

	interface Props extends HTMLInputAttributes {
		value?: string;
	}

	let { value = $bindable(''), ...rest }: Props = $props();
</script>

<input bind:value {...rest} />

<style>
	input {
		width: 100%;
	}
</style>
