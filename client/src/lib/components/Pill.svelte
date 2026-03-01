<script lang="ts">
	import type { Snippet } from 'svelte';

	interface Props {
		variant?: 'neutral' | 'info' | 'success' | 'warn' | 'danger';
		size?: 'sm' | 'md';
		icon?: string;
		title?: string;
		children: Snippet;
	}

	let { variant = 'neutral', size = 'md', icon, title, children }: Props = $props();
</script>

<span class="pill pill-{size} pill-{variant}" {title}>
	{#if icon}<span class="pill-icon">{icon}</span>{/if}
	{@render children()}
</span>

<style>
	.pill {
		display: inline-flex;
		align-items: center;
		gap: 0.25rem;
		border: 1px solid;
		font-family: inherit;
		line-height: 1;
		white-space: nowrap;
	}

	.pill-sm {
		font-size: 0.75rem;
		padding: 0.125rem 0.375rem;
	}

	.pill-md {
		font-size: 0.875rem;
		padding: 0.25rem 0.5rem;
	}

	.pill-neutral { border-color: var(--color-secondary-dark); color: var(--color-secondary-dark); }
	.pill-info    { border-color: var(--color-tertiary);       color: var(--color-tertiary); }
	.pill-success { border-color: var(--color-success);        color: var(--color-success); }
	.pill-warn    { border-color: var(--color-secondary-light); color: var(--color-secondary-light); }
	.pill-danger  { border-color: var(--color-tertiary-invert); color: var(--color-tertiary-invert); }

	.pill-icon {
		display: inline-flex;
	}
</style>
