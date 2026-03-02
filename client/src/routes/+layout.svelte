<script lang="ts">
	import '../app.css';
	import favicon from '$lib/assets/favicon.svg';

	let { children } = $props();
</script>

<svelte:head>
	<link rel="icon" href={favicon} />
</svelte:head>

<nav>
	<a href="/jobs">Jobs</a>
	<a href="/runs">Runs</a>
</nav>

{@render children()}

<style>
	nav {
		display: flex;
		gap: 1.5rem;
		padding: 0.75rem 1rem;
		border-bottom: 1px solid var(--color-primary-dark);
	}

	nav a {
		color: inherit;
		text-decoration: none;
		font-weight: 600;
	}

	nav a:hover {
		border-bottom: 2px solid var(--color-primary-dark);
	}
</style>
