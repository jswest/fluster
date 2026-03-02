<script lang="ts">
	import '../app.css';
	import favicon from '$lib/assets/favicon.svg';

	let { children, data } = $props();
</script>

<svelte:head>
	<link rel="icon" href={favicon} />
	<title>{data.projectName} — fluster</title>
</svelte:head>

<nav>
	<span class="project-name">{data.projectName}</span>
	<a href="/jobs">Jobs</a>
	<a href="/runs">Runs</a>
	<span class="db-path muted">{data.dbPath}</span>
</nav>

{@render children()}

<footer>
	<span class="muted">fluster v1.0.0 — read-only</span>
</footer>

<style>
	nav {
		display: flex;
		align-items: center;
		gap: 1.5rem;
		padding: 0.75rem 1rem;
		border-bottom: 1px solid var(--color-primary-dark);
	}

	.project-name {
		font-weight: 700;
	}

	nav a {
		color: inherit;
		text-decoration: none;
		font-weight: 600;
	}

	nav a:hover {
		border-bottom: 2px solid var(--color-primary-dark);
	}

	.db-path {
		margin-left: auto;
		font-size: 0.75rem;
		overflow: hidden;
		text-overflow: ellipsis;
		white-space: nowrap;
		max-width: 20rem;
	}

	footer {
		padding: 0.5rem 1rem;
		text-align: center;
		font-size: 0.75rem;
	}
</style>
