<script lang="ts">
	import { Pill, Button, Input, Card, EmptyState, Spinner } from '$lib';

	let inputValue = $state('');
</script>

<div class="container stack" style="padding-top: 2rem; padding-bottom: 2rem;">
	<h1>fluster — component gallery</h1>

	<section class="stack">
		<h2>Card</h2>
		<Card>
			<p style="margin: 0;">A simple card with a black border and white background.</p>
		</Card>
	</section>

	<section class="stack">
		<h2>Pill</h2>
		<div class="row" style="flex-wrap: wrap;">
			<Pill variant="neutral">neutral</Pill>
			<Pill variant="info">info</Pill>
			<Pill variant="success">success</Pill>
			<Pill variant="warn">warn</Pill>
			<Pill variant="danger">danger</Pill>
		</div>
		<div class="row" style="flex-wrap: wrap;">
			<Pill variant="neutral" size="sm">sm neutral</Pill>
			<Pill variant="info" size="sm" icon="i">sm info</Pill>
			<Pill variant="success" size="sm" title="All good">sm success</Pill>
		</div>
	</section>

	<section class="stack">
		<h2>Button</h2>
		<div class="row">
			<Button>Default</Button>
			<Button disabled>Disabled</Button>
		</div>
	</section>

	<section class="stack">
		<h2>Input</h2>
		<div style="max-width: 24rem;">
			<Input placeholder="Type something..." bind:value={inputValue} />
			{#if inputValue}
				<p class="muted">Value: {inputValue}</p>
			{/if}
		</div>
	</section>

	<section class="stack">
		<h2>Spinner</h2>
		<div class="row" style="align-items: center;">
			<Spinner size="sm" />
			<span class="muted">sm</span>
			<Spinner size="md" />
			<span class="muted">md</span>
		</div>
	</section>

	<section class="stack">
		<h2>EmptyState</h2>
		<Card>
			<EmptyState message="No clusters found." icon="?" />
		</Card>
	</section>
</div>
