<script lang="ts">
	interface Props {
		message: string;
		icon?: string;
	}

	let { message, icon }: Props = $props();
</script>

<div class="empty" role="status">
	{#if icon}<span class="empty-icon">{icon}</span>{/if}
	<p class="empty-message">{message}</p>
</div>

<style>
	.empty {
		display: flex;
		flex-direction: column;
		align-items: center;
		justify-content: center;
		padding: 2rem;
		color: var(--color-fg-secondary);
	}

	.empty-icon {
		font-size: 2rem;
		margin-bottom: 0.5rem;
	}

	.empty-message {
		margin: 0;
	}
</style>
