<script lang="ts">
	interface Props {
		size?: 'sm' | 'md';
	}

	let { size = 'md' }: Props = $props();
</script>

<span class="spinner spinner-{size}" role="status" aria-label="Loading">
	<span class="bar"></span>
	<span class="bar"></span>
	<span class="bar"></span>
</span>

<style>
	.spinner {
		display: inline-flex;
		align-items: center;
		gap: 2px;
	}

	.spinner-sm .bar {
		width: 2px;
		height: 12px;
	}

	.spinner-md .bar {
		width: 3px;
		height: 20px;
	}

	.bar {
		background: var(--color-primary-dark);
		animation: pulse 1s ease-in-out infinite;
	}

	.bar:nth-child(2) {
		animation-delay: 0.15s;
	}

	.bar:nth-child(3) {
		animation-delay: 0.3s;
	}

	@keyframes pulse {
		0%, 100% {
			transform: scaleY(0.4);
			opacity: 0.4;
		}
		50% {
			transform: scaleY(1);
			opacity: 1;
		}
	}
</style>
