import type { ComponentProps } from 'svelte';
import type Pill from '$lib/components/Pill.svelte';

type PillVariant = NonNullable<ComponentProps<typeof Pill>['variant']>;

export const statusVariant: Record<string, PillVariant> = {
	queued: 'neutral',
	running: 'warn',
	succeeded: 'success',
	failed: 'danger',
	canceled: 'info'
};

export function formatTime(iso: string | null): string {
	if (!iso) return '\u2014';
	return new Date(iso + 'Z').toLocaleString();
}

export function formatPercent(n: number): string {
	return (n * 100).toFixed(1) + '%';
}
