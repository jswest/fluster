<script lang="ts">
	type Point = {
		itemId: number;
		x: number; // grid row (grid_i)
		y: number; // grid col (grid_j)
		clusterId: number;
		recordName: string;
		embeddingText: string;
		metadata: Record<string, unknown>;
		imageArtifactId: string | null;
	};

	type SomGridData = {
		gridX: number;
		gridY: number;
		nodes: { i: number; j: number; umatrixDist: number }[];
	};

	interface Props {
		points: Point[];
		somGrid: SomGridData;
		getColor: (clusterId: number) => string;
		focusClusterId?: number | null;
		onSelect?: (itemId: number) => void;
	}

	let { points, somGrid, getColor, focusClusterId = null, onSelect }: Props = $props();

	type Cell = {
		i: number;
		j: number;
		items: Point[];
		dominantCluster: number | null;
		umatrixDist: number;
	};

	const cellKey = (i: number, j: number) => `${i},${j}`;

	// One cell per grid node, in row-major (i outer, j inner) order so the CSS
	// grid auto-places them left-to-right, top-to-bottom.
	const cells = $derived.by(() => {
		const umatrix = new Map<string, number>();
		for (const n of somGrid.nodes) umatrix.set(cellKey(n.i, n.j), n.umatrixDist);

		const bucket = new Map<string, Point[]>();
		for (const p of points) {
			const k = cellKey(p.x, p.y);
			let arr = bucket.get(k);
			if (!arr) bucket.set(k, (arr = []));
			arr.push(p);
		}

		const out: Cell[] = [];
		for (let i = 0; i < somGrid.gridX; i++) {
			for (let j = 0; j < somGrid.gridY; j++) {
				const items = bucket.get(cellKey(i, j)) ?? [];
				out.push({
					i,
					j,
					items,
					dominantCluster: dominantCluster(items),
					umatrixDist: umatrix.get(cellKey(i, j)) ?? 0
				});
			}
		}
		return out;
	});

	const maxCount = $derived(cells.reduce((m, c) => Math.max(m, c.items.length), 0));

	function dominantCluster(items: Point[]): number | null {
		if (items.length === 0) return null;
		const counts = new Map<number, number>();
		for (const p of items) counts.set(p.clusterId, (counts.get(p.clusterId) ?? 0) + 1);
		let best = items[0].clusterId;
		let bestN = -1;
		for (const [id, n] of counts) {
			if (n > bestN) {
				bestN = n;
				best = id;
			}
		}
		return best;
	}

	function cellMatchesFocus(cell: Cell): boolean {
		if (focusClusterId == null) return true;
		return cell.items.some((p) => p.clusterId === focusClusterId);
	}

	function cellFill(cell: Cell): string {
		if (cell.dominantCluster == null) return 'transparent';
		const intensity = maxCount > 0 ? 0.35 + 0.65 * (cell.items.length / maxCount) : 0;
		const color = getColor(cell.dominantCluster);
		return mixWithAlpha(color, intensity);
	}

	// A hex color at a given alpha, as rgba().
	function mixWithAlpha(hex: string, alpha: number): string {
		const m = /^#?([0-9a-f]{6})$/i.exec(hex.trim());
		if (!m) return hex;
		const n = parseInt(m[1], 16);
		const r = (n >> 16) & 255;
		const g = (n >> 8) & 255;
		const b = n & 255;
		return `rgba(${r}, ${g}, ${b}, ${alpha.toFixed(3)})`;
	}

	// --- Interaction ---
	let hovered: Cell | null = $state(null);
	let tooltipX = $state(0);
	let tooltipY = $state(0);
	let selectedKey: string | null = $state(null);
	let container: HTMLDivElement | undefined = $state();

	function handleEnter(cell: Cell, e: MouseEvent) {
		hovered = cell;
		moveTooltip(e);
	}

	function moveTooltip(e: MouseEvent) {
		if (!container) return;
		const rect = container.getBoundingClientRect();
		tooltipX = e.clientX - rect.left;
		tooltipY = e.clientY - rect.top;
	}

	function handleClick(cell: Cell) {
		if (cell.items.length === 0) {
			selectedKey = null;
			return;
		}
		selectedKey = cellKey(cell.i, cell.j);
		onSelect?.(cell.items[0].itemId);
	}
</script>

<div class="som-wrap" bind:this={container}>
	<div
		class="grid"
		style="grid-template-columns: repeat({somGrid.gridY}, 1fr); grid-template-rows: repeat({somGrid.gridX}, 1fr); aspect-ratio: {somGrid.gridY} / {somGrid.gridX};"
		role="presentation"
		onmouseleave={() => (hovered = null)}
	>
		{#each cells as cell (cellKey(cell.i, cell.j))}
			<button
				type="button"
				class="cell"
				class:faded={!cellMatchesFocus(cell)}
				class:selected={selectedKey === cellKey(cell.i, cell.j)}
				class:empty={cell.items.length === 0}
				style="background:
					linear-gradient(rgba(255,255,255,{(cell.umatrixDist * 0.12).toFixed(3)}), rgba(255,255,255,{(cell.umatrixDist * 0.12).toFixed(3)})),
					{cellFill(cell)};"
				aria-label="cell {cell.i},{cell.j}, {cell.items.length} items"
				onmouseenter={(e) => handleEnter(cell, e)}
				onmousemove={moveTooltip}
				onclick={() => handleClick(cell)}
			></button>
		{/each}
	</div>

	{#if hovered}
		<div class="tooltip" style="left: {tooltipX + 12}px; top: {tooltipY - 12}px;">
			<div class="tooltip-head">cell {hovered.i},{hovered.j}</div>
			{#if hovered.items.length === 0}
				<div class="muted">empty</div>
			{:else}
				<div>{hovered.items.length} item{hovered.items.length === 1 ? '' : 's'}</div>
				{#if hovered.dominantCluster != null}
					<div class="tooltip-cluster muted">
						<span class="swatch" style="background: {getColor(hovered.dominantCluster)}"></span>
						cluster {hovered.dominantCluster}
					</div>
				{/if}
				{#if hovered.items.length === 1}
					<div class="tooltip-name">{hovered.items[0].recordName || 'unnamed'}</div>
				{/if}
			{/if}
		</div>
	{/if}
</div>

<style>
	.som-wrap {
		position: absolute;
		inset: 0;
		display: flex;
		align-items: center;
		justify-content: center;
		padding: 2rem;
		background: var(--color-bg);
	}

	.grid {
		display: grid;
		gap: 1px;
		max-width: 100%;
		max-height: 100%;
		width: auto;
		height: 100%;
	}

	.cell {
		padding: 0;
		margin: 0;
		border: 1px solid rgba(255, 255, 255, 0.06);
		border-radius: 0;
		cursor: pointer;
		transition: opacity 0.1s;
		min-width: 0;
		min-height: 0;
	}

	.cell.empty {
		cursor: default;
	}

	.cell.faded {
		opacity: 0.15;
	}

	.cell:hover {
		outline: 1px solid var(--color-fg);
		outline-offset: -1px;
	}

	.cell.selected {
		outline: 2px solid var(--color-fg);
		outline-offset: -2px;
	}

	.tooltip {
		position: absolute;
		background: var(--color-bg-secondary);
		color: var(--color-fg);
		padding: 0.5rem;
		font-size: 0.75rem;
		max-width: 16rem;
		pointer-events: none;
		z-index: 10;
		line-height: 1.5;
	}

	.tooltip-head {
		font-weight: 600;
	}

	.tooltip-name {
		word-break: break-word;
		margin-top: 0.25rem;
	}

	.tooltip-cluster {
		display: flex;
		align-items: center;
		gap: 0.375rem;
		font-size: 0.6875rem;
	}

	.swatch {
		display: inline-block;
		width: 0.625rem;
		height: 0.625rem;
		border-radius: 2px;
	}
</style>
