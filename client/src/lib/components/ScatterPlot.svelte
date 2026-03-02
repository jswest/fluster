<script lang="ts">
	import Input from './Input.svelte';

	type Point = {
		itemId: number;
		x: number;
		y: number;
		clusterId: number;
		recordName: string;
		embeddingTextPreview: string;
		imageArtifactId: string | null;
	};

	interface Props {
		points: Point[];
		getColor: (clusterId: number) => string;
		focusClusterId?: number | null;
		onSelect?: (itemId: number) => void;
	}

	let { points, getColor, focusClusterId = null, onSelect }: Props = $props();

	// --- Canvas refs ---
	let canvas: HTMLCanvasElement | undefined = $state();
	let container: HTMLDivElement | undefined = $state();
	const POINT_RADIUS = 3;
	const HOVER_RADIUS = 4;
	const HIT_THRESHOLD = 20;
	const PADDING = 40;
	const MIN_ZOOM = 0.5;

	// --- Transform state (pan + zoom) ---
	let panX = $state(0);
	let panY = $state(0);
	let zoom = $state(1);
	let dragging = $state(false);
	let dragStartX = 0;
	let dragStartY = 0;
	let panStartX = 0;
	let panStartY = 0;
	let dragDistance = 0;

	// --- Interaction state ---
	let hoveredPoint: Point | null = $state(null);
	let tooltipX = $state(0);
	let tooltipY = $state(0);
	let selectedItemId: number | null = $state(null);

	// --- Search + filter state ---
	let searchInput = $state('');
	let searchQuery = $state('');
	let filterMode: 'fade' | 'hide' = $state('fade');
	let debounceTimer: ReturnType<typeof setTimeout> | undefined;

	$effect(() => {
		clearTimeout(debounceTimer);
		const value = searchInput;
		debounceTimer = setTimeout(() => {
			searchQuery = value;
		}, 200);
		return () => clearTimeout(debounceTimer);
	});

	// --- Data bounds ---
	const bounds = $derived.by(() => {
		if (points.length === 0) return { minX: 0, maxX: 1, minY: 0, maxY: 1 };
		let minX = Infinity, maxX = -Infinity, minY = Infinity, maxY = -Infinity;
		for (const p of points) {
			if (p.x < minX) minX = p.x;
			if (p.x > maxX) maxX = p.x;
			if (p.y < minY) minY = p.y;
			if (p.y > maxY) maxY = p.y;
		}
		const dx = maxX - minX || 1;
		const dy = maxY - minY || 1;
		return { minX: minX - dx * 0.05, maxX: maxX + dx * 0.05, minY: minY - dy * 0.05, maxY: maxY + dy * 0.05 };
	});

	// --- Search matching ---
	const matchSet = $derived.by(() => {
		const q = searchQuery.trim().toLowerCase();
		if (!q) return null;
		const tokens = q.split(/\s+/);
		const matched = new Set<number>();
		for (const p of points) {
			const haystack = (p.recordName + ' ' + p.embeddingTextPreview).toLowerCase();
			if (tokens.every((t) => haystack.includes(t))) {
				matched.add(p.itemId);
			}
		}
		return matched;
	});

	const matchCount = $derived(matchSet ? matchSet.size : points.length);

	// --- Visibility check: combines search filter + cluster focus ---
	function isPointVisible(
		p: Point,
		ms: Set<number> | null,
		fcId: number | null,
		fm: 'fade' | 'hide'
	): 'full' | 'faded' | 'hidden' {
		const searchMatch = !ms || ms.has(p.itemId);
		const clusterMatch = fcId == null || p.clusterId === fcId;
		const visible = searchMatch && clusterMatch;
		if (visible) return 'full';
		return fm === 'hide' ? 'hidden' : 'faded';
	}

	// --- Coordinate transforms ---
	function dataToScreen(dataX: number, dataY: number, width: number, height: number): [number, number] {
		const b = bounds;
		const plotW = width - PADDING * 2;
		const plotH = height - PADDING * 2;
		const sx = PADDING + ((dataX - b.minX) / (b.maxX - b.minX)) * plotW;
		const sy = PADDING + ((dataY - b.minY) / (b.maxY - b.minY)) * plotH;
		return [(sx * zoom) + panX, (sy * zoom) + panY];
	}

	// --- Find nearest point ---
	function findNearest(mx: number, my: number, width: number, height: number): Point | null {
		let best: Point | null = null;
		let bestDist = HIT_THRESHOLD * HIT_THRESHOLD;
		for (const p of points) {
			if (isPointVisible(p, matchSet, focusClusterId, filterMode) === 'hidden') continue;
			const [sx, sy] = dataToScreen(p.x, p.y, width, height);
			const dx = sx - mx;
			const dy = sy - my;
			const d = dx * dx + dy * dy;
			if (d < bestDist) {
				bestDist = d;
				best = p;
			}
		}
		return best;
	}

	// --- Drawing ---
	$effect(() => {
		if (!canvas || !container) return;
		const ctx = canvas.getContext('2d');
		if (!ctx) return;

		const width = container.clientWidth;
		const height = container.clientHeight;
		if (width <= 0 || height <= 0) return;

		const dpr = window.devicePixelRatio || 1;
		canvas.width = width * dpr;
		canvas.height = height * dpr;
		canvas.style.width = width + 'px';
		canvas.style.height = height + 'px';
		ctx.setTransform(dpr, 0, 0, dpr, 0, 0);

		// Clear
		ctx.clearRect(0, 0, width, height);
		ctx.fillStyle = '#FFFFFF';
		ctx.fillRect(0, 0, width, height);

		// Access reactive dependencies so the effect re-runs on state changes
		const _zoom = zoom;
		const _panX = panX;
		const _panY = panY;
		const _selectedItemId = selectedItemId;
		const _hoveredPoint = hoveredPoint;
		const _matchSet = matchSet;
		const _focusClusterId = focusClusterId;
		const _filterMode = filterMode;

		// Draw points
		for (const p of points) {
			const vis = isPointVisible(p, _matchSet, _focusClusterId, _filterMode);
			if (vis === 'hidden') continue;

			const [sx, sy] = dataToScreen(p.x, p.y, width, height);

			// Skip offscreen
			if (sx < -10 || sx > width + 10 || sy < -10 || sy > height + 10) continue;

			const isHovered = _hoveredPoint?.itemId === p.itemId;
			const isSelected = _selectedItemId === p.itemId;

			ctx.globalAlpha = vis === 'faded' ? 0.1 : 1;
			ctx.fillStyle = getColor(p.clusterId);
			ctx.beginPath();
			ctx.arc(sx, sy, isHovered ? HOVER_RADIUS : POINT_RADIUS, 0, Math.PI * 2);
			ctx.fill();

			if (isSelected) {
				ctx.globalAlpha = 1;
				ctx.strokeStyle = '#000000';
				ctx.lineWidth = 2;
				ctx.beginPath();
				ctx.arc(sx, sy, POINT_RADIUS + 3, 0, Math.PI * 2);
				ctx.stroke();
			}
		}

		ctx.globalAlpha = 1;
	});

	// --- Event handlers ---
	function handleMouseDown(e: MouseEvent) {
		dragging = true;
		dragDistance = 0;
		dragStartX = e.clientX;
		dragStartY = e.clientY;
		panStartX = panX;
		panStartY = panY;
	}

	function handleMouseMove(e: MouseEvent) {
		if (!canvas || !container) return;
		const rect = canvas.getBoundingClientRect();
		const mx = e.clientX - rect.left;
		const my = e.clientY - rect.top;

		if (dragging) {
			const dx = e.clientX - dragStartX;
			const dy = e.clientY - dragStartY;
			dragDistance = Math.sqrt(dx * dx + dy * dy);
			panX = panStartX + dx;
			panY = panStartY + dy;
			hoveredPoint = null;
		} else {
			const width = container.clientWidth;
			const height = container.clientHeight;
			const nearest = findNearest(mx, my, width, height);
			hoveredPoint = nearest;
			if (nearest) {
				tooltipX = mx;
				tooltipY = my;
			}
		}
	}

	function handleMouseUp(e: MouseEvent) {
		if (dragging && dragDistance < 3 && canvas && container) {
			const rect = canvas.getBoundingClientRect();
			const mx = e.clientX - rect.left;
			const my = e.clientY - rect.top;
			const width = container.clientWidth;
			const height = container.clientHeight;
			const nearest = findNearest(mx, my, width, height);
			if (nearest) {
				selectedItemId = nearest.itemId;
				onSelect?.(nearest.itemId);
			} else {
				selectedItemId = null;
			}
		}
		dragging = false;
	}

	function handleWheel(e: WheelEvent) {
		e.preventDefault();
		if (!canvas) return;
		const rect = canvas.getBoundingClientRect();
		const mx = e.clientX - rect.left;
		const my = e.clientY - rect.top;

		const factor = e.deltaY > 0 ? 0.9 : 1.1;
		const newZoom = Math.max(MIN_ZOOM, Math.min(zoom * factor, 50));

		// Zoom centered on cursor
		panX = mx - ((mx - panX) / zoom) * newZoom;
		panY = my - ((my - panY) / zoom) * newZoom;
		zoom = newZoom;
	}

	function handleMouseLeave() {
		hoveredPoint = null;
		if (dragging) dragging = false;
	}
</script>

<div class="scatter-container">
	<div class="controls">
		<div class="search-wrap">
			<Input placeholder="Search points..." bind:value={searchInput} />
		</div>
		<span class="muted match-count">{matchCount} / {points.length}</span>
		<button
			class="mode-btn"
			class:active={filterMode === 'fade'}
			onclick={() => filterMode = 'fade'}
		>Fade</button>
		<button
			class="mode-btn"
			class:active={filterMode === 'hide'}
			onclick={() => filterMode = 'hide'}
		>Hide</button>
		<button
			class="mode-btn"
			onclick={() => { zoom = 1; panX = 0; panY = 0; }}
		>Reset view</button>
	</div>

	<div class="canvas-wrap" bind:this={container}>
		<canvas
			bind:this={canvas}
			onmousedown={handleMouseDown}
			onmousemove={handleMouseMove}
			onmouseup={handleMouseUp}
			onmouseleave={handleMouseLeave}
			onwheel={handleWheel}
		></canvas>

		{#if hoveredPoint && !dragging}
			<div
				class="tooltip"
				style="left: {tooltipX + 12}px; top: {tooltipY - 12}px;"
			>
				{#if hoveredPoint.imageArtifactId}
					<img
						src="/api/artifacts/{hoveredPoint.imageArtifactId}"
						alt=""
						class="tooltip-thumb"
					/>
				{/if}
				<div class="tooltip-name">{hoveredPoint.recordName || 'unnamed'}</div>
				<div class="tooltip-preview">{hoveredPoint.embeddingTextPreview.slice(0, 80)}{hoveredPoint.embeddingTextPreview.length > 80 ? '...' : ''}</div>
				<div class="tooltip-cluster muted">cluster {hoveredPoint.clusterId}</div>
			</div>
		{/if}
	</div>
</div>

<style>
	.scatter-container {
		width: 100%;
		height: 100%;
		position: relative;
	}

	.controls {
		position: absolute;
		top: 0.5rem;
		right: 0.5rem;
		display: flex;
		align-items: center;
		gap: 0.5rem;
		z-index: 5;
	}

	.search-wrap {
		width: 14rem;
	}

	.match-count {
		font-size: 0.8125rem;
		white-space: nowrap;
	}

	.mode-btn {
		padding: 0.25rem 0.75rem;
		font-size: 0.8125rem;
	}

	.mode-btn.active {
		background: var(--color-primary-light);
		color: var(--color-primary-dark);
		border: 1px solid var(--color-primary-dark);
	}

	.canvas-wrap {
		position: absolute;
		inset: 0;
		cursor: grab;
	}

	.canvas-wrap:active {
		cursor: grabbing;
	}

	canvas {
		display: block;
	}

	.tooltip {
		position: absolute;
		background: var(--color-primary-dark);
		color: var(--color-primary-light);
		padding: 0.5rem;
		font-size: 0.75rem;
		max-width: 20rem;
		pointer-events: none;
		z-index: 10;
		line-height: 1.4;
	}

	.tooltip-thumb {
		max-width: 8rem;
		max-height: 6rem;
		object-fit: contain;
		margin-bottom: 0.25rem;
		display: block;
	}

	.tooltip-name {
		font-weight: 600;
		margin-bottom: 0.25rem;
	}

	.tooltip-preview {
		word-break: break-word;
		margin-bottom: 0.25rem;
	}

	.tooltip-cluster {
		font-size: 0.6875rem;
		color: var(--color-secondary-dark);
	}
</style>
