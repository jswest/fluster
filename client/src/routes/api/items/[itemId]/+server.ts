import { json, error } from '@sveltejs/kit';
import { getItemDetail } from '$lib/server/queries/items';

export function GET({ params }) {
	const itemId = Number(params.itemId);
	if (Number.isNaN(itemId)) throw error(400, 'Invalid item ID');

	const item = getItemDetail(itemId);
	if (!item) throw error(404, 'Item not found');

	return json(item);
}
