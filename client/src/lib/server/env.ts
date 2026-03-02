import { building } from '$app/environment';
import { env } from '$env/dynamic/private';

if (!building) {
	if (!env.FLUSTER_DB_PATH) {
		throw new Error('FLUSTER_DB_PATH not set. Use `fluster show` or `fluster chill`.');
	}

	if (!env.FLUSTER_PROJECT_NAME) {
		throw new Error('FLUSTER_PROJECT_NAME not set. Use `fluster show` or `fluster chill`.');
	}
}

export const dbPath: string = env.FLUSTER_DB_PATH ?? '';
export const projectName: string = env.FLUSTER_PROJECT_NAME ?? '';
export const projectDir: string = env.FLUSTER_PROJECT_DIR ?? '';
