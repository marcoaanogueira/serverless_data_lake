/**
 * Application Parameters
 *
 * These parameters configure the frontend application.
 * In production, these will be injected at build time or via environment variables.
 */

export const appParams = {
  appId: import.meta.env.VITE_APP_ID || 'data-platform',
  apiUrl: import.meta.env.VITE_API_URL || '',
  appBaseUrl: import.meta.env.VITE_APP_BASE_URL || window.location.origin,
};
