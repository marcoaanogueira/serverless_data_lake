/**
 * Data Lake API Client
 *
 * API client for the serverless data lake backend.
 */

const API_BASE_URL = import.meta.env.VITE_API_URL || '';

class DataLakeClient {
  constructor() {
    this.baseUrl = API_BASE_URL;
  }

  async request(endpoint, options = {}) {
    const url = `${this.baseUrl}${endpoint}`;

    const config = {
      headers: {
        'Content-Type': 'application/json',
        ...options.headers,
      },
      ...options,
    };

    if (options.body && typeof options.body === 'object') {
      config.body = JSON.stringify(options.body);
    }

    const response = await fetch(url, config);

    if (!response.ok) {
      const error = await response.json().catch(() => ({ message: 'Request failed' }));
      throw new Error(error.message || `HTTP error! status: ${response.status}`);
    }

    return response.json();
  }

  // Generic CRUD operations for entities
  createEntityClient(entityPath) {
    return {
      list: async (orderBy, limit) => {
        let endpoint = `/api/${entityPath}`;
        const params = new URLSearchParams();
        if (orderBy) params.append('order_by', orderBy);
        if (limit) params.append('limit', limit.toString());
        if (params.toString()) endpoint += `?${params.toString()}`;
        return this.request(endpoint);
      },

      get: async (id) => {
        return this.request(`/api/${entityPath}/${id}`);
      },

      create: async (data) => {
        return this.request(`/api/${entityPath}`, {
          method: 'POST',
          body: data,
        });
      },

      update: async (id, data) => {
        return this.request(`/api/${entityPath}/${id}`, {
          method: 'PUT',
          body: data,
        });
      },

      delete: async (id) => {
        return this.request(`/api/${entityPath}/${id}`, {
          method: 'DELETE',
        });
      },
    };
  }
}

// Create the main client instance
const client = new DataLakeClient();

// Export entity clients
export const dataLakeApi = {
  entities: {
    IngestionEndpoint: client.createEntityClient('endpoints'),
    GoldJob: client.createEntityClient('jobs'),
    QueryHistory: client.createEntityClient('query-history'),
  },

  // Query execution endpoint
  executeQuery: async (query) => {
    return client.request('/api/consumption/query', {
      method: 'POST',
      body: { query },
    });
  },

  // Data ingestion endpoint
  ingestData: async (domain, table, data) => {
    return client.request(`/api/ingestion/${domain}/${table}`, {
      method: 'POST',
      body: data,
    });
  },
};

export default dataLakeApi;
