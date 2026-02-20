/**
 * Data Lake API Client
 *
 * API client for the serverless data lake backend.
 * Connects to the Endpoints API for schema management.
 */

const API_BASE_URL = import.meta.env.VITE_API_URL || '';

class DataLakeClient {
  constructor() {
    this.baseUrl = API_BASE_URL;
  }

  async request(endpoint, options = {}) {
    const url = `${this.baseUrl}${endpoint}`;
    const apiKey = localStorage.getItem('dataLakeApiKey');

    const config = {
      headers: {
        'Content-Type': 'application/json',
        ...(apiKey ? { 'x-api-key': apiKey } : {}),
        ...options.headers,
      },
      ...options,
    };

    if (options.body && typeof options.body === 'object') {
      config.body = JSON.stringify(options.body);
    }

    const response = await fetch(url, config);

    if (!response.ok) {
      // Token expired or revoked — clear session and force re-login
      if (response.status === 401 || response.status === 403) {
        localStorage.removeItem('dataLakeApiKey');
        window.location.reload();
        return;
      }
      const error = await response.json().catch(() => ({ message: 'Request failed' }));
      throw new Error(error.detail || error.message || `HTTP error! status: ${response.status}`);
    }

    return response.json();
  }
}

// Create the main client instance
const client = new DataLakeClient();

/**
 * Endpoints API Client
 *
 * Manages ingestion endpoint schemas with versioning.
 * Schemas are stored as YAML files in S3 with automatic versioning.
 */
const EndpointsClient = {
  /**
   * List all endpoints
   * @param {string} orderBy - Order by field (prefix with - for descending)
   * @param {string} domain - Filter by domain
   */
  list: async (orderBy, domain) => {
    const params = new URLSearchParams();
    if (orderBy) params.append('order_by', orderBy);
    if (domain) params.append('domain', domain);
    const query = params.toString() ? `?${params.toString()}` : '';
    return client.request(`/endpoints${query}`);
  },

  /**
   * Get endpoint by domain and name
   * @param {string} domain - Business domain
   * @param {string} name - Table/dataset name
   * @param {number} version - Specific version (optional, defaults to latest)
   */
  get: async (domain, name, version) => {
    const params = version ? `?version=${version}` : '';
    return client.request(`/endpoints/${domain}/${name}${params}`);
  },

  /**
   * Create a new endpoint
   * @param {object} data - Endpoint data { name, domain, mode, columns, description }
   */
  create: async (data) => {
    return client.request('/endpoints', {
      method: 'POST',
      body: data,
    });
  },

  /**
   * Update an endpoint (creates new version)
   * @param {string} domain - Business domain
   * @param {string} name - Table/dataset name
   * @param {object} data - Updated endpoint data
   */
  update: async (domain, name, data) => {
    return client.request(`/endpoints/${domain}/${name}`, {
      method: 'PUT',
      body: data,
    });
  },

  /**
   * Delete an endpoint and all versions
   * @param {string} domain - Business domain
   * @param {string} name - Table/dataset name
   */
  delete: async (domain, name) => {
    return client.request(`/endpoints/${domain}/${name}`, {
      method: 'DELETE',
    });
  },

  /**
   * Get raw YAML schema
   * @param {string} domain - Business domain
   * @param {string} name - Table/dataset name
   * @param {number} version - Specific version (optional)
   */
  getYaml: async (domain, name, version) => {
    const params = version ? `?version=${version}` : '';
    return client.request(`/endpoints/${domain}/${name}/yaml${params}`);
  },

  /**
   * List all versions of an endpoint
   * @param {string} domain - Business domain
   * @param {string} name - Table/dataset name
   */
  listVersions: async (domain, name) => {
    return client.request(`/endpoints/${domain}/${name}/versions`);
  },

  /**
   * Get presigned URL to download YAML
   * @param {string} domain - Business domain
   * @param {string} name - Table/dataset name
   * @param {number} version - Specific version (optional)
   */
  getDownloadUrl: async (domain, name, version) => {
    const params = version ? `?version=${version}` : '';
    return client.request(`/endpoints/${domain}/${name}/download${params}`);
  },

  /**
   * Infer schema from a sample payload
   * @param {object} data - { payload: { ... } } - Sample JSON payload
   * @returns {object} - { columns: [...], payload_keys: [...] }
   */
  infer: async (data) => {
    return client.request('/endpoints/infer', {
      method: 'POST',
      body: data,
    });
  },
};

/**
 * Gold Jobs API Client
 *
 * Manages gold layer transformation jobs.
 * Jobs are stored as YAML configs in S3 via the Transform Jobs API.
 */
const GoldJobsClient = {
  list: async (orderBy, domain) => {
    const params = new URLSearchParams();
    if (orderBy) params.append('order_by', orderBy);
    if (domain) params.append('domain', domain);
    const query = params.toString() ? `?${params.toString()}` : '';
    return client.request(`/transform/jobs${query}`);
  },
  get: async (domain, jobName) => {
    return client.request(`/transform/jobs/${domain}/${jobName}`);
  },
  create: async (data) => {
    return client.request('/transform/jobs', {
      method: 'POST',
      body: data,
    });
  },
  update: async (domain, jobName, data) => {
    return client.request(`/transform/jobs/${domain}/${jobName}`, {
      method: 'PUT',
      body: data,
    });
  },
  delete: async (id) => {
    // id format: "domain/job_name"
    return client.request(`/transform/jobs/${id}`, {
      method: 'DELETE',
    });
  },
  run: async (domain, jobName) => {
    return client.request(`/transform/jobs/${domain}/${jobName}/run`, {
      method: 'POST',
    });
  },
  getExecution: async (executionId) => {
    return client.request(`/transform/executions/${executionId}`);
  },
};

/**
 * Query History Client (localStorage-based)
 */
const HISTORY_KEY = 'tadpole_query_history';
const HISTORY_MAX = 50;

const QueryHistoryClient = {
  list: async (limit = 20) => {
    try {
      const items = JSON.parse(localStorage.getItem(HISTORY_KEY) || '[]');
      return items.slice(0, limit);
    } catch {
      return [];
    }
  },
  create: async (data) => {
    try {
      const items = JSON.parse(localStorage.getItem(HISTORY_KEY) || '[]');
      const entry = { ...data, id: Date.now(), created_date: new Date().toISOString() };
      items.unshift(entry);
      localStorage.setItem(HISTORY_KEY, JSON.stringify(items.slice(0, HISTORY_MAX)));
      return entry;
    } catch {
      return data;
    }
  },
};

/**
 * AI Agent API Client
 *
 * Manages AI-powered ingestion and transformation pipelines.
 * Jobs run async — call run() then poll getJob() until completion.
 */
const AgentClient = {
  ingestion: {
    plan: async (data) =>
      client.request('/agent/ingestion/plan', { method: 'POST', body: data }),
    run: async (data) =>
      client.request('/agent/ingestion/run', { method: 'POST', body: data }),
    getJob: async (jobId) =>
      client.request(`/agent/ingestion/jobs/${jobId}`),
  },
  transformation: {
    plan: async (data) =>
      client.request('/agent/transformation/plan', { method: 'POST', body: data }),
    run: async (data) =>
      client.request('/agent/transformation/run', { method: 'POST', body: data }),
    getJob: async (jobId) =>
      client.request(`/agent/transformation/jobs/${jobId}`),
  },
};

/**
 * Ingestion Plans API Client
 *
 * Manages AI-generated ingestion plan configs stored in S3.
 * Each plan defines which API endpoints to sync and how often.
 */
const IngestionPlansClient = {
  list: async () => {
    const result = await client.request('/ingestion/plans');
    return result.plans || [];
  },
  get: async (planName) => {
    return client.request(`/ingestion/plans/${planName}`);
  },
  delete: async (planName) => {
    return client.request(`/ingestion/plans/${planName}`, { method: 'DELETE' });
  },
  run: async (planName) => {
    return client.request(`/ingestion/plans/${planName}/run`, { method: 'POST' });
  },
};

// Export the API client
export const dataLakeApi = {
  endpoints: EndpointsClient,
  ingestionPlans: IngestionPlansClient,
  goldJobs: GoldJobsClient,
  queryHistory: QueryHistoryClient,
  agent: AgentClient,

  // Catalog tables (silver + gold) from Glue
  catalogTables: {
    list: async () => {
      const result = await client.request('/consumption/tables');
      return result.tables || [];
    },
  },

  // Silver tables (filtered from catalog)
  silverTables: {
    list: async () => {
      const result = await client.request('/consumption/tables');
      return (result.tables || []).filter(t => t.layer !== 'gold');
    },
  },

  // Query execution endpoint
  executeQuery: async (query) => {
    return client.request(`/consumption/query?sql=${encodeURIComponent(query)}`, {
      method: 'GET',
    });
  },

  // Data ingestion endpoint
  ingestData: async (domain, table, data) => {
    return client.request(`/ingestion/${domain}/${table}`, {
      method: 'POST',
      body: data,
    });
  },
};

export default dataLakeApi;
