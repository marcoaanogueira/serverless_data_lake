import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';

// Mock the API client
vi.mock('@/api/dataLakeClient', () => ({
  default: {
    endpoints: {
      list: vi.fn().mockResolvedValue([]),
      create: vi.fn().mockResolvedValue({
        name: 'test_table',
        domain: 'sales',
        endpoint_url: '/ingestion/sales/test_table',
        columns: [{ name: 'id', type: 'integer' }],
      }),
    },
    goldJobs: {
      list: vi.fn().mockResolvedValue([]),
    },
    queryHistory: {
      list: vi.fn().mockResolvedValue([]),
    },
  },
}));

// Import after mocking
import DataPlatform from '@/pages/DataPlatform';
import dataLakeApi from '@/api/dataLakeClient';

const createTestQueryClient = () =>
  new QueryClient({
    defaultOptions: {
      queries: {
        retry: false,
      },
    },
  });

const renderWithProviders = (ui) => {
  const queryClient = createTestQueryClient();
  return render(
    <QueryClientProvider client={queryClient}>
      {ui}
    </QueryClientProvider>
  );
};

describe('Endpoint Creation Flow', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('renders the Extract module by default', () => {
    renderWithProviders(<DataPlatform />);
    // There are multiple "Extract" texts - one in navbar and one in section header
    const extractElements = screen.getAllByText('Extract');
    expect(extractElements.length).toBeGreaterThanOrEqual(1);
  });

  it('shows Create New and View All tabs', () => {
    renderWithProviders(<DataPlatform />);
    expect(screen.getByText('Create New')).toBeInTheDocument();
    expect(screen.getByText(/View All/)).toBeInTheDocument();
  });

  it('shows New Endpoint form when Create New is active', () => {
    renderWithProviders(<DataPlatform />);
    expect(screen.getByText('New Endpoint')).toBeInTheDocument();
  });

  it('renders domain and table name inputs', () => {
    renderWithProviders(<DataPlatform />);
    expect(screen.getByPlaceholderText('sales, ads, finance...')).toBeInTheDocument();
    expect(screen.getByPlaceholderText('my_dataset')).toBeInTheDocument();
  });

  it('renders schema mode tabs', () => {
    renderWithProviders(<DataPlatform />);
    expect(screen.getByText('Manual')).toBeInTheDocument();
    expect(screen.getByText('Auto')).toBeInTheDocument();
    expect(screen.getByText('JSON')).toBeInTheDocument();
  });

  it('shows manual schema form by default', () => {
    renderWithProviders(<DataPlatform />);
    expect(screen.getByPlaceholderText('column_name')).toBeInTheDocument();
  });

  it('switches to Auto Inference when clicking Auto tab', async () => {
    renderWithProviders(<DataPlatform />);

    fireEvent.click(screen.getByText('Auto'));

    await waitFor(() => {
      expect(screen.getByText('Auto Inference')).toBeInTheDocument();
    });
  });

  it('switches to Single Column when clicking JSON tab', async () => {
    renderWithProviders(<DataPlatform />);

    fireEvent.click(screen.getByText('JSON'));

    await waitFor(() => {
      expect(screen.getByText('Single Column (JSON)')).toBeInTheDocument();
    });
  });

  it('shows validation error when domain is empty', async () => {
    renderWithProviders(<DataPlatform />);

    const createButton = screen.getByText('Create Endpoint');
    fireEvent.click(createButton);

    await waitFor(() => {
      expect(screen.getByText('Domain is required')).toBeInTheDocument();
    });
  });

  it('shows validation error for invalid domain format', async () => {
    renderWithProviders(<DataPlatform />);

    const domainInput = screen.getByPlaceholderText('sales, ads, finance...');
    fireEvent.change(domainInput, { target: { value: '123invalid' } });

    const createButton = screen.getByText('Create Endpoint');
    fireEvent.click(createButton);

    await waitFor(() => {
      expect(screen.getByText('Domain must be snake_case')).toBeInTheDocument();
    });
  });

  it('shows validation error when table name is empty', async () => {
    renderWithProviders(<DataPlatform />);

    const domainInput = screen.getByPlaceholderText('sales, ads, finance...');
    fireEvent.change(domainInput, { target: { value: 'sales' } });

    const createButton = screen.getByText('Create Endpoint');
    fireEvent.click(createButton);

    await waitFor(() => {
      expect(screen.getByText('Table name is required')).toBeInTheDocument();
    });
  });

  it('shows validation error for empty columns in manual mode', async () => {
    renderWithProviders(<DataPlatform />);

    const domainInput = screen.getByPlaceholderText('sales, ads, finance...');
    const tableInput = screen.getByPlaceholderText('my_dataset');

    fireEvent.change(domainInput, { target: { value: 'sales' } });
    fireEvent.change(tableInput, { target: { value: 'orders' } });

    const createButton = screen.getByText('Create Endpoint');
    fireEvent.click(createButton);

    await waitFor(() => {
      expect(screen.getByText('At least one column is required')).toBeInTheDocument();
    });
  });

  it('creates endpoint successfully with valid data', async () => {
    renderWithProviders(<DataPlatform />);

    const domainInput = screen.getByPlaceholderText('sales, ads, finance...');
    const tableInput = screen.getByPlaceholderText('my_dataset');
    const columnInput = screen.getByPlaceholderText('column_name');

    fireEvent.change(domainInput, { target: { value: 'sales' } });
    fireEvent.change(tableInput, { target: { value: 'orders' } });
    fireEvent.change(columnInput, { target: { value: 'id' } });

    const createButton = screen.getByText('Create Endpoint');
    fireEvent.click(createButton);

    await waitFor(() => {
      expect(dataLakeApi.endpoints.create).toHaveBeenCalledWith({
        name: 'orders',
        domain: 'sales',
        mode: 'manual',
        columns: expect.arrayContaining([
          expect.objectContaining({ name: 'id', description: null })
        ]),
      });
    });
  });

  it('allows creating endpoint with single_column mode', async () => {
    renderWithProviders(<DataPlatform />);

    // Fill in domain and table
    const domainInput = screen.getByPlaceholderText('sales, ads, finance...');
    const tableInput = screen.getByPlaceholderText('my_dataset');

    fireEvent.change(domainInput, { target: { value: 'logs' } });
    fireEvent.change(tableInput, { target: { value: 'events' } });

    // Switch to JSON mode
    fireEvent.click(screen.getByText('JSON'));

    await waitFor(() => {
      expect(screen.getByText('Single Column (JSON)')).toBeInTheDocument();
    });

    const createButton = screen.getByText('Create Endpoint');
    fireEvent.click(createButton);

    await waitFor(() => {
      expect(dataLakeApi.endpoints.create).toHaveBeenCalledWith({
        name: 'events',
        domain: 'logs',
        mode: 'single_column',
        columns: [{ name: 'data', type: 'json', required: true, primary_key: false }],
      });
    });
  });

  it('renders Tadpole logo in navbar', () => {
    renderWithProviders(<DataPlatform />);
    expect(screen.getByText('Tadpole')).toBeInTheDocument();
  });

  it('renders navigation tabs (Extract, Transform, Load)', () => {
    renderWithProviders(<DataPlatform />);
    // Use getAllByText since some texts appear multiple times
    expect(screen.getAllByText('Extract').length).toBeGreaterThanOrEqual(1);
    expect(screen.getAllByText('Transform').length).toBeGreaterThanOrEqual(1);
    expect(screen.getAllByText('Load').length).toBeGreaterThanOrEqual(1);
  });

  it('switches to Transform module when clicking Transform tab', async () => {
    renderWithProviders(<DataPlatform />);

    fireEvent.click(screen.getByRole('button', { name: /Transform/i }));

    await waitFor(() => {
      expect(screen.getByText('New Job')).toBeInTheDocument();
    });
  });

  it('switches to Load module when clicking Load tab', async () => {
    renderWithProviders(<DataPlatform />);

    fireEvent.click(screen.getByRole('button', { name: /Load/i }));

    await waitFor(() => {
      expect(screen.getByText('Tables')).toBeInTheDocument();
    });
  });
});
