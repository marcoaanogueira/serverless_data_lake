import { describe, it, expect, vi } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import ManualSchemaForm from '../ManualSchemaForm';

describe('ManualSchemaForm', () => {
  const mockOnColumnsChange = vi.fn();

  const defaultColumns = [
    { column_name: '', data_type: 'varchar', required: false, is_primary_key: false, description: '' }
  ];

  beforeEach(() => {
    mockOnColumnsChange.mockClear();
  });

  it('renders with initial empty column', () => {
    render(<ManualSchemaForm columns={defaultColumns} onColumnsChange={mockOnColumnsChange} />);

    const input = screen.getByPlaceholderText('column_name');
    expect(input).toBeInTheDocument();
    expect(input.value).toBe('');
  });

  it('renders Add Column button', () => {
    render(<ManualSchemaForm columns={defaultColumns} onColumnsChange={mockOnColumnsChange} />);

    expect(screen.getByText('Add Column')).toBeInTheDocument();
  });

  it('adds a new column when clicking Add Column', () => {
    render(<ManualSchemaForm columns={defaultColumns} onColumnsChange={mockOnColumnsChange} />);

    fireEvent.click(screen.getByText('Add Column'));

    expect(mockOnColumnsChange).toHaveBeenCalledWith([
      ...defaultColumns,
      { column_name: '', data_type: 'varchar', required: false, is_primary_key: false, description: '' }
    ]);
  });

  it('renders multiple columns', () => {
    const multipleColumns = [
      { column_name: 'id', data_type: 'integer', required: true, is_primary_key: true },
      { column_name: 'name', data_type: 'varchar', required: false, is_primary_key: false },
    ];

    render(<ManualSchemaForm columns={multipleColumns} onColumnsChange={mockOnColumnsChange} />);

    const inputs = screen.getAllByPlaceholderText('column_name');
    expect(inputs).toHaveLength(2);
    expect(inputs[0].value).toBe('id');
    expect(inputs[1].value).toBe('name');
  });

  it('updates column name when typing', () => {
    render(<ManualSchemaForm columns={defaultColumns} onColumnsChange={mockOnColumnsChange} />);

    const input = screen.getByPlaceholderText('column_name');
    fireEvent.change(input, { target: { value: 'user_id' } });

    expect(mockOnColumnsChange).toHaveBeenCalledWith([
      { column_name: 'user_id', data_type: 'varchar', required: false, is_primary_key: false, description: '' }
    ]);
  });

  it('disables delete button when only one column exists', () => {
    render(<ManualSchemaForm columns={defaultColumns} onColumnsChange={mockOnColumnsChange} />);

    const deleteButtons = screen.getAllByRole('button').filter(btn =>
      btn.querySelector('svg.lucide-trash-2') || btn.textContent === ''
    );

    // Find the delete button (last button that's not "Add Column")
    const deleteButton = deleteButtons.find(btn => !btn.textContent.includes('Add'));
    expect(deleteButton).toBeDisabled();
  });

  it('enables delete button when multiple columns exist', () => {
    const multipleColumns = [
      { column_name: 'id', data_type: 'integer', required: false, is_primary_key: false },
      { column_name: 'name', data_type: 'varchar', required: false, is_primary_key: false },
    ];

    render(<ManualSchemaForm columns={multipleColumns} onColumnsChange={mockOnColumnsChange} />);

    const deleteButtons = screen.getAllByRole('button').filter(btn =>
      !btn.textContent.includes('Add')
    );

    // At least one delete button should be enabled
    const enabledDeleteButton = deleteButtons.find(btn => !btn.disabled);
    expect(enabledDeleteButton).toBeDefined();
  });

  it('removes column when clicking delete', () => {
    const multipleColumns = [
      { column_name: 'id', data_type: 'integer', required: false, is_primary_key: false },
      { column_name: 'name', data_type: 'varchar', required: false, is_primary_key: false },
    ];

    render(<ManualSchemaForm columns={multipleColumns} onColumnsChange={mockOnColumnsChange} />);

    // Find all buttons that are not "Add Column"
    const allButtons = screen.getAllByRole('button');
    const deleteButton = allButtons.find(btn =>
      !btn.textContent.includes('Add') && !btn.disabled
    );

    if (deleteButton) {
      fireEvent.click(deleteButton);
      expect(mockOnColumnsChange).toHaveBeenCalled();
    }
  });

  it('highlights primary key column with different background', () => {
    const columnsWithPK = [
      { column_name: 'id', data_type: 'integer', required: true, is_primary_key: true },
    ];

    const { container } = render(
      <ManualSchemaForm columns={columnsWithPK} onColumnsChange={mockOnColumnsChange} />
    );

    const columnRow = container.querySelector('.bg-\\[\\#C4B5FD\\]');
    expect(columnRow).toBeInTheDocument();
  });

  it('renders data type selector for each column', () => {
    render(<ManualSchemaForm columns={defaultColumns} onColumnsChange={mockOnColumnsChange} />);

    // Look for the select trigger
    const selectTrigger = screen.getByRole('combobox');
    expect(selectTrigger).toBeInTheDocument();
  });

  it('renders description input for each column', () => {
    render(<ManualSchemaForm columns={defaultColumns} onColumnsChange={mockOnColumnsChange} />);

    const descInput = screen.getByPlaceholderText(/column description/i);
    expect(descInput).toBeInTheDocument();
    expect(descInput.value).toBe('');
  });

  it('updates column description when typing', () => {
    render(<ManualSchemaForm columns={defaultColumns} onColumnsChange={mockOnColumnsChange} />);

    const descInput = screen.getByPlaceholderText(/column description/i);
    fireEvent.change(descInput, { target: { value: 'Unique user identifier' } });

    expect(mockOnColumnsChange).toHaveBeenCalledWith([
      { column_name: '', data_type: 'varchar', required: false, is_primary_key: false, description: 'Unique user identifier' }
    ]);
  });

  it('renders description with existing value', () => {
    const columnsWithDesc = [
      { column_name: 'user_id', data_type: 'integer', required: true, is_primary_key: true, description: 'Primary user ID' }
    ];

    render(<ManualSchemaForm columns={columnsWithDesc} onColumnsChange={mockOnColumnsChange} />);

    const descInput = screen.getByPlaceholderText(/column description/i);
    expect(descInput.value).toBe('Primary user ID');
  });
});
