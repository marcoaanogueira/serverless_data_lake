import { describe, it, expect, vi } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import SchemaModeTabs from '../SchemaModeTabs';

describe('SchemaModeTabs', () => {
  const mockOnModeChange = vi.fn();

  beforeEach(() => {
    mockOnModeChange.mockClear();
  });

  it('renders all three schema mode options', () => {
    render(<SchemaModeTabs activeMode="manual" onModeChange={mockOnModeChange} />);

    expect(screen.getByText('Manual')).toBeInTheDocument();
    expect(screen.getByText('Auto')).toBeInTheDocument();
    expect(screen.getByText('JSON')).toBeInTheDocument();
  });

  it('renders descriptions for each mode', () => {
    render(<SchemaModeTabs activeMode="manual" onModeChange={mockOnModeChange} />);

    expect(screen.getByText('Define columns')).toBeInTheDocument();
    expect(screen.getByText('Infer schema')).toBeInTheDocument();
    expect(screen.getByText('Raw storage')).toBeInTheDocument();
  });

  it('highlights the active mode (manual)', () => {
    render(<SchemaModeTabs activeMode="manual" onModeChange={mockOnModeChange} />);

    const manualButton = screen.getByText('Manual').closest('button');
    expect(manualButton).toHaveClass('bg-[#A8E6CF]');
  });

  it('highlights the active mode (auto_inference)', () => {
    render(<SchemaModeTabs activeMode="auto_inference" onModeChange={mockOnModeChange} />);

    const autoButton = screen.getByText('Auto').closest('button');
    expect(autoButton).toHaveClass('bg-[#C4B5FD]');
  });

  it('highlights the active mode (single_column)', () => {
    render(<SchemaModeTabs activeMode="single_column" onModeChange={mockOnModeChange} />);

    const jsonButton = screen.getByText('JSON').closest('button');
    expect(jsonButton).toHaveClass('bg-[#FECACA]');
  });

  it('calls onModeChange when clicking Manual tab', () => {
    render(<SchemaModeTabs activeMode="auto_inference" onModeChange={mockOnModeChange} />);

    fireEvent.click(screen.getByText('Manual'));
    expect(mockOnModeChange).toHaveBeenCalledWith('manual');
  });

  it('calls onModeChange when clicking Auto tab', () => {
    render(<SchemaModeTabs activeMode="manual" onModeChange={mockOnModeChange} />);

    fireEvent.click(screen.getByText('Auto'));
    expect(mockOnModeChange).toHaveBeenCalledWith('auto_inference');
  });

  it('calls onModeChange when clicking JSON tab', () => {
    render(<SchemaModeTabs activeMode="manual" onModeChange={mockOnModeChange} />);

    fireEvent.click(screen.getByText('JSON'));
    expect(mockOnModeChange).toHaveBeenCalledWith('single_column');
  });

  it('renders as a 3-column grid', () => {
    const { container } = render(
      <SchemaModeTabs activeMode="manual" onModeChange={mockOnModeChange} />
    );

    const grid = container.querySelector('.grid-cols-3');
    expect(grid).toBeInTheDocument();
  });
});
