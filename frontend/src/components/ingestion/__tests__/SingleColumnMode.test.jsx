import { describe, it, expect } from 'vitest';
import { render, screen } from '@testing-library/react';
import SingleColumnMode from '../SingleColumnMode';

describe('SingleColumnMode', () => {
  it('renders the Single Column (JSON) title', () => {
    render(<SingleColumnMode />);
    expect(screen.getByText('Single Column (JSON)')).toBeInTheDocument();
  });

  it('renders description about data storage', () => {
    render(<SingleColumnMode />);
    expect(
      screen.getByText(/All data stored in a single/i)
    ).toBeInTheDocument();
  });

  it('renders the data column code example', () => {
    render(<SingleColumnMode />);
    expect(screen.getByText('data')).toBeInTheDocument();
  });

  it('renders query example with json_extract', () => {
    render(<SingleColumnMode />);
    expect(
      screen.getByText(/json_extract\(data, '\$\.field'\)/i)
    ).toBeInTheDocument();
  });

  it('renders with peach background color', () => {
    const { container } = render(<SingleColumnMode />);
    const card = container.querySelector('.bg-\\[\\#FECACA\\]');
    expect(card).toBeInTheDocument();
  });

  it('renders info section with helpful tip', () => {
    render(<SingleColumnMode />);
    expect(screen.getByText(/Query fields with/i)).toBeInTheDocument();
  });
});
