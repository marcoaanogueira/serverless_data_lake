import { describe, it, expect } from 'vitest';
import { render, screen } from '@testing-library/react';
import AutoInferenceDisplay from '../AutoInferenceDisplay';

describe('AutoInferenceDisplay', () => {
  it('renders the Auto Inference title', () => {
    render(<AutoInferenceDisplay />);
    expect(screen.getByText('Auto Inference')).toBeInTheDocument();
  });

  it('renders the description text', () => {
    render(<AutoInferenceDisplay />);
    expect(screen.getByText('Magic schema detection')).toBeInTheDocument();
  });

  it('renders explanatory text about schema inference', () => {
    render(<AutoInferenceDisplay />);
    expect(
      screen.getByText(/Schema will be automatically inferred from your first data payload/i)
    ).toBeInTheDocument();
  });

  it('renders with lilac background color', () => {
    const { container } = render(<AutoInferenceDisplay />);
    const card = container.querySelector('.bg-\\[\\#C4B5FD\\]');
    expect(card).toBeInTheDocument();
  });

  it('renders the magic wand illustration', () => {
    render(<AutoInferenceDisplay />);
    const img = screen.getByAltText('Magic Wand');
    expect(img).toBeInTheDocument();
    expect(img).toHaveAttribute('src', '/illustrations/magic-wand.png');
  });
});
