import { describe, it, expect, vi } from 'vitest'
import { render, screen, fireEvent } from '@testing-library/react'
import { AuthOverlay } from './AuthOverlay'

describe('AuthOverlay', () => {
  it('submits the entered token', () => {
    const onSubmit = vi.fn()
    render(<AuthOverlay failed={false} onSubmit={onSubmit} />)

    fireEvent.change(screen.getByLabelText('Access token'), { target: { value: 'my-token' } })
    fireEvent.click(screen.getByRole('button', { name: 'Unlock' }))

    expect(onSubmit).toHaveBeenCalledWith('my-token')
  })

  it('does not submit an empty token', () => {
    const onSubmit = vi.fn()
    render(<AuthOverlay failed={false} onSubmit={onSubmit} />)
    fireEvent.click(screen.getByRole('button', { name: 'Unlock' }))
    expect(onSubmit).not.toHaveBeenCalled()
  })

  it('shows an error message when a previous attempt failed', () => {
    render(<AuthOverlay failed={true} onSubmit={vi.fn()} />)
    expect(screen.getByText(/incorrect password/i)).toBeInTheDocument()
  })
})
