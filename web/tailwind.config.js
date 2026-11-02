/** @type {import('tailwindcss').Config} */
export default {
  content: ['./index.html', './src/**/*.{vue,js}'],
  theme: {
    extend: {
      colors: {
        void: '#08090d',
        abyss: '#0b0e14',
        panel: '#0e121b',
        raised: '#131927',
        edge: '#1d2637',
        edgeBright: '#2b3a52',
        ink: '#e6edf7',
        dim: '#8b9bb4',
        faint: '#5b6b84',
        cyan: {
          DEFAULT: '#22d3ee',
          soft: '#67e8f9',
          deep: '#0891b2',
        },
        violet: {
          DEFAULT: '#a78bfa',
          soft: '#c4b5fd',
          deep: '#7c3aed',
        },
        signal: '#34d399',
        warning: '#fbbf24',
        danger: '#fb7185',
      },
      fontFamily: {
        sans: ['Inter', 'system-ui', 'sans-serif'],
        mono: ['"JetBrains Mono"', 'ui-monospace', 'SFMono-Regular', 'monospace'],
      },
      boxShadow: {
        'glow-cyan': '0 0 24px -6px rgba(34, 211, 238, 0.45)',
        'glow-violet': '0 0 24px -6px rgba(167, 139, 250, 0.45)',
        panel: '0 18px 48px -24px rgba(0, 0, 0, 0.9)',
      },
      letterSpacing: {
        hud: '0.22em',
      },
      animation: {
        pulseSoft: 'pulseSoft 3.2s ease-in-out infinite',
        scan: 'scan 7s linear infinite',
      },
      keyframes: {
        pulseSoft: {
          '0%, 100%': { opacity: '0.45' },
          '50%': { opacity: '1' },
        },
        scan: {
          '0%': { transform: 'translateY(-100%)' },
          '100%': { transform: 'translateY(100vh)' },
        },
      },
    },
  },
  plugins: [],
}
