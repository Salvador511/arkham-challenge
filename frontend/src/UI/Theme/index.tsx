'use client'
import { CssBaseline, ThemeProvider } from '@mui/material'
import themeConfig from '~/UI/Theme/createTheme'

const ThemeRegistry = ({ children }: { children: React.ReactNode }) => (
  <ThemeProvider theme={themeConfig}>
    <CssBaseline />
    {children}
  </ThemeProvider>
)

export default ThemeRegistry