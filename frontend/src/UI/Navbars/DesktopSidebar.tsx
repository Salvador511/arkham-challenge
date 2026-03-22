'use client'
import { Button, Stack } from '@mui/material'
import { styled } from '@mui/material/styles'
import getClassPrefixer from '~/UI/classPrefixer'
import { SIDEBAR_OPTS } from '~/UI/Navbars/utils'
import { logo } from '~/UI/Images'
import type { DashboardCategory } from '~/types/ui'

const displayName = 'DesktopSidebar'
const classes = getClassPrefixer(displayName) as any

const SidebarContainer = styled('div')(() => ({
  display: 'flex',
  flexDirection: 'column',
  alignItems: 'center',
  justifyContent: 'flex-start',
  [`& .${classes.button}`]: {
    padding: '1ch',
    width: '12rem',
    '@media (max-width: 1024px)': {
      width: '8rem',
    },
    '@media (max-width: 768px)': {
      width: '6rem',
    },
  },
}))

interface DesktopSidebarProps {
  selectedCategory: DashboardCategory
  setSelectedCategory: (newValue: DashboardCategory) => void
  isNavigationLocked?: boolean
}

const DesktopSidebar = ({ selectedCategory, setSelectedCategory, isNavigationLocked = false }: DesktopSidebarProps) => {
  const handleOptionClick = (title: DashboardCategory) => {
    if (isNavigationLocked) return
    setSelectedCategory(title)
  }

  return (
    <SidebarContainer>
      <img src={logo} alt="Logo" width={150} height={150}/>
      <Stack spacing={1} marginTop={4}>
        {SIDEBAR_OPTS.map((option, index) => {
          const { Icon, title } = option

          return (
            <Button
              key={`${title}-${index}`}
              variant={selectedCategory === title ? 'contained' : 'outlined'}
              startIcon={Icon ? <Icon /> : null}
              onClick={() => handleOptionClick(title)}
              disabled={isNavigationLocked}
              className={classes.button}
              sx={selectedCategory === title ? { color: 'white' } : {}}
            >
              {title}
            </Button>
          )
        })}
      </Stack>
    </SidebarContainer>
  )
}

export default DesktopSidebar
