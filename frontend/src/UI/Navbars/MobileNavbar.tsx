'use client'
import { Button, IconButton, Stack } from '@mui/material'
import { styled } from '@mui/material/styles'
import classNames from 'clsx'
import { useState } from 'react'
import { AiOutlineClose } from 'react-icons/ai'
import { FaBars } from 'react-icons/fa'
import getClassPrefixer from '~/UI/classPrefixer'
import { logo } from '~/UI/Images'
import { SIDEBAR_OPTS } from './utils'
import type { DashboardCategory } from '~/types/ui'

const displayName = 'MobileNav'
const classes = getClassPrefixer(displayName) as any

const NavbarContainer = styled('div')(({ theme }: any) => ({
  [`& .${classes.navbar}`]: {
    height: '100%',
    display: 'flex',
    flexDirection: 'row',
    justifyContent: 'space-between',
    alignItems: 'center',
    padding: '2rem',
  },
  [`& .${classes.barsIcon}`]: {
    margin: '1ch',
    fontSize: '1rem',
    color: theme.palette.primary.main,
  },
  [`& .${classes.menu}`]: {
    background: theme.palette.background.main,
    width: '100vw',
    height: '100vh',
    display: 'flex',
    padding: '2rem',
    flexDirection: 'column',
    justifyContent: 'flex-start',
    position: 'fixed',
    zIndex: 2,
    top: 0,
    right: '-100%',
    transition: '850ms',
    boxShadow: '-4px -1px 11px 0px rgba(0,0,0,0.5)',
  },
  [`& .${classes.menuActive}`]: {
    right: 0,
    transition: '350ms',
  },
  [`& .${classes.menuItems}`]: {
    padding: '1rem',
    width: '100%',
  },

  [`& .${classes.button}`]: {
    padding: '1ch',
    marginTop: '1rem',
    display: 'flex',
    justifyContent: 'flex-start',
    textDecoration: 'none',
    width: '100%',
  },
}))

type MobileNavbarProps = {
  selectedCategory: DashboardCategory
  setSelectedCategory: (newValue: DashboardCategory) => void
  isNavigationLocked?: boolean
}

const MobileNavbar = ({ selectedCategory, setSelectedCategory, isNavigationLocked = false }: MobileNavbarProps) => {
  const [sidebarOpen, setSidebarOpen] = useState(false)

  const handleOptionClick = (title: DashboardCategory) => {
    if (isNavigationLocked) return
    setSelectedCategory(title)
    setSidebarOpen(false)
  }

  return (
    <NavbarContainer className={classes.navbar}>
      <div className={classes.navbar}>
        <img src={logo} alt="logo" width={40} height={40} />
        <IconButton onClick={() => setSidebarOpen(true)}>
          <FaBars className={classes.barsIcon} />
        </IconButton>
      </div>
      <nav
        className={classNames({
          [classes.menu]: true,
          [classes.menuActive]: sidebarOpen,
        })}
      >
        <div className={classes.menuItems}>
          <Stack direction="row" justifyContent="flex-end">
            <IconButton onClick={() => setSidebarOpen(false)}>
              <AiOutlineClose className={classes.navIcon} />
            </IconButton>
          </Stack>
          <Stack spacing={2}>
            {SIDEBAR_OPTS.map((option, index) => {
              const { Icon, title } = option
              return (
                <Button
                  key={`${title}-${index}`}
                  className={classes.button}
                  variant={selectedCategory === title ? 'contained' : 'text'}
                  startIcon={Icon ? <Icon /> : null}
                  onClick={() => handleOptionClick(title)}
                  disabled={isNavigationLocked}
                  fullWidth
                >
                  {title}
                </Button>
              )
            })}
          </Stack>
        </div>
      </nav>
    </NavbarContainer>
  )
}

export default MobileNavbar
