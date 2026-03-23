import DesktopSidebar from '~/UI/Navbars/DesktopSidebar'
import Home from '~/Home/Home'
import OutagesTable from '~/OutagesTable/OutagesTable'
import Graphs from '~/Graphs/Graphs'
import { Alert, CircularProgress, Snackbar, Typography as T } from '@mui/material'
import { styled } from '@mui/material/styles'
import getClassPrefixer from '~/UI/classPrefixer'
import { useState } from 'react'
import MobileNavbar from '~/UI/Navbars/MobileNavbar'
import type { DashboardCategory, SnackbarMessage } from '~/types/ui'

const displayName = 'DashboardPage'
const classes = getClassPrefixer(displayName) as any
const Container = styled('div')(() => ({
  display: 'flex',
  flexDirection: 'row',
  width: '100%',
  justifyContent: 'center',
  alignItems: 'flex-start',
  padding: '6rem',
  '@media (max-width: 1024px)': {
    padding: '4rem',
  },
  '@media (max-width: 768px)': {
    padding: '2rem',
  },
  [`& .${classes.sidebar}`]: {
    display: 'flex',
    flexDirection: 'column',
    alignItems: 'center',
    justifyContent: 'flex-start',
    position: 'sticky',
    top: '6rem',
    '@media (max-width: 1024px)': {
      top: '4rem',
    },
    '@media (max-width: 768px)': {
      display: 'none',
    },
  },
  [`& .${classes.mobileNav}`]: {
    display: 'none',
    '@media (max-width: 768px)': {
      display: 'block',
      position: 'fixed',
      top: 0,
      left: 0,
      right: 0,
      zIndex: 1000,
    },
  },
  [`& .${classes.content}`]: {
    width: '100%',
    height: '100%',
    display: 'flex',
    flexDirection: 'column',
    marginLeft: '6rem ',
    '@media (max-width: 1024px)': {
      marginLeft: '4rem',
    },
    '@media (max-width: 768px)': {
      marginLeft: 0,
      marginTop: '4rem',
    },
  },
  [`& .${classes.buttonContainer}`]: {
    display: 'flex',
    justifyContent: 'center',
  },
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
  [`& .${classes.containerTools}`]: {
    display: 'flex',
    alignItems: 'flex-end',
    flexDirection: 'row',
    width: '100%',
    marginBottom: '1ch',
    justifyContent: 'space-between',
  },
  [`& .${classes.tableContainer}`]: {
    width: '100%',
  },
  [`& .${classes.refreshOverlay}`]: {
    position: 'fixed',
    inset: 0,
    zIndex: 1400,
    backgroundColor: 'rgba(0, 0, 0, 0.60)',
    display: 'flex',
    flexDirection: 'column',
    alignItems: 'center',
    justifyContent: 'center',
    gap: '1rem',
    pointerEvents: 'all',
  },
  [`& .${classes.refreshContent}`]: {
    display: 'flex',
    flexDirection: 'column',
    alignItems: 'center',
    justifyContent: 'center',
    gap: '1rem',
    transform: 'translateY(-12vh)',
  },
  [`& .${classes.refreshText}`]: {
    textAlign: 'center',
  },
}))

interface AppProps {
  selectedCategory: DashboardCategory
  setSelectedCategory: React.Dispatch<React.SetStateAction<DashboardCategory>>
  snackbarMessage: SnackbarMessage | null
  setSnackbarMessage: React.Dispatch<React.SetStateAction<SnackbarMessage | null>>
  isGlobalRefreshing: boolean
  setIsGlobalRefreshing: React.Dispatch<React.SetStateAction<boolean>>
}

const App = ({
  selectedCategory,
  setSelectedCategory,
  snackbarMessage,
  setSnackbarMessage,
  isGlobalRefreshing,
  setIsGlobalRefreshing,
}: AppProps) => {

  return (
    <Container>
      <div className={classes.sidebar}>
        <DesktopSidebar
          selectedCategory={selectedCategory}
          setSelectedCategory={setSelectedCategory}
          isNavigationLocked={isGlobalRefreshing}
        />
      </div>
      <div className={classes.mobileNav}>
        <MobileNavbar
          selectedCategory={selectedCategory}
          setSelectedCategory={setSelectedCategory}
          isNavigationLocked={isGlobalRefreshing}
        />
      </div>
      <div className={classes.content}>
        {selectedCategory === 'Home' && <Home/>}
        {selectedCategory === 'US Outages' && (
          <OutagesTable
            setSnackbarMessage={setSnackbarMessage}
            type='usOutage'
            setIsGlobalRefreshing={setIsGlobalRefreshing}
          />
        )}
        {selectedCategory === 'Facilities Outages' && (
          <OutagesTable
            setSnackbarMessage={setSnackbarMessage}
            type='facilityOutage'
            setIsGlobalRefreshing={setIsGlobalRefreshing}
          />
        )}
        {selectedCategory === 'Graphs' && <Graphs />}
      </div>
      {isGlobalRefreshing && (
        <div className={classes.refreshOverlay}>
          <div className={classes.refreshContent}>
            <CircularProgress size={90} />
            <T variant='h6' color='text.main' className={classes.refreshText}>
              Refreshing data... please wait.
            </T>
          </div>
        </div>
      )}
      <Snackbar
        open={Boolean(snackbarMessage)}
        autoHideDuration={5000}
        onClose={() => setSnackbarMessage(null)}
        anchorOrigin={{ vertical: 'top', horizontal: 'right' }}
      >
        <Alert severity={snackbarMessage?.severity}>{snackbarMessage?.message}</Alert>
      </Snackbar>
    </Container>
  )
}

const Wrapper = () => {
  const [selectedCategory, setSelectedCategory] = useState<DashboardCategory>('Home')
  const [snackbarMessage, setSnackbarMessage] = useState<SnackbarMessage | null>(null)
  const [isGlobalRefreshing, setIsGlobalRefreshing] = useState(false)

  return (
    <App
      selectedCategory={selectedCategory}
      setSelectedCategory={setSelectedCategory}
      snackbarMessage={snackbarMessage}
      setSnackbarMessage={setSnackbarMessage}
      isGlobalRefreshing={isGlobalRefreshing}
      setIsGlobalRefreshing={setIsGlobalRefreshing}
    />

  )
}

export default Wrapper
