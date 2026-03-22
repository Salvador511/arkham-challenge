'use client'
import { Typography as T } from '@mui/material'
import { styled } from '@mui/material/styles'
import getClassPrefixer from '~/UI/classPrefixer'
import { noData } from '~/UI/Images'

const displayName = 'NotAvailable'
const classes = getClassPrefixer(displayName) as any
const NotAvailableContainer = styled('div')(() => ({
  display: 'flex',
  height: '100%',
  justifyContent: 'center',
  alignItems: 'center',
  flexDirection: 'column',
  overflow: 'hidden',
  [`& .${classes.scrollArea}`]: {
    width: '100%',
    height: '100%',
    display: 'flex',
    flexDirection: 'column',
    overflow: 'auto',
  },
  [`& .${classes.itemsContainer}`]: {
    display: 'flex',
    justifyContent: 'center',
    alignItems: 'center',
    flexDirection: 'row',
    height: '100%',
    gap: '2rem',
  },
  [`& .${classes.textContainer}`]: {
    display: 'flex',
    flexDirection: 'column',
    justifyContent: 'center',
    height: '100%',
    gap: '1rem',
    width: '100%',
    '@media (max-width: 768px)': {
      width: '100%',
    },
  },
  [`& .${classes.subtitle}`]: {
    marginTop: '1rem',
  },
  [`& .${classes.imageContainer}`]: {
    '@media (max-width: 768px)': {
      display: 'none',
    },
  },
}))

const NotAvailable = () => {
  return (
    <NotAvailableContainer>
      <div className={classes.scrollArea}>
        <div className={classes.itemsContainer}>
          <div className={classes.textContainer}>
            <T variant="h4" color="primary.main">
              Algo salió mal :(
            </T>
            <T variant="body1" className={classes.subtitle}>
              No pudimos cargar la información en este momento. Por favor, intenta recargar la página o contacta al
              equipo de soporte si el problema persiste.
            </T>
          </div>
          <div className={classes.imageContainer}>
            <img src={noData} alt="No Data" style={{ height: 400, width: 550 }} />
          </div>
        </div>
      </div>
    </NotAvailableContainer>
  )
}

export default NotAvailable
