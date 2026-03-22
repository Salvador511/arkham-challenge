'use client'
import RefreshIcon from '@mui/icons-material/Refresh'
import { Button, Typography as T } from '@mui/material'
import { styled } from '@mui/material/styles'
import getClassPrefixer from '~/UI/classPrefixer'
import { noData } from '~/UI/Images'

const displayName = 'NotAvailable'
const classes = getClassPrefixer(displayName) as any
const NotAvailableContainer = styled('div')(({ theme }: any) => ({
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
  [`& .${classes.actionContainer}`]: {
    marginTop: '1rem',
    display: 'flex',
    alignItems: 'center',
    justifyContent: 'flex-start',
  },
  [`& .${classes.actionButton}`]: {
    color: theme.palette.text.main,
    paddingInline: '1rem',
    '& .MuiSvgIcon-root': {
      color: theme.palette.text.main,
    },
  },
  [`& .${classes.imageContainer}`]: {
    '@media (max-width: 768px)': {
      display: 'none',
    },
  },
}))

type NotAvailableProps = {
  onAction?: () => void
  actionLabel?: string
  isActionLoading?: boolean
}

const NotAvailable = ({ onAction = undefined, actionLabel = 'Refresh', isActionLoading = false }: NotAvailableProps) => {
  return (
    <NotAvailableContainer>
      <div className={classes.scrollArea}>
        <div className={classes.itemsContainer}>
          <div className={classes.textContainer}>
            <T variant="h4" color="primary.main">
              Algo salió mal :(
            </T>
            <T variant="body1" className={classes.subtitle} color="text.main">
              No pudimos cargar la información en este momento. Por favor, intenta recargar la página o contacta al
              equipo de soporte si el problema persiste.
            </T>
            {onAction && (
              <div className={classes.actionContainer}>
                <Button
                  variant="text"
                  startIcon={<RefreshIcon />}
                  onClick={onAction}
                  disabled={isActionLoading}
                  className={classes.actionButton}
                >
                  {isActionLoading ? 'Refreshing...' : actionLabel}
                </Button>
              </div>
            )}
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
