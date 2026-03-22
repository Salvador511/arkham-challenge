import RefreshIcon from '@mui/icons-material/Refresh'
import { Autocomplete, Button, TextField, Typography as T } from '@mui/material'
import { styled } from '@mui/material/styles'
import { useEffect, useRef, useState } from 'react'
import getClassPrefixer from '~/UI/classPrefixer'
import NotAvailable from '~/NotAvailable/NotAvailable'
import Loading from '~/UI/Shared/Loading'
import { useApiInfiniteQuery, useApiQuery, useApiMutation } from '~/Libs/apiFetch'

import type { SnackbarMessage } from '~/types/ui'
import type { USOutages, FacilityOutages } from '~/types/outages'

const displayName = 'OutagesTable'
const classes = getClassPrefixer(displayName) as any

const HEADERS_CONFIG = {
  usOutage: {
    headers: ['Date', 'Capacity', 'Outage', 'Outage %'],
    columns: '1fr 1fr 1fr 1fr',
    fields: ['date', 'capacity', 'outage', 'percent_outage'],
  },
  facilityOutage: {
    headers: ['Date', 'Facility', 'Capacity', 'Outage', 'Outage %'],
    columns: '1.5fr 2fr 1fr 1fr 1fr',
    fields: ['date', 'facility_name', 'capacity', 'outage', 'percent_outage'],
  },
}

const Container = styled('div')<{ gridColumns?: string }>(({ theme, gridColumns }: any) => ({
  width: '100%',
  height: '100%',
  display: 'flex',
  flexDirection: 'column',
  [`& .${classes.gridHeader}, & .${classes.gridItems}`]: {
    display: 'grid',
    gridTemplateColumns: gridColumns || '2fr 1fr 1fr 1fr',
  },
  [`& .${classes.stickyHeader}`]: {
    position: 'sticky',
    top: 0,
    zIndex: 10,
    backgroundColor: theme.palette.background.main,
    paddingTop: '2rem',
  },
  [`& .${classes.gridHeader}`]: {
    marginBottom: '0.5rem',
  },
  [`& .${classes.item}`]: {
    paddingLeft: '1ch',
    paddingRight: '1ch',
    display: 'flex',
    alignItems: 'center',
    justifyContent: 'flex-start',
    whiteSpace: 'nowrap',
    textOverflow: 'ellipsis',
    overflow: 'hidden',
  },
  [`& .${classes.gridItems}`]: {
    width: '100%',
    marginTop: '0.5ch',
    padding: '0.5ch',
    background: theme.palette.primary.enabled,
    textTransform: 'none',
    '&:hover': {
      background: theme.palette.primary.main,
      '& .MuiTypography-root': {
        color: theme.palette.common.white,
      },
    },
  },
  [`& .${classes.toolsContainer}`]: {
    width: '100%',
    display: 'flex',
    flexDirection: 'row',
    justifyContent: 'space-between',
    alignItems: 'flex-start',
    paddingBottom: '1rem',
  },
  [`& .${classes.buttonsContainer}`]: {
    display: 'flex',
    flexDirection: 'row',
    gap: '1rem',
  },
  [`& .${classes.filtersContainer}`]: {
    display: 'flex',
    flexDirection: 'row',
    gap: '1rem',
    alignItems: 'flex-end',
    '@media (max-width: 768px)': {
      flexDirection: 'column',
      width: '100%',
    },
  },
  [`& .${classes.filterField}`]: {
    minWidth: '180px',
  },
  [`& .${classes.filterField} .MuiInputBase-root`]: {
    backgroundColor: 'transparent',
  },
  [`& .${classes.filterField} .MuiInputLabel-root`]: {
    color: theme.palette.text.main,
    fontSize: '0.875rem',
  },
  [`& .${classes.filterField} .MuiInputLabel-shrink`]: {
    transform: 'translate(14px, -9px) scale(0.75)',
  },
  [`& .${classes.filterField} .MuiOutlinedInput-root`]: {
    color: theme.palette.text.main,
    '& fieldset': {
      borderColor: theme.palette.text.main,
    },
    '&:hover fieldset': {
      borderColor: theme.palette.text.main,
    },
    '&.Mui-focused fieldset': {
      borderColor: theme.palette.text.main,
      borderWidth: 2,
    },
  },
  [`& .${classes.filterField} .MuiOutlinedInput-input`]: {
    padding: '8px 12px',
    fontSize: '0.875rem',
    '&::placeholder': {
      color: theme.palette.text.main,
      opacity: 0.5,
    },
  },
  [`& .${classes.filterField} input[type="date"]::-webkit-calendar-picker-indicator`]: {
    filter: 'invert(1)',
  },
  [`& .${classes.toolButton}`]: {
    padding: '0.5ch 1ch',
    color: theme.palette.text.main,
  },
  [`& .${classes.tableWrapper}`]: {
    width: '100%',
    overflowY: 'auto',
    WebkitOverflowScrolling: 'touch',
    '@media (max-width: 768px)': {
      overflowX: 'scroll',
    },
  },
  [`& .${classes.tableContent}`]: {
    minWidth: '100%',
    '@media (max-width: 768px)': {
      minWidth: '700px',
    },
  },
  [`& .${classes.observerTarget}`]: {
    gridColumn: '1 / -1',
    height: '5rem',
    marginTop: '2rem',
    padding: '2rem',
    display: 'flex',
    alignItems: 'center',
    justifyContent: 'center',
    gap: '1rem',
    flexDirection: 'column',
  },
}))

type Facility = {
  facility_id: string
  facility_name: string
}

type OutagesTableProps = {
  usOutageData: USOutages[] | FacilityOutages[]
  type: 'usOutage' | 'facilityOutage'
  observerTarget: React.RefObject<HTMLDivElement | null>
  isFetchingNextPage: boolean
  dateFrom: string
  dateTo: string
  facilityId: string
  facilities: Facility[]
  onDateFromChange: (value: string) => void
  onDateToChange: (value: string) => void
  onFacilityIdChange: (value: string) => void
  onRefresh: () => void
  isRefreshing: boolean
}

const OutagesTable = ({
  usOutageData,
  type,
  observerTarget,
  isFetchingNextPage,
  dateFrom,
  dateTo,
  facilityId,
  facilities,
  onDateFromChange,
  onDateToChange,
  onFacilityIdChange,
  onRefresh,
  isRefreshing,
}: OutagesTableProps) => {
  const config = HEADERS_CONFIG[type]

  const today = new Date().toISOString().split('T')[0]

  const formatCellValue = (key: string, value: any) => {
    if (key === 'date') return value
    if (key === 'capacity') return String(value).toLocaleString() + ' MW'
    if (key === 'outage') return String(value).toLocaleString() + ' MW'
    if (key === 'percent_outage') return value.toFixed(2) + '%'
    return String(value)
  }

  return (
    <Container gridColumns={config.columns}>
      <div className={classes.stickyHeader}>
        <div className={classes.toolsContainer}>
          <div className={classes.filtersContainer}>
            <TextField
              label="From"
              type="date"
              value={dateFrom}
              onChange={e => onDateFromChange(e.target.value)}
              className={classes.filterField}
              InputLabelProps={{ shrink: true }}
              inputProps={{ max: today }}
              variant="outlined"
              size="small"
            />
            <TextField
              label="To"
              type="date"
              value={dateTo}
              onChange={e => onDateToChange(e.target.value)}
              className={classes.filterField}
              InputLabelProps={{ shrink: true }}
              inputProps={{ max: today }}
              variant="outlined"
              size="small"
            />
            {type === 'facilityOutage' && (
              <Autocomplete
                options={facilities}
                getOptionLabel={option => option.facility_name}
                value={facilities.find(f => f.facility_id === facilityId) || null}
                onChange={(_, newValue) => onFacilityIdChange(newValue?.facility_id || '')}
                renderInput={params => (
                  <TextField
                    {...params}
                    label="Facility Name"
                    placeholder="Optional"
                    variant="outlined"
                    size="small"
                    className={classes.filterField}
                  />
                )}
                disablePortal
              />
            )}
          </div>
          <div className={classes.buttonsContainer}>
            <Button
              variant="text"
              className={classes.toolButton}
              startIcon={<RefreshIcon />}
              onClick={onRefresh}
              disabled={isRefreshing}
            >
              {isRefreshing ? 'Refreshing...' : 'Refresh'}
            </Button>
          </div>
        </div>
        <div className={classes.gridHeader}>
          {config.headers.map((item, index) => (
            <div className={classes.item} key={index}>
              <T variant="subtitle1" color="text.main" fontWeight="bold">
                {item}
              </T>
            </div>
          ))}
        </div>
      </div>

      <div className={classes.tableWrapper}>
        <div className={classes.tableContent}>
          {usOutageData.map((row, idx) => (
            <div key={idx} className={classes.gridItems} onClick={() => {}}>
              {config.fields.map((field, index) => (
                <div className={classes.item} key={index}>
                  <T variant="subtitle1" color="primary" fontWeight="bold">
                    {formatCellValue(field, row[field as keyof typeof row])}
                  </T>
                </div>
              ))}
            </div>
          ))}

          <div ref={observerTarget} className={classes.observerTarget}>
            {isFetchingNextPage && (
              <Loading />
            )}
          </div>
        </div>
      </div>

    </Container>
  )
}

type WrapperProps = {
  setSnackbarMessage: (msg: SnackbarMessage | null) => void
  setIsGlobalRefreshing: (value: boolean) => void
  type: 'usOutage' | 'facilityOutage'
  date_from?: string
  date_to?: string
  facility_id?: string
}

const Wrapper = ({ setSnackbarMessage, setIsGlobalRefreshing, type }: WrapperProps) => {
  const observerTarget = useRef<HTMLDivElement>(null)
  const dataset = type === 'usOutage' ? 'us' : 'facility'
  const [dateFrom, setDateFrom] = useState('')
  const [dateTo, setDateTo] = useState('')
  const [facilityId, setFacilityId] = useState('')
  const [isRefreshing, setIsRefreshing] = useState(false)

  const apiUrl = import.meta.env.VITE_API_BASE_URL
  const { data: facilitiesData, isLoading: isFacilitiesLoading, isError: isFacilitiesError } = useApiQuery({
    url: `${apiUrl}data?dataset=plants`,
    key: 'facilities',
  })

  const facilities = type === 'facilityOutage' && facilitiesData?.data ? facilitiesData.data : []

  const { data, fetchNextPage, hasNextPage, isFetchingNextPage, isLoading, isError } = useApiInfiniteQuery({
    url: `${apiUrl}data`,
    key: `outages-${type}`,
    dataset: dataset as 'us' | 'facility',
    limit: 100,
    date_from: dateFrom || undefined,
    date_to: dateTo || undefined,
    facility_id: facilityId || undefined,
  })

  const { mutate: refreshData } = useApiMutation({
    url: `${apiUrl}refresh`,
    method: 'POST',
    keys: ['outages-usOutage', 'outages-facilityOutage', 'facilities'],
  })

  useEffect(() => {
    const observer = new IntersectionObserver(
      entries => {
        if (entries[0].isIntersecting && hasNextPage && !isFetchingNextPage) {
          fetchNextPage()
        }
      },
      { threshold: 0.1 }
    )

    if (observerTarget.current) {
      observer.observe(observerTarget.current)
    }

    return () => observer.disconnect()
  }, [fetchNextPage, hasNextPage, isFetchingNextPage])

  const allData = data?.pages.flatMap(page => page.data) ?? []
  const hasContentError = isError || isFacilitiesError || allData.length === 0

  if (isLoading || isFacilitiesLoading) return <Loading />

  const handleRefresh = () => {
    setIsRefreshing(true)
    setIsGlobalRefreshing(true)
    refreshData(undefined, {
      onSuccess: (response: any) => {
        const { extraction_type, status, retry_after_seconds } = response

        if (status === 'processing') {
          setSnackbarMessage({
            message: `${extraction_type === 'full' ? 'Full' : 'Incremental'} data extraction started. This may take a few minutes...`,
            severity: 'info'
          })
          if (extraction_type === 'full' && retry_after_seconds) {
            setTimeout(() => {
              setIsRefreshing(false)
              setIsGlobalRefreshing(false)
            }, retry_after_seconds * 1000)
          } else {
            setIsRefreshing(false)
            setIsGlobalRefreshing(false)
          }
        } else if (status === 'success') {
          setSnackbarMessage({
            message: `${extraction_type === 'incremental' ? 'Incremental' : 'Full'} data extraction completed successfully`,
            severity: 'success'
          })
          setIsRefreshing(false)
          setIsGlobalRefreshing(false)
        }
      },
      onError: (error: any) => {
        console.error('Error refreshing data:', error)
        setSnackbarMessage({
          message: error?.message || 'Error refreshing data',
          severity: 'error'
        })
        setIsRefreshing(false)
        setIsGlobalRefreshing(false)
      }
    })
  }

  if (hasContentError) {
    return (
      <NotAvailable
        onAction={handleRefresh}
        isActionLoading={isRefreshing}
        actionLabel="Refresh"
      />)
  }

  return (
    <OutagesTable
      usOutageData={allData}
      type={type}
      observerTarget={observerTarget}
      isFetchingNextPage={isFetchingNextPage}
      dateFrom={dateFrom}
      dateTo={dateTo}
      facilityId={facilityId}
      facilities={facilities}
      onDateFromChange={setDateFrom}
      onDateToChange={setDateTo}
      onFacilityIdChange={setFacilityId}
      onRefresh={handleRefresh}
      isRefreshing={isRefreshing}
    />
  )
}

export default Wrapper
