import {
  Autocomplete,
  Checkbox,
  FormControlLabel,
  TextField,
  Typography as T,
} from '@mui/material'
import { styled, useTheme } from '@mui/material/styles'
import { useMemo, useState } from 'react'
import { useQueries } from '@tanstack/react-query'
import { LineChart } from '@mui/x-charts/LineChart'
import getClassPrefixer from '~/UI/classPrefixer'
import Loading from '~/UI/Shared/Loading'
import NotAvailable from '~/NotAvailable/NotAvailable'
import { apiFetch, useApiQuery } from '~/Libs/apiFetch'
import type { FacilityOutages, USOutages } from '~/types/outages'

const displayName = 'Graphs'
const classes = getClassPrefixer(displayName) as any

const MAX_FACILITY_SERIES = 5
const DEFAULT_LIMIT = 250
const MAX_API_LIMIT = 1000
const MAX_DATE_RANGE_DAYS = 60
const GraphsContainer = styled('div')(({ theme }: any) => ({
  width: '100%',
  height: '100%',
  display: 'flex',
  flexDirection: 'column',
  gap: '1rem',
  color: theme.palette.text.main,
  [`& .${classes.header}`]: {
    display: 'flex',
    flexDirection: 'column',
    gap: '0.5rem',
  },
  [`& .${classes.filters}`]: {
    width: '100%',
    display: 'flex',
    flexDirection: 'column',
    gap: '1rem',
  },
  [`& .${classes.dateRangeRow}`]: {
    width: '100%',
    display: 'grid',
    gridTemplateColumns: 'repeat(2, minmax(180px, 1fr))',
    gap: '1rem',
    alignItems: 'end',
    '@media (max-width: 768px)': {
      gridTemplateColumns: '1fr',
    },
  },
  [`& .${classes.facilitiesRow}`]: {
    width: '100%',
    display: 'flex',
    flexDirection: 'column',
    gap: '0.35rem',
  },
  [`& .${classes.usToggleRow}`]: {
    width: '100%',
    display: 'flex',
    flexDirection: 'column',
    gap: '0.2rem',
  },
  [`& .${classes.usToggleLabel}`]: {
    marginLeft: 0,
    marginRight: 0,
    width: 'fit-content',
    alignSelf: 'flex-start',
    justifyContent: 'flex-start',
    '& .MuiTypography-root': {
      fontWeight: 600,
      color: theme.palette.text.main,
    },
    '& .MuiCheckbox-root': {
      color: theme.palette.text.main,
    },
    '& .MuiCheckbox-root.Mui-checked': {
      color: theme.palette.primary.main,
    },
  },
  [`& .${classes.inputField}`]: {
    '& .MuiOutlinedInput-root': {
      color: theme.palette.text.main,
      '& fieldset': {
        borderColor: theme.palette.text.main,
      },
      '&:hover fieldset': {
        borderColor: theme.palette.text.main,
      },
      '&.Mui-focused fieldset': {
        borderColor: theme.palette.text.main,
      },
    },
    '& .MuiAutocomplete-endAdornment .MuiSvgIcon-root': {
      color: theme.palette.text.main,
    },
    '& .MuiAutocomplete-tag': {
      backgroundColor: theme.palette.primary.main,
      color: theme.palette.text.main,
    },
    '& .MuiAutocomplete-tag .MuiChip-deleteIcon': {
      color: theme.palette.text.muted,
    },
    '& .MuiAutocomplete-tag .MuiChip-deleteIcon:hover': {
      color: theme.palette.text.main,
    },
    '& .MuiInputLabel-root': {
      color: theme.palette.text.main,
    },
    '& input[type="date"]::-webkit-calendar-picker-indicator': {
      filter: 'invert(1)',
    },
  },
  [`& .${classes.chartWrapper}`]: {
    width: '100%',
    padding: '1rem',
    border: `1px solid ${theme.palette.text.main}`,
    borderRadius: '8px',
    backgroundColor: theme.palette.text.enabled,
  },
  [`& .${classes.helperText}`]: {
    color: theme.palette.text.muted,
  },
}))

type Facility = {
  facility_id: string
  facility_name: string
}

type ChartRow = {
  date: string
  us: number | null
  [facilityId: string]: number | string | null
}

type GraphsProps = {
  includeUs: boolean
  dateFrom: string
  dateTo: string
  effectiveLimit: number
  facilities: Facility[]
  selectedFacilityIds: string[]
  chartRows: ChartRow[]
  onIncludeUsChange: (value: boolean) => void
  onDateFromChange: (value: string) => void
  onDateToChange: (value: string) => void
  onFacilitiesChange: (value: string[]) => void
}

const Graphs = ({
  includeUs,
  dateFrom,
  dateTo,
  effectiveLimit,
  facilities,
  selectedFacilityIds,
  chartRows,
  onIncludeUsChange,
  onDateFromChange,
  onDateToChange,
  onFacilitiesChange,
}: GraphsProps) => {
  const theme = useTheme() as any
  const today = new Date()
  const todayString = formatDateInput(today)
  const fromDate = parseDateInput(dateFrom)
  const toDate = parseDateInput(dateTo)
  const minFromDate = toDate ? addDays(toDate, -(MAX_DATE_RANGE_DAYS - 1)) : undefined
  const maxToDateByRange = fromDate ? addDays(fromDate, MAX_DATE_RANGE_DAYS - 1) : undefined
  const toMaxDate = maxToDateByRange ? minDate(today, maxToDateByRange) : today
  const xAxisData = chartRows.map(row => row.date)

  const selectedFacilities = facilities.filter(facility => selectedFacilityIds.includes(facility.facility_id))

  const seriesColors = [
    theme.palette.red.main,
    theme.palette.darkRed.main,
    theme.palette.gray.main,
    theme.palette.primary.strong,
    theme.palette.primary.muted,
  ]

  const usSeries: {
    id: string
    label: string
    data: Array<number | null>
    connectNulls: boolean
    color: string
  }[] = []

  if (includeUs) {
    usSeries.push({
      id: 'us-series',
      label: 'US Outage',
      data: chartRows.map(row => row.us as number | null),
      connectNulls: true,
      color: theme.palette.primary.main,
    })
  }

  const chartSeries = [
    ...usSeries,
    ...selectedFacilities.map((facility, index) => ({
      id: `facility-${facility.facility_id}`,
      label: facility.facility_name,
      data: chartRows.map(row => row[facility.facility_id] as number | null),
      connectNulls: true,
      color: seriesColors[index % seriesColors.length],
    })),
  ]

  const hasSeriesToRender = chartSeries.length > 0

  return (
    <GraphsContainer>
      <div className={classes.header}>
        <T variant="h4" color="primary.main" fontWeight="bold">
          Outages Comparison Graph
        </T>
        <T color="text.main" variant="body2">
          Compare US outage against multiple facilities. You can select up to {MAX_FACILITY_SERIES} facilities.
        </T>
      </div>

      <div className={classes.filters}>
        <div className={classes.dateRangeRow}>
          <TextField
            label="From"
            type="date"
            value={dateFrom}
            onChange={event => onDateFromChange(event.target.value)}
            className={classes.inputField}
            InputLabelProps={{ shrink: true }}
            inputProps={{
              max: toDate ? formatDateInput(toDate) : todayString,
              min: minFromDate ? formatDateInput(minFromDate) : undefined,
            }}
            variant="outlined"
            size="small"
          />

          <TextField
            label="To"
            type="date"
            value={dateTo}
            onChange={event => onDateToChange(event.target.value)}
            className={classes.inputField}
            InputLabelProps={{ shrink: true }}
            inputProps={{
              max: formatDateInput(toMaxDate),
              min: fromDate ? formatDateInput(fromDate) : undefined,
            }}
            variant="outlined"
            size="small"
          />
        </div>

        <div className={classes.facilitiesRow}>
          <Autocomplete
            multiple
            disableCloseOnSelect
            options={facilities}
            getOptionLabel={option => option.facility_name}
            value={selectedFacilities}
            onChange={(_, newValue) => {
              const nextIds = newValue.slice(0, MAX_FACILITY_SERIES).map(item => item.facility_id)
              onFacilitiesChange(nextIds)
            }}
            renderInput={params => (
              <TextField
                {...params}
                label="Facilities to compare"
                placeholder="Select facilities"
                variant="outlined"
                size="small"
                className={classes.inputField}
              />
            )}
          />
        </div>

        <div className={classes.usToggleRow}>
          <FormControlLabel
            labelPlacement="start"
            control={
              <Checkbox
                checked={includeUs}
                onChange={event => onIncludeUsChange(event.target.checked)}
              />
            }
            label="Include US national outage line"
            className={classes.usToggleLabel}
          />
          <T variant="body2" className={classes.helperText}>
            Maximum date range: {MAX_DATE_RANGE_DAYS} days. Rows per series are auto-calculated from range: {effectiveLimit}.
          </T>
        </div>
      </div>


      <div className={classes.chartWrapper}>
        {hasSeriesToRender ? (
          <LineChart
            height={400}
            xAxis={[
              {
                scaleType: 'point',
                data: xAxisData,
                tickLabelStyle: { fill: theme.palette.common.white, fontSize: 10 },
              },
            ]}
            yAxis={[
              {
                label: 'Outage (MW)',
                tickLabelStyle: { fill: theme.palette.common.white },
                labelStyle: { fill: theme.palette.common.white },
              },
            ]}
            series={chartSeries}
            grid={{ horizontal: true, vertical: false }}
            sx={{
              '& .MuiChartsAxis-line, & .MuiChartsAxis-tick': { stroke: `${theme.palette.common.white} !important` },
              '& .MuiChartsGrid-line': { stroke: `${theme.palette.text.disable} !important` },
              '& .MuiMarkElement-root': { strokeWidth: 1.5 },
              '& .MuiChartsLegend-label': {
                fill: `${theme.palette.common.white} !important`,
                color: `${theme.palette.common.white} !important`,
              },
              '& .MuiChartsLegend-root text, & .MuiChartsLegend-root tspan': {
                fill: `${theme.palette.common.white} !important`,
                userSelect: 'none',
                WebkitUserSelect: 'none',
                MozUserSelect: 'none',
                msUserSelect: 'none',
                pointerEvents: 'none',
              },
              '& .MuiChartsAxis-label': { fill: `${theme.palette.common.white} !important` },
              '& .MuiChartsAxis-tickLabel': { fill: `${theme.palette.common.white} !important` },
            }}
          />
        ) : (
          <T color="text.main">Select at least one facility or enable US to render the graph.</T>
        )}
      </div>
    </GraphsContainer>
  )
}

const buildDataUrl = ({
  base,
  dataset,
  limit,
  dateFrom,
  dateTo,
  facilityId,
}: {
  base: string
  dataset: 'us' | 'facility' | 'plants'
  limit?: number
  dateFrom?: string
  dateTo?: string
  facilityId?: string
}) => {
  const params = new URLSearchParams()
  params.append('dataset', dataset)

  if (limit) params.append('limit', String(limit))
  if (dateFrom) params.append('date_from', dateFrom)
  if (dateTo) params.append('date_to', dateTo)
  if (facilityId) params.append('facility_id', facilityId)

  return `${base}data?${params.toString()}`
}

const formatDateInput = (date: Date) => date.toISOString().split('T')[0]

const parseDateInput = (value: string): Date | null => {
  if (!value) return null
  const [year, month, day] = value.split('-').map(Number)
  if (!year || !month || !day) return null
  const date = new Date(year, month - 1, day)
  return Number.isNaN(date.getTime()) ? null : date
}

const addDays = (date: Date, days: number) => {
  const next = new Date(date)
  next.setDate(next.getDate() + days)
  return next
}

const minDate = (left: Date, right: Date) => (left <= right ? left : right)

const Wrapper = () => {
  const apiUrl = import.meta.env.VITE_API_BASE_URL
  const [includeUs, setIncludeUs] = useState(true)
  const [dateTo, setDateTo] = useState(() => formatDateInput(new Date()))
  const [dateFrom, setDateFrom] = useState(() => {
    const now = new Date()
    now.setDate(now.getDate() - 30)
    return formatDateInput(now)
  })
  const [selectedFacilityIds, setSelectedFacilityIds] = useState<string[]>([])

  const handleDateFromChange = (value: string) => {
    const nextFrom = parseDateInput(value)
    const currentTo = parseDateInput(dateTo)

    if (!nextFrom) {
      setDateFrom(value)
      return
    }

    let adjustedTo = currentTo

    if (!adjustedTo || adjustedTo < nextFrom) {
      adjustedTo = nextFrom
    }

    const maxTo = addDays(nextFrom, MAX_DATE_RANGE_DAYS - 1)
    if (adjustedTo > maxTo) {
      adjustedTo = maxTo
    }

    setDateFrom(formatDateInput(nextFrom))
    setDateTo(formatDateInput(adjustedTo))
  }

  const handleDateToChange = (value: string) => {
    const nextTo = parseDateInput(value)
    const currentFrom = parseDateInput(dateFrom)

    if (!nextTo) {
      setDateTo(value)
      return
    }

    let adjustedFrom = currentFrom

    if (!adjustedFrom || adjustedFrom > nextTo) {
      adjustedFrom = nextTo
    }

    const minFrom = addDays(nextTo, -(MAX_DATE_RANGE_DAYS - 1))
    if (adjustedFrom < minFrom) {
      adjustedFrom = minFrom
    }

    setDateFrom(formatDateInput(adjustedFrom))
    setDateTo(formatDateInput(nextTo))
  }

  const effectiveLimit = useMemo(() => {
    if (!dateFrom || !dateTo) return DEFAULT_LIMIT

    const start = new Date(dateFrom)
    const end = new Date(dateTo)

    if (Number.isNaN(start.getTime()) || Number.isNaN(end.getTime())) {
      return DEFAULT_LIMIT
    }

    const msPerDay = 1000 * 60 * 60 * 24
    const diffInDays = Math.floor((end.getTime() - start.getTime()) / msPerDay) + 1

    if (diffInDays <= 0) return DEFAULT_LIMIT

    return Math.min(diffInDays, MAX_DATE_RANGE_DAYS, MAX_API_LIMIT)
  }, [dateFrom, dateTo])

  const {
    data: facilitiesResponse,
    isLoading: isFacilitiesLoading,
    isError: isFacilitiesError,
  } = useApiQuery({
    key: 'facilities',
    url: buildDataUrl({
      base: apiUrl,
      dataset: 'plants',
    }),
  })

  const {
    data: usResponse,
    isLoading: isUsLoading,
    isError: isUsError,
  } = useApiQuery({
    key: ['graph', 'us', { dateFrom, dateTo, effectiveLimit, includeUs }],
    url: buildDataUrl({
      base: apiUrl,
      dataset: 'us',
      limit: effectiveLimit,
      dateFrom: dateFrom || undefined,
      dateTo: dateTo || undefined,
    }),
    enabled: includeUs,
  })

  const facilities: Facility[] = facilitiesResponse?.data ?? []

  const selectedFacilities = facilities.filter((facility: Facility) => selectedFacilityIds.includes(facility.facility_id))

  const facilityQueries = useQueries({
    queries: selectedFacilities.map((facility: Facility) => ({
      queryKey: ['graph', 'facility', facility.facility_id, { dateFrom, dateTo, effectiveLimit }],
      queryFn: () =>
        apiFetch({
          method: 'GET',
          url: buildDataUrl({
            base: apiUrl,
            dataset: 'facility',
            limit: effectiveLimit,
            dateFrom: dateFrom || undefined,
            dateTo: dateTo || undefined,
            facilityId: facility.facility_id,
          }),
        }),
      staleTime: 1000 * 60 * 5,
    })),
  })

  const isFacilitySeriesLoading = facilityQueries.some(query => query.isLoading)
  const hasFacilitySeriesError = facilityQueries.some(query => query.isError)
  const effectiveUsLoading = includeUs ? isUsLoading : false
  const effectiveUsError = includeUs ? isUsError : false

  const chartRows = useMemo(() => {
    const usData: USOutages[] = includeUs ? (usResponse?.data ?? []) : []

    const rowsByDate = new Map<string, ChartRow>()

    usData.forEach(item => {
      rowsByDate.set(item.date, {
        date: item.date,
        us: item.outage,
      })
    })

    facilityQueries.forEach((query, index) => {
      const facility = selectedFacilities[index]
      const facilityResponse = query.data as { data: FacilityOutages[] } | undefined
      const facilityData: FacilityOutages[] = facilityResponse?.data ?? []

      facilityData.forEach(item => {
        if (!rowsByDate.has(item.date)) {
          rowsByDate.set(item.date, { date: item.date, us: null })
        }
        const row = rowsByDate.get(item.date)!
        row[facility.facility_id] = item.outage
      })
    })

    const rows = Array.from(rowsByDate.values()).sort((a, b) => a.date.localeCompare(b.date))

    return rows.map(row => {
      const normalized: ChartRow = { ...row }
      selectedFacilities.forEach((facility: Facility) => {
        if (typeof normalized[facility.facility_id] === 'undefined') {
          normalized[facility.facility_id] = null
        }
      })
      return normalized
    })
  }, [includeUs, usResponse?.data, facilityQueries, selectedFacilities])

  if (isFacilitiesLoading || effectiveUsLoading || isFacilitySeriesLoading) {
    return <Loading />
  }

  if (isFacilitiesError || effectiveUsError || hasFacilitySeriesError) {
    return <NotAvailable />
  }

  if (chartRows.length === 0) {
    return <NotAvailable />
  }

  return (
    <Graphs
      includeUs={includeUs}
      dateFrom={dateFrom}
      dateTo={dateTo}
      effectiveLimit={effectiveLimit}
      facilities={facilities}
      selectedFacilityIds={selectedFacilityIds}
      chartRows={chartRows}
      onIncludeUsChange={setIncludeUs}
      onDateFromChange={handleDateFromChange}
      onDateToChange={handleDateToChange}
      onFacilitiesChange={setSelectedFacilityIds}
    />
  )
}

export default Wrapper
