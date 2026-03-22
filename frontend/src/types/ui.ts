export type DashboardCategory =
  | 'Home'
  | 'US Outages'
  | 'Facilities Outages'
  | 'Graphs'

export type SnackbarSeverity =
| 'success'
| 'info'
| 'warning'
| 'error'


export type SnackbarMessage = {
  severity: SnackbarSeverity
  message: string
}