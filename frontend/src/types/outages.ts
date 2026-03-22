export type USOutages = {
  date: string
  capacity: number
  outage: number
  percent_outage: number
}

export type FacilityOutages = {
  date: string
  facility_id: string
  capacity: number
  outage: number
  percent_outage: number
  facility_name: string
}