import SsidChartIcon from '@mui/icons-material/SsidChart'
import HomeIcon from '@mui/icons-material/Home'
import type { ComponentType } from 'react'
import type { DashboardCategory } from '~/types/ui'

export interface SidebarOption {
  title: DashboardCategory
  Icon: ComponentType<any> | null
  requiresAuth?: boolean
  adminOnly?: boolean
  unloggedOnly?: boolean
}

export const SIDEBAR_OPTS: SidebarOption[] = [
  {
    title: 'Home',
    Icon: HomeIcon,
  },
  {
    title: 'US Outages',
    Icon: null,
  },
  {
    title: 'Facilities Outages',
    Icon: null,
  },
  {
    title: 'Graphs',
    Icon: SsidChartIcon,
  }
]
