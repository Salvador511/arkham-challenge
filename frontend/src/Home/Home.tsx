'use client'
import { Typography as T } from '@mui/material'
import { styled } from '@mui/material/styles'
import getClassPrefixer from '~/UI/classPrefixer'
import { powerPlant } from '~/UI/Images'

const displayName = 'Home'
const classes = getClassPrefixer(displayName) as any
const HomeContainer = styled('div')(({ theme }: any) => ({
  width: '100%',
  height: '100%',
  display: 'flex',
  flexDirection: 'column',
  [`& .${classes.mainContainer}`]: {
    display: 'flex',
    flexDirection: 'row',
    gap: '2rem',
  },
  [`& .${classes.textContainer}`]: {
    display: 'flex',
    flexDirection: 'column',
    justifyContent: 'flex-start',
    height: '100%',
    width: '70%',
    gap: '1ch',
    color: theme.palette.text.primary,

    '@media (max-width: 1024px)': {
      width: '95%',
    },
    '@media (max-width: 768px)': {
      width: '100%',
    },
  },
  [`& .${classes.subtitle}`]: {
    marginTop: '2rem',
    fontWeight: 'bold',
  },
  [`& .${classes.imageContainer}`]: {
    display: 'flex',
    justifyContent: 'center',
    alignItems: 'center',
    height: 'calc(100vh - 6rem)',
    '@media (max-width: 768px)': {
      display: 'none',
    },
  },
}))

const Home = () => {
  return (
    <HomeContainer>
      <div className={classes.mainContainer}>
        <div className={classes.textContainer}>
          <T variant="h4" color="primary.main" fontWeight="bold">
            U.S. Nuclear Power Plants Monitoring
          </T>
          <T textAlign="justify" color="text.main">
            This educational project is a platform to understand which nuclear power plants are operating 
            and which are shut down in the United States. An <strong>outage</strong> is simply when a plant 
            stops generating electricity, whether due to maintenance or technical issues. The data you see here comes from 
            the <strong>Energy Information Administration (EIA)</strong> at 
            <a href="https://www.eia.gov/" target="_blank" rel="noopener noreferrer"> www.eia.gov</a>, the official 
            U.S. government agency for energy data.
          </T>

          <T variant="h5" color="primary.main" className={classes.subtitle}>
            Why does this matter?
          </T>
          <T textAlign="justify" color="text.main">
            Nuclear power plants generate nearly 20% of the electricity consumed in the country. When a plant shuts down, 
            the electrical grid must compensate with other sources. Knowing the status of all plants helps 
            grid operators maintain system stability.
          </T>

          <T variant="h5" color="primary.main" className={classes.subtitle}>
            How did we build this platform?
          </T>
          <T textAlign="justify" color="text.main">
            The system consists of three parts working together:
          </T>

          <T variant="h6" color="primary.main">
            1. Data Collection
          </T>
          <T textAlign="justify" color="text.main">
            We regularly download data from the EIA. This gives us current information about which plants 
            are operating, how much energy they generate, and if they have any issues. The data is stored in an organized way 
            so we can access it later.
          </T>

          <T variant="h6" color="primary.main">
            2. The API (Access Point)
          </T>
          <T textAlign="justify" color="text.main">
            The API is like a counter where you can request information. For example: "give me data for plant X" 
            or "give me the national average". It does the work of finding and delivering the information you need quickly.
          </T>

          <T variant="h6" color="primary.main">
            3. The Visual Interface (this page)
          </T>
          <T textAlign="justify" color="text.main">
            This is what you're seeing right now. It takes information from the API and displays it in ways you can easily understand: 
            tables, charts, current data. You don't need to be technical to explore what's happening with nuclear power plants.
          </T>

          <T variant="h5" color="primary.main" className={classes.subtitle}>
            What information is available
          </T>
          <T textAlign="justify" color="text.main">
            <strong>• Per plant:</strong> Its capacity in megawatts, current status (operating or in outage), and history of shutdowns
            <br />
            <strong>• National view:</strong> A summary of the entire U.S. nuclear system right now
            <br />
            <strong>• Historical trends:</strong> Interactive graphs and charts showing outage patterns and capacity changes over time
          </T>

          <T variant="h5" color="primary.main" className={classes.subtitle}>
            Start exploring
          </T>
          <T textAlign="justify" color="text.main">
            Use the menu to navigate the dashboard and see the data in real time. 
            You don't need technical knowledge—everything is designed to be intuitive and easy to understand.
          </T>
        </div>
        <div className={classes.imageContainer}>
          <img
            src={powerPlant} alt="Power Plant" style={{ height: 200, width: 350 }} />
        </div>
      </div>
    </HomeContainer>
  )
}

export default Home
