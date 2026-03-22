'use client'
import { CircularProgress } from '@mui/material'
import { styled } from '@mui/material/styles'

const Overlay = styled('div')({
  width: '100%',
  height: '100%',
  display: 'flex',
  justifyContent: 'center',
  alignItems: 'center',
})

const Loading = ({ size = 60 }) => {
  return (
    <Overlay>
      <CircularProgress size={size} />
    </Overlay>
  )
}

export default Loading
