import colorUtil from 'color'

const states = {
  strong: 0.85,
  muted: 0.64,
  inactive: 0.5,
  disable: 0.32,
  active: 0.16,
  enabled: 0.08,
  modal: 0.25,
}

const getThemeColor = (color: string) => {
  const main = colorUtil(color)
  const variants = Object.entries(states).reduce((acc, [key, value]) => {
    return {
      ...acc,
      [key]: main.alpha(value).toString(),
    }
  }, {})
  return {
    main: main.toString(),
    ...variants,
  }
}

const contrast = getThemeColor('#2b8346')
const primary = getThemeColor('#F59E0B')
const text = getThemeColor('#f1f1f1')
const background = getThemeColor('#303030')
const gray = getThemeColor('#3c3c3c')
const red = getThemeColor('#f24b42')
const darkRed = getThemeColor('#9E443F')

const colors = {
  contrast,
  primary,
  text,
  background,
  gray,
  red,
  darkRed,
}

export default colors