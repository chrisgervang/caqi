import numpy as np

def calc_aqi(Cp, Ih, Il, BPh, BPl):
    a = (Ih - Il)
    b = (BPh - BPl)
    c = (Cp - BPl)
    return round((a/b) * c + Il)

def aqi_from_pm(pm):
    if pm < 0: return pm
    if pm > 1000: return np.nan 
    '''     
    Good                            0 - 50           0.0 - 15.0         0.0 – 12.0
    Moderate                        51 - 100         >15.0 - 40        12.1 – 35.4
    Unhealthy for Sensitive Groups  101 – 150        >40 – 65          35.5 – 55.4
    Unhealthy                       151 – 200        > 65 – 150       55.5 – 150.4
    Very Unhealthy                  201 – 300        > 150 – 250     150.5 – 250.4
    Hazardous                       301 – 400        > 250 – 350     250.5 – 350.4
    Hazardous                       401 – 500        > 350 – 500     350.5 – 500
    '''
    if pm > 350.5:
        return calc_aqi(pm, 500, 401, 500, 350.5)
    elif pm > 250.5:
        return calc_aqi(pm, 400, 301, 350.4, 250.5)
    elif pm > 150.5:
        return calc_aqi(pm, 300, 201, 250.4, 150.5)
    elif pm > 55.5:
        return calc_aqi(pm, 200, 151, 150.4, 55.5)
    elif pm > 35.5:
        return calc_aqi(pm, 150, 101, 55.4, 35.5)
    elif pm > 12.1:
        return calc_aqi(pm, 100, 51, 35.4, 12.1)
    elif pm >= 0:                
        return calc_aqi(pm, 50, 0, 12, 0)
    else: return np.nan
