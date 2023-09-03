import numpy as np

def get_rain_categories(rain):
    rain_intervals = [
        rain == 0,
        (rain < 2.5) & (rain > 0),
        (rain < 5) & (rain >= 2.5),
        (rain < 10) & (rain >= 5),
        rain >= 10
    ]
    rain_categories = [
        "no_rain", "light_rain", "moderate_rain", "strong_rain", "torrential_rain"
    ]
    return np.select(rain_intervals, rain_categories, default="no_rain")

print(get_rain_categories(0))
print(get_rain_categories(2))
print(get_rain_categories(3))
print(get_rain_categories(6))
print(get_rain_categories(11))
print(get_rain_categories(20))