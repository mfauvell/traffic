import numpy as np
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import make_pipeline, FeatureUnion
from sklearn.preprocessing import FunctionTransformer, OneHotEncoder, OrdinalEncoder, SplineTransformer, PowerTransformer, QuantileTransformer

all_bool_columns = [
    "prematch",
    "match",
    "postmatch",
    "bank_holiday",
    "work_office_day",
    "school_day", 
    "school_holiday",
    "state_of_alarm"
]

period_dict = dict(
    month=12,
    day=31,
    hour=24,
    minute=4,
    season=4,
    weekday=7
)

rain_columns = dict(
    one_hot=[f"rain_{i + 1}" for i in range(5)],
    ordinal=["rain"],
    numerico_power=["rain"],
    numerico_quantile_uniform=["rain"],
    numerico_quantile_normal=["rain"],
    drop=[],
    passthrough=["rain"]
)

wind_columns = dict(
    xy=["windx", "windy"],
    wind_speed=["wind"],
    drop=[]
)

season_transformer = dict(
    one_hot=OneHotEncoder(handle_unknown="ignore", sparse=False),
    ordinal=OrdinalEncoder(categories=[["summer", "spring", "fall", "winter"]]),
    drop="drop"
)

day_type_transformer = dict(
    one_hot=OneHotEncoder(handle_unknown="ignore", sparse=False),
    ordinal=OrdinalEncoder(categories=[["mon-fri", "sat", "sun"]]),
    drop="drop"
)

def get_rain_categories(rain):
    rain_intervals = [
        rain == 0,
        rain < 2.5,
        rain < 5,
        rain < 10,
        rain >= 15
    ]
    rain_categories = [
        "no_rain", "light_rain", "moderate_rain" "strong_rain", "torrential_rain"
    ]
    return np.select(rain_intervals, rain_categories, default="no_rain")

rain_categories_transformer = FunctionTransformer(get_rain_categories)

rain_one_hot = make_pipeline(
    rain_categories_transformer,
    OneHotEncoder(handle_unknown="ignore", sparse=False)
)

rain_ordinal = make_pipeline(
    rain_categories_transformer,
    OrdinalEncoder(categories=[["no_rain", "moderate_rain", "strong_rain"]]),
)

rain_transformer = dict(
    one_hot=rain_one_hot,
    ordinal=rain_ordinal,
    numerico_power=PowerTransformer(method='yeo-johnson'),
    numerico_quantile_uniform=QuantileTransformer(output_distribution="uniform"),
    numerico_quantile_normal=QuantileTransformer(output_distribution="normal"),
    passthrough="passthrough"
)

def transform_df(dataFrame, config, target, interactions = "drop"):
    transformer = preprocessing_transformer(config, target, interactions)
    dataFrame = transformer.fit_transform(dataFrame)
    return dataFrame

def preprocessing_transformer(config, target, interactions):
    # bool_columns = [k for (k, v) in temporal_dict.items() if k in all_bool_columns and v!="drop"]
    bool_columns = [c for c in all_bool_columns if config[c] == "passthrough"]
    transformers = [("target", "passthrough", [target])]
    n_columns_before_interactions = len(get_column_names(config, target))
    transformers += [
        ("hour_transformed", hour_transformer(config["hour"]), ["hour"]),
        ("bool", OrdinalEncoder(categories=[[False, True]] * len(bool_columns)), bool_columns),
        ("year", temp_categorical("year", config["year"]), ["year"]),
        ("season", season_transformer.get(config["season"]), ["season"]),
        ("day_type", day_type_transformer.get(config["day_type"]), ["day_type"]),
        ("month", temp_transformer(config["month"], period_dict["month"], "month"), ["month"]),
        ("day", temp_transformer(config["day"], period_dict["day"], "day"), ["day"]),
        ("weekday", temp_categorical("weekday", config["weekday"]), ["weekday"]),
        ("minute", temp_categorical("minute", config["minute"]), ["minute"]),
    ]

    if config["intensity"] != "drop":
        transformers.append(("intensity", config['intensity'], ["intensity"]))
    if config["occupation"] != "drop":
        transformers.append(("occupation", config['occupation'], ["occupation"]))
    
    if config["rain"] != "drop":
        transformers.append(("rain", rain_transformer.get(config["rain"]), ["rain"]))
    if config["wind"] != "drop":
        transformers.append(("wind", "passthrough", wind_columns.get(config["wind"])))
    if config["temperature"] != "drop":
        transformers.append(("temperature", config["temperature"], ["temperature"]))
    if config["humidity"] != "drop":
        transformers.append(("humidity", config["humidity"], ["humidity"]))
    if config["pressure"] != "drop":
        transformers.append(("pressure", config["pressure"], ["pressure"]))
    if config["radiation"] != "drop":
        transformers.append(("radiation", config["radiation"], ["solar_radiation"]))
    if config["ultraviolet"] != "drop":
        transformers.append(("ultraviolet", config["ultraviolet"], ["ultraviolet"]))

    step1 = ColumnTransformer(transformers=transformers)
    return step1

    # kernel_transformer = ColumnTransformer([
    #     ("target", "passthrough", [0, 1]),
    #     ("interactions", Nystroem(kernel="poly", degree=2, n_components=300, random_state=0),
    #      list(range(1, n_columns_before_interactions)))  # all columns different to the target
    # ])

    # if interactions == "poly":
    #     return FeatureUnion([
    #         ("marginal", step1),
    #         ("interactions", hour_workday_interaction(temporal_dict["hour"]))
    #     ])
    # elif interactions == "kernel":
    #     return make_pipeline(
    #         step1,
    #         kernel_transformer
    #     )
    # else:
    #     return step1
    
def get_column_names(config, target):
    column_names = [target]
    column_names += get_temp_column_names("hour", config["hour"])
    column_names += [c for c in all_bool_columns if config[c] == "passthrough"]
    column_names += get_temp_column_names("year", config["year"])
    column_names += get_temp_column_names("season", config["season"])
    column_names += get_temp_column_names("month", config["month"])
    column_names += get_temp_column_names("day", config["day"])
    column_names += get_temp_column_names("weekday", config["weekday"])
    column_names += get_temp_column_names("minute", config["minute"])
    column_names += rain_columns[config["rain"]]
    column_names += wind_columns[config["wind"]]
    column_names += [dim for dim in ["temperature", "humidity", "pressure", "radiation"] if
                    config[dim] == "passthrough"]
    return column_names

def get_temp_column_names(dimension, approach):
    if approach in ["passthrough", "ordinal"]:
        return [dimension]
    elif approach == "one_hot":
        if dimension == "year":
            return [f"{dimension}_{i + (dimension != 'hour') * 1}" for i in range(3)]
        return [f"{dimension}_{i + (dimension!='hour')*1}" for i in range(period_dict[dimension])]
    elif approach.startswith("spline"):
        if "_" in approach:
            n_splines = int(approach.split("_")[-1])
        else:
            n_splines = period_dict[dimension] // 2
        return [f"{dimension}_{i + 1}" for i in range(n_splines)]
    elif approach == "drop":
        return []
    elif approach.startswith("fourier"):
        if "_" in approach:
            n_variables = int(approach.split("_")[-1])
        else:
            n_variables = 1
        names = []
        for i in range(n_variables):
            names += [f"sin_{dimension}_{i+1}", f"cos_{dimension}_{i+1}"]
        return names
    
def get_rain_categories(rain):
    rain_intervals = [
        rain == 0,
        rain < 2.5,
        rain < 5,
        rain < 10,
        rain >= 15
    ]
    rain_categories = [
        "no_rain", "light_rain", "moderate_rain" "strong_rain", "torrential_rain"
    ]
    return np.select(rain_intervals, rain_categories, default="no_rain")

def hour_transformer(approach):
    if approach == "one_hot":
        return make_pipeline(
            FunctionTransformer(np.floor),
            temp_transformer(approach, period_dict["hour"], "hour")
        )
    else:
        return temp_transformer(approach, period_dict["hour"], "hour")
    
def temp_transformer(approach, period, dim):
    if approach == "passthrough":
        return "passthrough"
    elif approach == "one_hot":
        return OneHotEncoder(handle_unknown="ignore", sparse=False, categories=[get_temp_categories(dim)])
    elif approach.startswith("fourier"):
        if "_" in approach:
            n_variables = int(approach.split("_")[-1])
        else:
            n_variables = 1
        return sincos(period, n_variables=n_variables)
    elif approach.startswith("spline"):
        if "_" in approach:
            n_splines = int(approach.split("_")[-1])
        else:
            n_splines = period // 2
        return periodic_spline_transformer(period, n_splines=n_splines)
    elif approach == "drop":
        return approach
    
def sincos(period, n_variables=1):
    features = []
    for i in range(1, n_variables+1):
        features += [(f"sin_{i}", sin_transformer(period, n=i)),
                     (f"cos_{i}", cos_transformer(period, n=i))]
    return FeatureUnion(features)

def sin_transformer(period, n):
    return FunctionTransformer(lambda x: np.sin(x / period * 2 * n * np.pi))

def cos_transformer(period, n):
    return FunctionTransformer(lambda x: np.cos(x / period * 2 * n * np.pi))

def periodic_spline_transformer(period, n_splines=None, degree=3):
    if n_splines is None:
        n_splines = period
    n_knots = n_splines + 1  # periodic and include_bias is True
    return SplineTransformer(
        degree=degree,
        n_knots=n_knots,
        knots=np.linspace(0, period, n_knots).reshape(n_knots, 1),
        extrapolation="periodic",
        include_bias=True,
    )
    
def temp_categorical(dim, approach):
    if approach in ["passthrough", "drop"]:
        return approach
    elif approach == "one_hot":
        return OneHotEncoder(handle_unknown="ignore", sparse=False,
                          categories=[get_temp_categories(dim)])

def get_temp_categories(dim):
    if dim == "weekday":
        return list(range(1, 8))
    elif dim == "minute":
        return [0, 15, 30, 45]
    elif dim == "hour":
        return list(range(24))
    elif dim == "month":
        return list(range(1, 13))
    elif dim == "day_of_month":
        return list(range(1, 32))
    elif dim == "year":
        return [2019, 2020, 2021]