import copy

base_model_config = dict(
    batch_size=256,
    num_workers=0,
    seq_len = 16,
    diffsteps = 2,
    direction = "both",
    out_feats = 64,
    num_layers = 2,
    decay_steps = 2000,
    lr=0.01,
    gpu = 1,
    minimum_lr=2e-6,
    epochs=20,
    max_grad_norm=5.0,
    target="load",
    from_date = '2019-01-01 00:00:00',
    to_date = '2023-06-01 00:00:00',
    test_days_gap=30,
    graph_threshold = 10,
    graph_limit_distance = 0.01
)
base_temporal_features = dict(
    year = 'passthrough',
    month = 'passthrough',
    day = 'passthrough',
    hour = 'passthrough',
    minute = 'passthrough',
    weekday = 'passthrough',
    day_type = 'drop',
    season = 'drop'
)
base_calendar_features = dict(
    prematch = 'passthrough',
    match = 'passthrough',
    postmatch = 'passthrough',
    bank_holiday = 'passthrough',
    work_office_day = 'passthrough',
    school_day = 'passthrough',
    school_holiday = 'passthrough',
    state_of_alarm = 'passthrough'
)
base_traffic_features = dict(
    intensity = 'passthrough',
    occupation = 'passthrough'
)
base_meteo_features = dict(
    ultraviolet = 'drop',
    wind = 'drop',
    temperature = 'passthrough',
    humidity = 'passthrough',
    pressure = 'passthrough',
    radiation = 'passthrough',
    rain = 'passthrough',
)



def get_configs_graphs_study():
    config = base_model_config | base_temporal_features | base_calendar_features | base_traffic_features | base_meteo_features
    config['epochs'] = 10
    config1 = copy.deepcopy(config)
    config1['graph_threshold'] = 0
    config2 = copy.deepcopy(config)
    config2['graph_threshold'] = 10
    config3 = copy.deepcopy(config)
    config3['graph_threshold'] = 25
    config4 = copy.deepcopy(config)
    config4['graph_threshold'] = 50
    config5 = copy.deepcopy(config)
    config5['graph_threshold'] = 100
    config6 = copy.deepcopy(config)
    config6['graph_threshold'] = -1
    config6['graph_limit_distance'] = 0.1
    config7 = copy.deepcopy(config)
    config7['graph_threshold'] = -1
    config7['graph_limit_distance'] = 0.2
    config8 = copy.deepcopy(config)
    config8['graph_threshold'] = -1
    config8['graph_limit_distance'] = 0.3
    config9 = copy.deepcopy(config)
    config9['graph_threshold'] = -1
    config9['graph_limit_distance'] = 0.4
    config10 = copy.deepcopy(config)
    config10['graph_threshold'] = -1
    config10['graph_limit_distance'] = 0.5
    return dict(
        c1 = config1,
        c2 = config2,
        c3 = config3,
        c4 = config4,
        c5 = config5,
        c6 = config6,
        c7 = config7,
        c8 = config8,
        c9 = config9,
        c10 = config10
    )   