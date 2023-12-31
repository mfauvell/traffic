import copy

base_model_config = dict(
    batch_size=192,
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
    epochs=10,
    max_grad_norm=5.0,
    target="load",
    from_date = '2019-01-01 00:00:00',
    to_date = '2023-06-01 00:00:00',
    test_days_gap=30,
    graph_threshold = -1,
    graph_limit_distance = 0.1,
    combine_graph = False
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
drop_temporal_features = dict(
    year = 'drop',
    month = 'drop',
    day = 'drop',
    hour = 'drop',
    minute = 'drop',
    weekday = 'drop',
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
drop_calendar_features = dict(
    prematch = 'drop',
    match = 'drop',
    postmatch = 'drop',
    bank_holiday = 'drop',
    work_office_day = 'drop',
    school_day = 'drop',
    school_holiday = 'drop',
    state_of_alarm = 'drop'
)
base_traffic_features = dict(
    intensity = 'passthrough',
    occupation = 'passthrough'
)
drop_traffic_features = dict(
    intensity = 'drop',
    occupation = 'drop'
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
drop_meteo_features = dict(
    ultraviolet = 'drop',
    wind = 'drop',
    temperature = 'drop',
    humidity = 'drop',
    pressure = 'drop',
    radiation = 'drop',
    rain = 'drop',
)

def get_configs_train():
    config6 = base_model_config | base_temporal_features | drop_calendar_features | drop_traffic_features |drop_meteo_features
    config6['prematch'] = 'passthrough'
    config6['match'] = 'passthrough' 
    config6['postmatch'] = 'passthrough' 
    config7 = base_model_config | base_temporal_features | drop_calendar_features | drop_traffic_features |drop_meteo_features
    config7['rain'] = 'passthrough'
    config7['temperature'] = 'passthrough'
    config8 = base_model_config | base_temporal_features | drop_calendar_features | drop_traffic_features |drop_meteo_features
    config8['rain'] = 'ordinal'
    config8['temperature'] = 'passthrough'
    config9 = base_model_config | base_temporal_features | base_calendar_features | drop_traffic_features |drop_meteo_features
    config9['day_type'] = 'one_hot'
    config9['season'] = 'one_hot'
    config10 = base_model_config | base_temporal_features | drop_calendar_features | drop_traffic_features |drop_meteo_features
    config10['bank_holiday'] = 'passthrough'
    config10['work_office_day'] = 'passthrough'
    config11 = base_model_config | base_temporal_features | drop_calendar_features | drop_traffic_features |drop_meteo_features
    config11['school_holiday'] = 'passthrough'
    config11['school_day'] = 'passthrough'
    return dict(
        c1 = base_model_config | drop_temporal_features | drop_calendar_features | drop_traffic_features |drop_meteo_features,
        c2 = base_model_config | base_temporal_features | drop_calendar_features | drop_traffic_features |drop_meteo_features,
        c3 = base_model_config | base_temporal_features | base_calendar_features | drop_traffic_features |drop_meteo_features,
        c4 = base_model_config | base_temporal_features | drop_calendar_features | base_traffic_features |drop_meteo_features,
        c5 = base_model_config | base_temporal_features | drop_calendar_features | drop_traffic_features |base_meteo_features,
        c6 = config6,
        c7 = config7,
        c8 = config8,
        c9 = config9,
        c10 = config10,
        c11 = config11
    )

def get_configs_graphs_study():
    config = base_model_config | base_temporal_features | base_calendar_features | drop_traffic_features | base_meteo_features
    config['epochs'] = 10
    config1 = copy.deepcopy(config)
    config1['graph_threshold'] = 0
    config2 = copy.deepcopy(config)
    config2['graph_threshold'] = 5
    config3 = copy.deepcopy(config)
    config3['graph_threshold'] = 10
    config4 = copy.deepcopy(config)
    config4['graph_threshold'] = -1
    config4['graph_limit_distance'] = 0.05
    config5 = copy.deepcopy(config)
    config5['graph_threshold'] = -1
    config5['graph_limit_distance'] = 0.1
    config6 = copy.deepcopy(config)
    config6['graph_threshold'] = -1
    config6['graph_limit_distance'] = 0.15
    config7 = copy.deepcopy(config)
    config7['combine_graph'] = True
    config7['graph_limit_distance'] = 0.05
    config8 = copy.deepcopy(config)
    config8['combine_graph'] = True
    config8['graph_limit_distance'] = 0.10
    config9 = copy.deepcopy(config)
    config9['combine_graph'] = True
    config9['graph_limit_distance'] = 0.15
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
    )   

def get_final_config():
    config = base_model_config | base_temporal_features | drop_calendar_features | drop_traffic_features |drop_meteo_features
    config['prematch'] = 'passthrough'
    config['match'] = 'passthrough' 
    config['postmatch'] = 'passthrough'
    config['bank_holiday'] = 'passthrough'
    config['work_office_day'] = 'passthrough'
    config['rain'] = 'ordinal'
    config['temperature'] = 'passthrough'
    config['school_holiday'] = 'passthrough'
    config['school_day'] = 'passthrough'
    config2 = base_model_config | base_temporal_features | drop_calendar_features | drop_traffic_features |drop_meteo_features
    config2['prematch'] = 'passthrough'
    config2['match'] = 'passthrough' 
    config2['postmatch'] = 'passthrough'
    config2['rain'] = 'ordinal'
    config2['temperature'] = 'passthrough'
    config2['school_holiday'] = 'passthrough'
    config2['school_day'] = 'passthrough'
    return dict(c1 = config, c2 = config2)