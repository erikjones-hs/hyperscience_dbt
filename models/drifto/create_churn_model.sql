{{
    config(
        materialized='model',
        ml_config={
            'max_models': 3,
            'target': 'TARGET'
        }
    )
}}

select * from DEV.DRIFTO.TRAINING_v1