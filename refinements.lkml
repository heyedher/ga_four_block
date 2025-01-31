include: "**/*.view.lkml"
include: "**/*.explore.lkml"

datagroup: hour {
  sql_trigger: SELECT EXTRACT(HOUR FROM CURRENT_TIMESTAMP()) ;;
}

datagroup: six_minute_refresh {
  sql_trigger: SELECT TIMESTAMP_SECONDS(DIV(UNIX_SECONDS(CURRENT_TIMESTAMP()), 360) * 360);;
  max_cache_age: "6 minutes"
}

# #
# Use LookML refinements to refine views and explores defined in the remote project.
# Learn more at: https://cloud.google.com/looker/docs/data-modeling/learning-lookml/refinements
#
#
# For example we could add a new dimension to a view:
#     view: +flights {
#       dimension: air_carrier {
#         type: string
#         sql: ${TABLE}.air_carrier ;;
#       }
#     }
#
# Or apply a label to an explore:
#     explore: +aircraft {
#       label: "Aircraft Simplified"
#     }
#

view: +session_list_with_event_history {
  derived_table: {
    datagroup_trigger: hour
    sql: select partition_date session_date
            ,  (select value.int_value from UNNEST(events.event_params) where key = "ga_session_id") ga_session_id
            ,  (select value.int_value from UNNEST(events.event_params) where key = "ga_session_number") ga_session_number
            ,  events.user_pseudo_id
            -- unique key for session:
            ,  partition_date||(select value.int_value from UNNEST(events.event_params) where key = "ga_session_id")||(select value.int_value from UNNEST(events.event_params) where key = "ga_session_number")||events.user_pseudo_id as sl_key
            ,  row_number() over (partition by (partition_date||(select value.int_value from UNNEST(events.event_params) where key = "ga_session_id")||(select value.int_value from UNNEST(events.event_params) where key = "ga_session_number")||events.user_pseudo_id) order by events.event_timestamp) event_rank
            ,  (TIMESTAMP_DIFF(TIMESTAMP_MICROS(LEAD(events.event_timestamp) OVER (PARTITION BY partition_date||(select value.int_value from UNNEST(events.event_params) where key = "ga_session_id")||(select value.int_value from UNNEST(events.event_params) where key = "ga_session_number")||events.user_pseudo_id ORDER BY events.event_timestamp asc))
               ,TIMESTAMP_MICROS(events.event_timestamp),second)/86400.0) time_to_next_event
            , case when events.event_name = 'page_view' then row_number() over (partition by (partition_date||(select value.int_value from UNNEST(events.event_params) where key = "ga_session_id")||(select value.int_value from UNNEST(events.event_params) where key = "ga_session_number")||events.user_pseudo_id), case when events.event_name = 'page_view' then true else false end order by events.event_timestamp)
              else 0 end as page_view_rank
            , case when events.event_name = 'page_view' then row_number() over (partition by (partition_date||(select value.int_value from UNNEST(events.event_params) where key = "ga_session_id")||(select value.int_value from UNNEST(events.event_params) where key = "ga_session_number")||events.user_pseudo_id), case when events.event_name = 'page_view' then true else false end order by events.event_timestamp desc)
              else 0 end as page_view_reverse_rank
            , case when events.event_name = 'page_view' then (TIMESTAMP_DIFF(TIMESTAMP_MICROS(LEAD(events.event_timestamp) OVER (PARTITION BY partition_date||(select value.int_value from UNNEST(events.event_params) where key = "ga_session_id")||(select value.int_value from UNNEST(events.event_params) where key = "ga_session_number")||events.user_pseudo_id , case when events.event_name = 'page_view' then true else false end ORDER BY events.event_timestamp asc))
            ,TIMESTAMP_MICROS(events.event_timestamp),second)/86400.0) else null end as time_to_next_page -- this window function yields 0 duration results when session page_view count = 1.
            -- raw event data:
            , events.event_date
            , events.event_timestamp
            , events.event_name
            , events.event_params
            , events.event_previous_timestamp
            , events.event_value_in_usd
            , events.event_bundle_sequence_id
            , events.event_server_timestamp_offset
            , events.user_id
            -- , events.user_pseudo_id
            , events.user_properties
            , events.user_first_touch_timestamp
            , events.user_ltv
            , events.device
            , events.geo
            , events.app_info
            , events.traffic_source
            , events.stream_id
            , events.platform
            , events.event_dimensions
            , events.ecommerce
            , ARRAY(select as STRUCT it.* EXCEPT(item_params) from unnest(events.items) as it) as items
            from `ga4_export.events_intraday_partitioned_view` events
            where {% incrementcondition %} partition_date {%  endincrementcondition %} ;;
  }
}

# Changing trigger to re-build pdt daily

view: +device_geo {
  derived_table: {
    sql_trigger_value: SELECT EXTRACT(HOUR FROM CURRENT_TIMESTAMP()) ;;
  }
}

view: +session_facts {
  derived_table: {
    sql_trigger_value: SELECT EXTRACT(HOUR FROM CURRENT_TIMESTAMP()) ;;
  }
}

view: +session_tags {
  derived_table: {
    sql_trigger_value: SELECT EXTRACT(HOUR FROM CURRENT_TIMESTAMP()) ;;
  }
}

# view: +session_event_packing {
#   derived_table: {
#     sql_trigger_value: ${session_facts.SQL_TABLE_NAME} ;;
#   }
# }

# view: +sessions {
#   derived_table: {
#     sql_trigger_value: ${device_geo.SQL_TABLE_NAME} ;;
#   }
# }

view: +future_purchase_model {
  derived_table: {
    sql: SELECT 1 ;;
  }
}
