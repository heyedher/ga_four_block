- dashboard: acquisition
  title: "[GA4] Acquisition"
  layout: newspaper
  preferred_viewer: dashboards-next
  elements:
  - name: ''
    type: text
    title_text: ''
    subtitle_text: ''
    body_text: "---\n<div  style=\"text-align: center;\">\n\t<div>\n\t\t<h1 style=\"\
      font-size: 28px;\">Acquisition Overview</h1><h2 style=\"font-size: 16px;\">Use\
      \ Audience Cohort filter to change what field the data is grouped by for the\
      \ Overview metrics</h2></div>\n</div>"
    row: 4
    col: 0
    width: 24
    height: 4
  - title: Sessions
    name: Sessions
    model: ga4
    explore: sessions
    type: single_value
    fields: [sessions.total_sessions, sessions.total_first_visit_sessions_percentage]
    limit: 500
    column_limit: 50
    custom_color_enabled: true
    show_single_value_title: true
    show_comparison: false
    comparison_type: value
    comparison_reverse_colors: false
    show_comparison_label: true
    enable_conditional_formatting: true
    conditional_formatting_include_totals: false
    conditional_formatting_include_nulls: false
    color_application:
      collection_id: legacy
      palette_id: looker_classic
    custom_color: "#FFF"
    single_value_title: Sessions
    comparison_label: First Visit Sessions
    conditional_formatting: [{type: not null, value: !!null '', background_color: "#F9AB00",
        font_color: "#FFF", color_application: {collection_id: 7c56cc21-66e4-41c9-81ce-a60e1c3967b2,
          palette_id: 56d0c358-10a0-4fd6-aa0b-b117bef527ab}, bold: false, italic: false,
        strikethrough: false, fields: !!null ''}]
    x_axis_gridlines: false
    y_axis_gridlines: true
    show_view_names: false
    show_y_axis_labels: true
    show_y_axis_ticks: true
    y_axis_tick_density: default
    y_axis_tick_density_custom: 5
    show_x_axis_label: true
    show_x_axis_ticks: true
    y_axis_scale_mode: linear
    x_axis_reversed: false
    y_axis_reversed: false
    plot_size_by_field: false
    trellis: ''
    stacking: ''
    limit_displayed_rows: false
    legend_position: center
    point_style: none
    show_value_labels: false
    label_density: 25
    x_axis_scale: auto
    y_axis_combined: true
    ordering: none
    show_null_labels: false
    show_totals_labels: false
    show_silhouette: false
    totals_color: "#808080"
    defaults_version: 1
    series_types: {}
    note_state: collapsed
    note_display: hover
    note_text: Sessions based on filters
    listen:
      Audience Cohort [Required]: sessions.audience_selector
      Date: sessions.session_date
      Country [Optional]: sessions.geo_data_country
      Continent [Optional]: sessions.geo_data_continent
      Source [Optional]: sessions.session_attribution_source
      Source / Medium [Optional]: sessions.session_attribution_source_medium
      Medium [Optional]: sessions.session_attribution_medium
      Channel [Optional]: attribution_sources.attribution_source_channel
    row: 8
    col: 0
    width: 8
    height: 3
  - title: Users
    name: Users
    model: ga4
    explore: sessions
    type: single_value
    fields: [sessions.total_users]
    sorts: [sessions.total_users desc]
    limit: 500
    column_limit: 50
    custom_color_enabled: true
    show_single_value_title: true
    show_comparison: false
    comparison_type: value
    comparison_reverse_colors: false
    show_comparison_label: true
    enable_conditional_formatting: true
    conditional_formatting_include_totals: false
    conditional_formatting_include_nulls: false
    color_application:
      collection_id: 7c56cc21-66e4-41c9-81ce-a60e1c3967b2
      palette_id: 5d189dfc-4f46-46f3-822b-bfb0b61777b1
      options:
        steps: 5
    x_axis_gridlines: false
    y_axis_gridlines: true
    show_view_names: false
    show_y_axis_labels: true
    show_y_axis_ticks: true
    y_axis_tick_density: default
    y_axis_tick_density_custom: 5
    show_x_axis_label: true
    show_x_axis_ticks: true
    y_axis_scale_mode: linear
    x_axis_reversed: false
    y_axis_reversed: false
    plot_size_by_field: false
    trellis: ''
    stacking: ''
    limit_displayed_rows: false
    legend_position: center
    series_types: {}
    point_style: none
    show_value_labels: false
    label_density: 25
    x_axis_scale: auto
    y_axis_combined: true
    ordering: none
    show_null_labels: false
    show_totals_labels: false
    show_silhouette: false
    totals_color: "#808080"
    custom_color: "#FFF"
    single_value_title: Total Users
    comparison_label: Returning Users
    conditional_formatting: [{type: not null, value: !!null '', background_color: "#1A73E8",
        font_color: "#FFF", color_application: {collection_id: 7c56cc21-66e4-41c9-81ce-a60e1c3967b2,
          palette_id: 56d0c358-10a0-4fd6-aa0b-b117bef527ab}, bold: false, italic: false,
        strikethrough: false, fields: !!null ''}]
    defaults_version: 1
    listen:
      Audience Cohort [Required]: sessions.audience_selector
      Date: sessions.session_date
      Country [Optional]: sessions.geo_data_country
      Continent [Optional]: sessions.geo_data_continent
      Source [Optional]: sessions.session_attribution_source
      Source / Medium [Optional]: sessions.session_attribution_source_medium
      Medium [Optional]: sessions.session_attribution_medium
      Channel [Optional]: attribution_sources.attribution_source_channel
    row: 8
    col: 8
    width: 8
    height: 3
  - title: Page Views
    name: Page Views
    model: ga4
    explore: sessions
    type: single_value
    fields: [events.total_page_views]
    sorts: [events.total_page_views desc]
    limit: 500
    column_limit: 50
    custom_color_enabled: true
    show_single_value_title: true
    show_comparison: false
    comparison_type: value
    comparison_reverse_colors: false
    show_comparison_label: true
    enable_conditional_formatting: true
    conditional_formatting_include_totals: false
    conditional_formatting_include_nulls: false
    x_axis_gridlines: false
    y_axis_gridlines: true
    show_view_names: false
    show_y_axis_labels: true
    show_y_axis_ticks: true
    y_axis_tick_density: default
    y_axis_tick_density_custom: 5
    show_x_axis_label: true
    show_x_axis_ticks: true
    y_axis_scale_mode: linear
    x_axis_reversed: false
    y_axis_reversed: false
    plot_size_by_field: false
    trellis: ''
    stacking: ''
    limit_displayed_rows: false
    legend_position: center
    series_types: {}
    point_style: none
    series_colors:
      events.total_page_views: "#EA4335"
    show_value_labels: true
    label_density: 25
    x_axis_scale: auto
    y_axis_combined: true
    ordering: none
    show_null_labels: false
    show_totals_labels: false
    show_silhouette: false
    totals_color: "#808080"
    custom_color: "#FFF"
    comparison_label: Unique Page Views
    conditional_formatting: [{type: not null, value: !!null '', background_color: "#c73727",
        font_color: !!null '', color_application: {collection_id: 7c56cc21-66e4-41c9-81ce-a60e1c3967b2,
          palette_id: 56d0c358-10a0-4fd6-aa0b-b117bef527ab}, bold: false, italic: false,
        strikethrough: false, fields: !!null ''}]
    defaults_version: 1
    hidden_fields: []
    y_axes: []
    listen:
      Audience Cohort [Required]: sessions.audience_selector
      Date: sessions.session_date
      Country [Optional]: sessions.geo_data_country
      Continent [Optional]: sessions.geo_data_continent
      Source [Optional]: sessions.session_attribution_source
      Source / Medium [Optional]: sessions.session_attribution_source_medium
      Medium [Optional]: sessions.session_attribution_medium
      Channel [Optional]: attribution_sources.attribution_source_channel
    row: 8
    col: 16
    width: 8
    height: 3
  - title: Sessions by Cohort
    name: Sessions by Cohort
    model: ga4
    explore: sessions
    type: looker_bar
    fields: [sessions.total_sessions, sessions.audience_trait]
    sorts: [sessions.total_sessions desc]
    limit: 500
    column_limit: 50
    x_axis_gridlines: false
    y_axis_gridlines: true
    show_view_names: false
    show_y_axis_labels: true
    show_y_axis_ticks: true
    y_axis_tick_density: default
    y_axis_tick_density_custom: 5
    show_x_axis_label: false
    show_x_axis_ticks: true
    y_axis_scale_mode: linear
    x_axis_reversed: false
    y_axis_reversed: false
    plot_size_by_field: false
    trellis: ''
    stacking: ''
    limit_displayed_rows: true
    legend_position: center
    point_style: none
    show_value_labels: true
    label_density: 25
    x_axis_scale: auto
    y_axis_combined: true
    ordering: none
    show_null_labels: false
    show_totals_labels: false
    show_silhouette: false
    totals_color: "#808080"
    color_application:
      collection_id: 7c56cc21-66e4-41c9-81ce-a60e1c3967b2
      palette_id: 5d189dfc-4f46-46f3-822b-bfb0b61777b1
      options:
        steps: 5
    limit_displayed_rows_values:
      show_hide: show
      first_last: first
      num_rows: '10'
    series_types: {}
    series_colors:
      sessions.total_sessions: "#F9AB00"
    custom_color_enabled: true
    show_single_value_title: true
    show_comparison: false
    comparison_type: value
    comparison_reverse_colors: false
    show_comparison_label: true
    enable_conditional_formatting: true
    conditional_formatting_include_totals: false
    conditional_formatting_include_nulls: false
    custom_color: "#FFF"
    single_value_title: Sessions
    comparison_label: First Visit Sessions
    conditional_formatting: [{type: not null, value: !!null '', background_color: "#F9AB00",
        font_color: "#FFF", color_application: {collection_id: 7c56cc21-66e4-41c9-81ce-a60e1c3967b2,
          palette_id: 56d0c358-10a0-4fd6-aa0b-b117bef527ab}, bold: false, italic: false,
        strikethrough: false, fields: !!null ''}]
    defaults_version: 1
    note_state: collapsed
    note_display: hover
    note_text: Sessions based on filters
    listen:
      Audience Cohort [Required]: sessions.audience_selector
      Date: sessions.session_date
      Country [Optional]: sessions.geo_data_country
      Continent [Optional]: sessions.geo_data_continent
      Source [Optional]: sessions.session_attribution_source
      Source / Medium [Optional]: sessions.session_attribution_source_medium
      Medium [Optional]: sessions.session_attribution_medium
      Channel [Optional]: attribution_sources.attribution_source_channel
    row: 11
    col: 0
    width: 8
    height: 10
  - title: Users by Cohort
    name: Users by Cohort
    model: ga4
    explore: sessions
    type: looker_bar
    fields: [sessions.total_users, sessions.audience_trait]
    sorts: [sessions.total_users desc]
    limit: 500
    column_limit: 50
    x_axis_gridlines: false
    y_axis_gridlines: true
    show_view_names: false
    show_y_axis_labels: true
    show_y_axis_ticks: true
    y_axis_tick_density: default
    y_axis_tick_density_custom: 5
    show_x_axis_label: false
    show_x_axis_ticks: true
    y_axis_scale_mode: linear
    x_axis_reversed: false
    y_axis_reversed: false
    plot_size_by_field: false
    trellis: ''
    stacking: ''
    limit_displayed_rows: false
    legend_position: center
    point_style: none
    show_value_labels: true
    label_density: 25
    x_axis_scale: auto
    y_axis_combined: true
    ordering: none
    show_null_labels: false
    show_totals_labels: false
    show_silhouette: false
    totals_color: "#808080"
    color_application:
      collection_id: 7c56cc21-66e4-41c9-81ce-a60e1c3967b2
      palette_id: 5d189dfc-4f46-46f3-822b-bfb0b61777b1
      options:
        steps: 5
    series_types: {}
    custom_color_enabled: true
    show_single_value_title: true
    show_comparison: false
    comparison_type: value
    comparison_reverse_colors: false
    show_comparison_label: true
    enable_conditional_formatting: true
    conditional_formatting_include_totals: false
    conditional_formatting_include_nulls: false
    custom_color: "#FFF"
    single_value_title: Total Users
    comparison_label: Returning Users
    conditional_formatting: [{type: not null, value: !!null '', background_color: "#1A73E8",
        font_color: "#FFF", color_application: {collection_id: 7c56cc21-66e4-41c9-81ce-a60e1c3967b2,
          palette_id: 56d0c358-10a0-4fd6-aa0b-b117bef527ab}, bold: false, italic: false,
        strikethrough: false, fields: !!null ''}]
    defaults_version: 1
    listen:
      Audience Cohort [Required]: sessions.audience_selector
      Date: sessions.session_date
      Country [Optional]: sessions.geo_data_country
      Continent [Optional]: sessions.geo_data_continent
      Source [Optional]: sessions.session_attribution_source
      Source / Medium [Optional]: sessions.session_attribution_source_medium
      Medium [Optional]: sessions.session_attribution_medium
      Channel [Optional]: attribution_sources.attribution_source_channel
    row: 11
    col: 8
    width: 8
    height: 10
  - title: Page Views by Cohort
    name: Page Views by Cohort
    model: ga4
    explore: sessions
    type: looker_bar
    fields: [events.total_page_views, sessions.audience_trait]
    sorts: [events.total_page_views desc]
    limit: 500
    column_limit: 50
    x_axis_gridlines: false
    y_axis_gridlines: true
    show_view_names: false
    show_y_axis_labels: true
    show_y_axis_ticks: true
    y_axis_tick_density: default
    y_axis_tick_density_custom: 5
    show_x_axis_label: false
    show_x_axis_ticks: true
    y_axis_scale_mode: linear
    x_axis_reversed: false
    y_axis_reversed: false
    plot_size_by_field: false
    trellis: ''
    stacking: ''
    limit_displayed_rows: false
    legend_position: center
    point_style: none
    show_value_labels: true
    label_density: 25
    x_axis_scale: auto
    y_axis_combined: true
    ordering: none
    show_null_labels: false
    show_totals_labels: false
    show_silhouette: false
    totals_color: "#808080"
    series_types: {}
    series_colors:
      events.total_page_views: "#EA4335"
    custom_color_enabled: true
    show_single_value_title: true
    show_comparison: false
    comparison_type: value
    comparison_reverse_colors: false
    show_comparison_label: true
    enable_conditional_formatting: true
    conditional_formatting_include_totals: false
    conditional_formatting_include_nulls: false
    custom_color: "#FFF"
    comparison_label: Unique Page Views
    conditional_formatting: [{type: not null, value: !!null '', background_color: "#c73727",
        font_color: !!null '', color_application: {collection_id: 7c56cc21-66e4-41c9-81ce-a60e1c3967b2,
          palette_id: 56d0c358-10a0-4fd6-aa0b-b117bef527ab}, bold: false, italic: false,
        strikethrough: false, fields: !!null ''}]
    defaults_version: 1
    hidden_fields: []
    y_axes: []
    listen:
      Audience Cohort [Required]: sessions.audience_selector
      Date: sessions.session_date
      Country [Optional]: sessions.geo_data_country
      Continent [Optional]: sessions.geo_data_continent
      Source [Optional]: sessions.session_attribution_source
      Source / Medium [Optional]: sessions.session_attribution_source_medium
      Medium [Optional]: sessions.session_attribution_medium
      Channel [Optional]: attribution_sources.attribution_source_channel
    row: 11
    col: 16
    width: 8
    height: 10
  - title: Session Breakdown
    name: Session Breakdown
    model: ga4
    explore: sessions
    type: looker_bar
    fields: [sessions.audience_trait, sessions.total_sessions, audience_cohorts.rank]
    pivots: [sessions.audience_trait, audience_cohorts.rank]
    sorts: [audience_cohorts.rank, sessions.audience_trait]
    limit: 500
    column_limit: 50
    x_axis_gridlines: false
    y_axis_gridlines: true
    show_view_names: false
    show_y_axis_labels: true
    show_y_axis_ticks: true
    y_axis_tick_density: default
    y_axis_tick_density_custom: 5
    show_x_axis_label: true
    show_x_axis_ticks: true
    y_axis_scale_mode: linear
    x_axis_reversed: false
    y_axis_reversed: false
    plot_size_by_field: false
    trellis: ''
    stacking: percent
    limit_displayed_rows: false
    legend_position: center
    point_style: none
    show_value_labels: true
    label_density: 25
    x_axis_scale: auto
    y_axis_combined: true
    ordering: none
    show_null_labels: false
    show_totals_labels: false
    show_silhouette: false
    totals_color: "#808080"
    series_types: {}
    defaults_version: 1
    listen:
      Audience Cohort [Required]: sessions.audience_selector
      Date: sessions.session_date
      Country [Optional]: sessions.geo_data_country
      Continent [Optional]: sessions.geo_data_continent
      Source [Optional]: sessions.session_attribution_source
      Source / Medium [Optional]: sessions.session_attribution_source_medium
      Medium [Optional]: sessions.session_attribution_medium
      Channel [Optional]: attribution_sources.attribution_source_channel
    row: 21
    col: 0
    width: 24
    height: 7
  - title: Session by Week
    name: Session by Week
    model: ga4
    explore: sessions
    type: looker_column
    fields: [sessions.audience_trait, sessions.total_sessions, audience_cohorts.rank,
      sessions.session_week]
    pivots: [sessions.audience_trait, audience_cohorts.rank]
    fill_fields: [sessions.session_week]
    sorts: [audience_cohorts.rank, sessions.audience_trait, sessions.session_week
        desc]
    limit: 500
    column_limit: 50
    x_axis_gridlines: false
    y_axis_gridlines: true
    show_view_names: false
    show_y_axis_labels: true
    show_y_axis_ticks: true
    y_axis_tick_density: default
    y_axis_tick_density_custom: 5
    show_x_axis_label: true
    show_x_axis_ticks: true
    y_axis_scale_mode: linear
    x_axis_reversed: false
    y_axis_reversed: false
    plot_size_by_field: false
    trellis: ''
    stacking: percent
    limit_displayed_rows: false
    legend_position: center
    point_style: none
    show_value_labels: true
    label_density: 25
    x_axis_scale: auto
    y_axis_combined: true
    ordering: none
    show_null_labels: false
    show_totals_labels: false
    show_silhouette: false
    totals_color: "#808080"
    series_types: {}
    defaults_version: 1
    listen:
      Audience Cohort [Required]: sessions.audience_selector
      Date: sessions.session_date
      Country [Optional]: sessions.geo_data_country
      Continent [Optional]: sessions.geo_data_continent
      Source [Optional]: sessions.session_attribution_source
      Source / Medium [Optional]: sessions.session_attribution_source_medium
      Medium [Optional]: sessions.session_attribution_medium
      Channel [Optional]: attribution_sources.attribution_source_channel
    row: 28
    col: 0
    width: 24
    height: 10
  - title: Engagement
    name: Engagement
    model: ga4
    explore: sessions
    type: looker_grid
    fields: [sessions.total_sessions, sessions.audience_trait, sessions.total_first_visit_sessions_percentage,
      sessions.total_bounced_sessions_percentage, sessions.average_page_views_per_session,
      sessions.average_session_duration]
    sorts: [sessions.total_sessions desc]
    limit: 500
    column_limit: 50
    total: true
    show_view_names: false
    show_row_numbers: true
    transpose: false
    truncate_text: true
    hide_totals: false
    hide_row_totals: false
    size_to_fit: true
    table_theme: white
    limit_displayed_rows: true
    enable_conditional_formatting: false
    header_text_alignment: left
    header_font_size: '20'
    rows_font_size: '20'
    conditional_formatting_include_totals: false
    conditional_formatting_include_nulls: false
    show_sql_query_menu_options: false
    show_totals: true
    show_row_totals: true
    series_labels:
      sessions.audience_trait: Cohort
      sessions.total_first_visit_sessions_percentage: "% New Sessions"
      sessions.total_bounced_sessions_percentage: Bounce Rate
      sessions.average_page_views_per_session: Pages / Session
      sessions.average_session_duration: Avg. Session Duration
    series_column_widths:
      sessions.audience_trait: 252
    series_cell_visualizations:
      sessions.total_first_visit_sessions_percentage:
        is_active: true
      sessions.total_bounced_sessions_percentage:
        is_active: true
      sessions.average_page_views_per_session:
        is_active: true
      sessions.average_session_duration:
        is_active: true
    limit_displayed_rows_values:
      show_hide: show
      first_last: first
      num_rows: '50'
    header_background_color: "#F9AB00"
    series_value_format:
      sessions.total_bounced_sessions_percentage:
        name: percent_0
        decimals: '0'
        format_string: "#,##0%"
        label: Percent (0)
        label_prefix: Percent
      sessions.total_first_visit_sessions_percentage:
        name: percent_0
        decimals: '0'
        format_string: "#,##0%"
        label: Percent (0)
        label_prefix: Percent
      sessions.average_page_views_per_session:
        name: decimal_2
        decimals: '2'
        format_string: "#,##0.00"
        label: Decimals (2)
        label_prefix: Decimals
    x_axis_gridlines: false
    y_axis_gridlines: true
    show_y_axis_labels: true
    show_y_axis_ticks: true
    y_axis_tick_density: default
    y_axis_tick_density_custom: 5
    show_x_axis_label: true
    show_x_axis_ticks: true
    y_axis_scale_mode: linear
    x_axis_reversed: false
    y_axis_reversed: false
    plot_size_by_field: false
    trellis: ''
    stacking: percent
    legend_position: center
    point_style: none
    show_value_labels: true
    label_density: 25
    x_axis_scale: auto
    y_axis_combined: true
    ordering: none
    show_null_labels: false
    show_totals_labels: false
    show_silhouette: false
    totals_color: "#808080"
    series_types: {}
    defaults_version: 1
    hidden_fields: [sessions.total_sessions]
    note_state: collapsed
    note_display: hover
    note_text: Engagement metrics by Audience Trait (based on Audience Cohort filter).
      Sorted by trait with highest traffic (i.e. Sessions).
    listen:
      Audience Cohort [Required]: sessions.audience_selector
      Date: sessions.session_date
      Country [Optional]: sessions.geo_data_country
      Continent [Optional]: sessions.geo_data_continent
      Source [Optional]: sessions.session_attribution_source
      Source / Medium [Optional]: sessions.session_attribution_source_medium
      Medium [Optional]: sessions.session_attribution_medium
      Channel [Optional]: attribution_sources.attribution_source_channel
    row: 38
    col: 0
    width: 24
    height: 11
  - name: " (2)"
    type: text
    title_text: ''
    subtitle_text: ''
    body_text: "---\n<div  style=\"text-align: center;\">\n\t<div>\n\t\t<h1 style=\"\
      font-size: 28px;\">Acquisition Detail</h1><h2 style=\"font-size: 16px;\">Audience\
      \ Cohort filter does not affect any tiles below this point</h2></div>\n</div>"
    row: 49
    col: 0
    width: 24
    height: 4
  - title: Top Referrers
    name: Top Referrers
    model: ga4
    explore: sessions
    type: looker_bar
    fields: [sessions.session_attribution_source, sessions.total_sessions]
    filters: {}
    sorts: [sessions.total_sessions desc]
    limit: 500
    column_limit: 50
    dynamic_fields: [{table_calculation: of_total, label: "% of Total", expression: "${sessions.total_sessions}/sum(${sessions.total_sessions})",
        value_format: !!null '', value_format_name: percent_0, _kind_hint: measure,
        _type_hint: number}]
    x_axis_gridlines: false
    y_axis_gridlines: false
    show_view_names: false
    show_y_axis_labels: true
    show_y_axis_ticks: true
    y_axis_tick_density: default
    y_axis_tick_density_custom: 5
    show_x_axis_label: false
    show_x_axis_ticks: true
    y_axis_scale_mode: linear
    x_axis_reversed: false
    y_axis_reversed: false
    plot_size_by_field: false
    trellis: ''
    stacking: ''
    limit_displayed_rows: true
    legend_position: center
    point_style: none
    show_value_labels: true
    label_density: 25
    x_axis_scale: auto
    y_axis_combined: true
    ordering: none
    show_null_labels: false
    show_totals_labels: false
    show_silhouette: false
    totals_color: "#808080"
    limit_displayed_rows_values:
      show_hide: show
      first_last: first
      num_rows: '10'
    hidden_series: [of_total]
    series_types: {}
    series_colors:
      sessions.total_sessions: "#F9AB00"
      of_total: "#facb04"
    defaults_version: 1
    listen:
      Date: sessions.session_date
      Country [Optional]: sessions.geo_data_country
      Continent [Optional]: sessions.geo_data_continent
      Source [Optional]: sessions.session_attribution_source
      Source / Medium [Optional]: sessions.session_attribution_source_medium
      Medium [Optional]: sessions.session_attribution_medium
      Channel [Optional]: attribution_sources.attribution_source_channel
    row: 53
    col: 0
    width: 12
    height: 10
  - title: Top Landing Page
    name: Top Landing Page
    model: ga4
    explore: sessions
    type: looker_bar
    fields: [sessions.total_sessions, events.event_param_page]
    filters:
      events.event_param_page: "-EMPTY"
      events.is_landing_page: 'Yes'
    sorts: [sessions.total_sessions desc]
    limit: 500
    column_limit: 50
    dynamic_fields: [{table_calculation: of_total, label: "% of Total", expression: "${sessions.total_sessions}/sum(${sessions.total_sessions})",
        value_format: !!null '', value_format_name: percent_0, _kind_hint: measure,
        _type_hint: number}]
    x_axis_gridlines: false
    y_axis_gridlines: false
    show_view_names: false
    show_y_axis_labels: true
    show_y_axis_ticks: true
    y_axis_tick_density: default
    y_axis_tick_density_custom: 5
    show_x_axis_label: false
    show_x_axis_ticks: true
    y_axis_scale_mode: linear
    x_axis_reversed: false
    y_axis_reversed: false
    plot_size_by_field: false
    trellis: ''
    stacking: ''
    limit_displayed_rows: true
    legend_position: center
    point_style: none
    show_value_labels: true
    label_density: 25
    x_axis_scale: auto
    y_axis_combined: true
    ordering: none
    show_null_labels: false
    show_totals_labels: false
    show_silhouette: false
    totals_color: "#808080"
    limit_displayed_rows_values:
      show_hide: show
      first_last: first
      num_rows: '10'
    hidden_series: [of_total]
    series_types: {}
    series_colors:
      sessions.total_sessions: "#F9AB00"
      of_total: "#facb04"
    defaults_version: 1
    hidden_fields: []
    y_axes: []
    listen:
      Date: sessions.session_date
      Country [Optional]: sessions.geo_data_country
      Continent [Optional]: sessions.geo_data_continent
      Source [Optional]: sessions.session_attribution_source
      Source / Medium [Optional]: sessions.session_attribution_source_medium
      Medium [Optional]: sessions.session_attribution_medium
      Channel [Optional]: attribution_sources.attribution_source_channel
    row: 53
    col: 12
    width: 12
    height: 10
  - name: " (3)"
    type: text
    title_text: ''
    subtitle_text: ''
    body_text: "<div style=\"border: solid 1px #1A73E8; border-radius: 5px; padding:\
    \ 3px 10px; background: #eaf1fe; text-align: center; margin-bottom: 10px;\"\
    >\n\t<div>\n\t\t<img style=\"height: 60px; margin-top: 30px;\" src=\"https://www.gstatic.com/analytics-suite/header/suite/v2/ic_analytics.svg\"\
    />\n\t</div><br>\n<nav style=\"font-size: 18px; position: absolute; bottom:\
    \ 0; text-align: center;\">\n  <a style=\"padding: 5px; line-height: 40px;\"\
    \ href=\"/dashboards-next/ga4::overview\">\n  \t<svg style=\"height: 16px; fill: #4285F4;\" class=\"\
    svg-icon\" viewBox=\"0 0 20 20\">\n\t\t\t\t\t\t\t<path d=\"M17.431,2.156h-3.715c-0.228,0-0.413,0.186-0.413,0.413v6.973h-2.89V6.687c0-0.229-0.186-0.413-0.413-0.413H6.285c-0.228,0-0.413,0.184-0.413,0.413v6.388H2.569c-0.227,0-0.413,0.187-0.413,0.413v3.942c0,0.228,0.186,0.413,0.413,0.413h14.862c0.228,0,0.413-0.186,0.413-0.413V2.569C17.844,2.342,17.658,2.156,17.431,2.156\
    \ M5.872,17.019h-2.89v-3.117h2.89V17.019zM9.587,17.019h-2.89V7.1h2.89V17.019z\
    \ M13.303,17.019h-2.89v-6.651h2.89V17.019z M17.019,17.019h-2.891V2.982h2.891V17.019z\"\
    ></path>\n</svg>\nGA Overview</a>\n<a style=\"padding: 5px; line-height: 40px;\"\
    \ href=\"/dashboards-next/ga4::audience\">\n<svg style=\"height: 16px; fill: #4285F4;\" class=\"svg-icon\"\
    \ viewBox=\"0 0 20 20\">\n<path d=\"M12.075,10.812c1.358-0.853,2.242-2.507,2.242-4.037c0-2.181-1.795-4.618-4.198-4.618S5.921,4.594,5.921,6.775c0,1.53,0.884,3.185,2.242,4.037c-3.222,0.865-5.6,3.807-5.6,7.298c0,0.23,0.189,0.42,0.42,0.42h14.273c0.23,0,0.42-0.189,0.42-0.42C17.676,14.619,15.297,11.677,12.075,10.812\
    \ M6.761,6.775c0-2.162,1.773-3.778,3.358-3.778s3.359,1.616,3.359,3.778c0,2.162-1.774,3.778-3.359,3.778S6.761,8.937,6.761,6.775\
    \ M3.415,17.69c0.218-3.51,3.142-6.297,6.704-6.297c3.562,0,6.486,2.787,6.705,6.297H3.415z\"\
    ></path>\n</svg>\nAudience</a>\n<a style=\"padding: 5px; line-height: 40px;\"\
    \ href=\"/dashboards-next/ga4::acquisition\">\n<svg style=\"height: 16px; fill: #4285F4;\" class=\"svg-icon\"\
    \ viewBox=\"0 0 20 20\">\n<path d=\"M8.749,9.934c0,0.247-0.202,0.449-0.449,0.449H4.257c-0.247,0-0.449-0.202-0.449-0.449S4.01,9.484,4.257,9.484H8.3C8.547,9.484,8.749,9.687,8.749,9.934\
    \ M7.402,12.627H4.257c-0.247,0-0.449,0.202-0.449,0.449s0.202,0.449,0.449,0.449h3.145c0.247,0,0.449-0.202,0.449-0.449S7.648,12.627,7.402,12.627\
    \ M8.3,6.339H4.257c-0.247,0-0.449,0.202-0.449,0.449c0,0.247,0.202,0.449,0.449,0.449H8.3c0.247,0,0.449-0.202,0.449-0.449C8.749,6.541,8.547,6.339,8.3,6.339\
    \ M18.631,4.543v10.78c0,0.248-0.202,0.45-0.449,0.45H2.011c-0.247,0-0.449-0.202-0.449-0.45V4.543c0-0.247,0.202-0.449,0.449-0.449h16.17C18.429,4.094,18.631,4.296,18.631,4.543\
    \ M17.732,4.993H2.46v9.882h15.272V4.993z M16.371,13.078c0,0.247-0.202,0.449-0.449,0.449H9.646c-0.247,0-0.449-0.202-0.449-0.449c0-1.479,0.883-2.747,2.162-3.299c-0.434-0.418-0.714-1.008-0.714-1.642c0-1.197,0.997-2.246,2.133-2.246s2.134,1.049,2.134,2.246c0,0.634-0.28,1.224-0.714,1.642C15.475,10.331,16.371,11.6,16.371,13.078M11.542,8.137c0,0.622,0.539,1.348,1.235,1.348s1.235-0.726,1.235-1.348c0-0.622-0.539-1.348-1.235-1.348S11.542,7.515,11.542,8.137\
    \ M15.435,12.629c-0.214-1.273-1.323-2.246-2.657-2.246s-2.431,0.973-2.644,2.246H15.435z\"\
    ></path>\n</svg>\nAcquisition</a>\n<a style=\"padding: 5px; line-height: 40px;\"\
    \ href=\"/dashboards-next/ga4::behavior\">\n<svg  style=\"height: 16px; fill: #4285F4;\" class=\"\
    svg-icon\" viewBox=\"0 0 20 20\">\n<path d=\"M17.237,3.056H2.93c-0.694,0-1.263,0.568-1.263,1.263v8.837c0,0.694,0.568,1.263,1.263,1.263h4.629v0.879c-0.015,0.086-0.183,0.306-0.273,0.423c-0.223,0.293-0.455,0.592-0.293,0.92c0.07,0.139,0.226,0.303,0.577,0.303h4.819c0.208,0,0.696,0,0.862-0.379c0.162-0.37-0.124-0.682-0.374-0.955c-0.089-0.097-0.231-0.252-0.268-0.328v-0.862h4.629c0.694,0,1.263-0.568,1.263-1.263V4.319C18.5,3.625,17.932,3.056,17.237,3.056\
    \ M8.053,16.102C8.232,15.862,8.4,15.597,8.4,15.309v-0.89h3.366v0.89c0,0.303,0.211,0.562,0.419,0.793H8.053z\
    \ M17.658,13.156c0,0.228-0.193,0.421-0.421,0.421H2.93c-0.228,0-0.421-0.193-0.421-0.421v-1.263h15.149V13.156z\
    \ M17.658,11.052H2.509V4.319c0-0.228,0.193-0.421,0.421-0.421h14.308c0.228,0,0.421,0.193,0.421,0.421V11.052z\"\
    ></path>\n</svg>\nBehavior</a>\n<a style=\"padding: 5px; line-height: 40px;\"\
    \ href=\"/dashboards-next/ga4::custom_goal_conversions\">\n<svg  style=\"height: 16px; fill: #4285F4;\" class=\"\
    svg-icon\" viewBox=\"0 0 20 20\">\n<path d=\"M15.94,10.179l-2.437-0.325l1.62-7.379c0.047-0.235-0.132-0.458-0.372-0.458H5.25c-0.241,0-0.42,0.223-0.373,0.458l1.634,7.376L4.06,10.179c-0.312,0.041-0.446,0.425-0.214,0.649l2.864,2.759l-0.724,3.947c-0.058,0.315,0.277,0.554,0.559,0.401l3.457-1.916l3.456,1.916c-0.419-0.238,0.56,0.439,0.56-0.401l-0.725-3.947l2.863-2.759C16.388,10.604,16.254,10.22,15.94,10.179M10.381,2.778h3.902l-1.536,6.977L12.036,9.66l-1.655-3.546V2.778z\
    \ M5.717,2.778h3.903v3.335L7.965,9.66L7.268,9.753L5.717,2.778zM12.618,13.182c-0.092,0.088-0.134,0.217-0.11,0.343l0.615,3.356l-2.938-1.629c-0.057-0.03-0.122-0.048-0.184-0.048c-0.063,0-0.128,0.018-0.185,0.048l-2.938,1.629l0.616-3.356c0.022-0.126-0.019-0.255-0.11-0.343l-2.441-2.354l3.329-0.441c0.128-0.017,0.24-0.099,0.295-0.215l1.435-3.073l1.435,3.073c0.055,0.116,0.167,0.198,0.294,0.215l3.329,0.441L12.618,13.182z\"\
    ></path>\n</svg>\nConversions</a>\n<a style=\"padding: 5px; line-height: 40px;\"\
    \ href=\"/dashboards-next/ga4::propensity_model\">\n<svg  style=\"height: 16px; fill: #4285F4;\" class=\"\
    svg-icon\" viewBox=\"0 0 20 20\">\n<path d=\"M15.94,10.179l-2.437-0.325l1.62-7.379c0.047-0.235-0.132-0.458-0.372-0.458H5.25c-0.241,0-0.42,0.223-0.373,0.458l1.634,7.376L4.06,10.179c-0.312,0.041-0.446,0.425-0.214,0.649l2.864,2.759l-0.724,3.947c-0.058,0.315,0.277,0.554,0.559,0.401l3.457-1.916l3.456,1.916c-0.419-0.238,0.56,0.439,0.56-0.401l-0.725-3.947l2.863-2.759C16.388,10.604,16.254,10.22,15.94,10.179M10.381,2.778h3.902l-1.536,6.977L12.036,9.66l-1.655-3.546V2.778z\
    \ M5.717,2.778h3.903v3.335L7.965,9.66L7.268,9.753L5.717,2.778zM12.618,13.182c-0.092,0.088-0.134,0.217-0.11,0.343l0.615,3.356l-2.938-1.629c-0.057-0.03-0.122-0.048-0.184-0.048c-0.063,0-0.128,0.018-0.185,0.048l-2.938,1.629l0.616-3.356c0.022-0.126-0.019-0.255-0.11-0.343l-2.441-2.354l3.329-0.441c0.128-0.017,0.24-0.099,0.295-0.215l1.435-3.073l1.435,3.073c0.055,0.116,0.167,0.198,0.294,0.215l3.329,0.441L12.618,13.182z\"\
    ></path>\n</svg>\nModels</a>\n</nav>\n</div>"
    row: 0
    col: 0
    width: 24
    height: 4
  filters:
  - name: Date
    title: Date
    type: field_filter
    default_value: 14 day
    allow_multiple_values: true
    required: false
    ui_config:
      type: relative_timeframes
      display: inline
      options: []
    model: ga4
    explore: sessions
    listens_to_filters: []
    field: sessions.session_date
  - name: Audience Cohort [Required]
    title: Audience Cohort [Required]
    type: field_filter
    default_value: Channel
    allow_multiple_values: true
    required: true
    ui_config:
      type: dropdown_menu
      display: inline
      options:
      - Channel
      - Medium
      - Source
      - Source Medium
    model: ga4
    explore: sessions
    listens_to_filters: []
    field: sessions.audience_selector
  - name: Country [Optional]
    title: Country [Optional]
    type: field_filter
    default_value: ''
    allow_multiple_values: true
    required: false
    ui_config:
      type: tag_list
      display: popover
      options: []
    model: ga4
    explore: sessions
    listens_to_filters: []
    field: sessions.geo_data_country
  - name: Continent [Optional]
    title: Continent [Optional]
    type: field_filter
    default_value: ''
    allow_multiple_values: true
    required: false
    ui_config:
      type: checkboxes
      display: popover
      options: []
    model: ga4
    explore: sessions
    listens_to_filters: []
    field: sessions.geo_data_continent
  - name: Source [Optional]
    title: Source [Optional]
    type: field_filter
    default_value: ''
    allow_multiple_values: true
    required: false
    ui_config:
      type: tag_list
      display: popover
      options: []
    model: ga4
    explore: sessions
    listens_to_filters: []
    field: sessions.session_attribution_source
  - name: Source / Medium [Optional]
    title: Source / Medium [Optional]
    type: field_filter
    default_value: ''
    allow_multiple_values: true
    required: false
    ui_config:
      type: tag_list
      display: popover
      options: []
    model: ga4
    explore: sessions
    listens_to_filters: []
    field: sessions.session_attribution_source_medium
  - name: Medium [Optional]
    title: Medium [Optional]
    type: field_filter
    default_value: ''
    allow_multiple_values: true
    required: false
    ui_config:
      type: tag_list
      display: popover
      options: []
    model: ga4
    explore: sessions
    listens_to_filters: []
    field: sessions.session_attribution_medium
  - name: Channel [Optional]
    title: Channel [Optional]
    type: field_filter
    default_value: ''
    allow_multiple_values: true
    required: false
    ui_config:
      type: checkboxes
      display: popover
      options: []
    model: ga4
    explore: sessions
    listens_to_filters: []
    field: attribution_sources.attribution_source_channel
