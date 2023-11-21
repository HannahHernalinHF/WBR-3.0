with cte1 as (
SELECT
        mm.dc_name,
        e.country_group,
        mm.metric as metric_name,
        internal_metric_id as metric_id,
        hellofresh_week,
        CASE
            WHEN MIN(metric_type) = 'proportional'
                    THEN round(sum(numerator_value)/ sum(denominator_value),5)
            ELSE  sum(numerator_value)
        END AS metric_value,
        bridge,
        action,
        NULL as target
FROM uploads_staging.isa__br__wbr_metric_manual mm
        JOIN materialized_views.isa__br_entity_dimension e
                ON mm.dc_name = e.dc_name
--WHERE requirement = 'All' and metric_group != 'People/WFM'
GROUP BY
        mm.dc_name,
        e.country_group,
        mm.metric,
        internal_metric_id,
        hellofresh_week,
        bridge,
        action,
        target),
cte2 as (
SELECT
        dv.dc_name,
        dv.country_group,
        dv.metric_name,
        dv.metric_id,
        dv.hellofresh_week,
        dv.nominator_value/dv.denominator_value AS metric_value,
        mm.bridge,
        mm.action,
        dv.target as target
 FROM materialized_views.isa__wbr_dashboard_view dv
        JOIN uploads_staging.isa__br__wbr_metric_manual mm
        ON dv.dc_name = mm.dc_name
                --AND dv.metric_id = mm.internal_metric_id
                AND dv.hellofresh_week = mm.hellofresh_week
--WHERE requirement = 'Bridge & Action' and dv.hellofresh_week >= '2023-W39'
GROUP BY
        dv.dc_name,
        dv.country_group,
        dv.metric_id,
        dv.metric_name,
        dv.hellofresh_week,
        --dv.metric_value,
        dv.nominator_value,
        dv.denominator_value,
        mm.bridge,
        mm.action,
        dv.target
)


, final AS (
SELECT *
  FROM cte1
UNION ALL
SELECT *
  FROM cte2

)

SELECT *
FROM final