SELECT AVG("weekly_value_sum") as quarterly_avg_value_per_week,
AVG("weekly_rfps") as quarterly_avg_rfps_per_week, 
AVG("weekly_contacted") as quarterly_avg_rfps_per_week,
quarter from (SELECT week,quarter, sum("Deal_Value") as weekly_value_sum, sum("rfp") as weekly_rfps, sum("contacted") as weekly_contacted  
from (select "Deal_created_at", extract(year from "Deal_created_at"::date)  '-'  extract(quarter from "Deal_created_at"::date) as quarter, 
extract(week from CAST("Deal_created_at" AS DATE)) as week,
"Deal_Value", (SELECT COUNT(*) FROM {{ref('raw_source')}} WHERE "Deal_Stage" = 'RFP') AS rfp,
(SELECT COUNT(*) FROM {{ref('raw_source')}} WHERE "Deal_Stage" = 'Contacted') as contacted  from {{ref('raw_source')}}) as new_view GROUP BY quarter, week) as weekly group by quarter order by quarter;