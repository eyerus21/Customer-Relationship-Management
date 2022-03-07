SELECT AVG("weekly_value_sum") as quarterly_avg_value_per_week, AVG("weekly_rfps") as quarterly_avg_rfps_per_week, AVG("weekly_io") as quarterly_avg_io_per_week, AVG("weekly_meeting") as quarterly_avg_meeting_per_week, quarter from (SELECT week,quarter, sum("Deal_Value") as weekly_value_sum, sum("rfp") as weekly_rfps, sum("io") as weekly_io, sum("meeting") as weekly_meeting  from (select "Deal_created_at", extract(year from "Deal_created_at"::date) || '-' || extract(quarter from "Deal_created_at"::date) as quarter, extract(week from CAST("Deal_created_at" AS DATE)) as week, "Deal_Value", (SELECT COUNT(*) FROM sales_table WHERE "Deal_Stage" = 'RFP') AS rfp,  (SELECT COUNT(*) FROM sales_table WHERE "Deal_Stage" = 'IO Sent') AS io, (SELECT COUNT(*) FROM sales_table WHERE "Deal_Stage" = 'Meeting') as meeting  from sales_table) as new_view GROUP BY quarter, week) as weekly group by quarter order by quarter