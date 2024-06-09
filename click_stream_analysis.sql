WITH limited_clicks AS (
    select
        *
    from
        assignment.public.assignment_click_stream
    limit
        1000000
)
select browser, count(distinct(user_id)) uniuqe_users_count from limited_clicks
where domain_label = 'youtube' and browser is not null
group by (browser)
order by uniuqe_users_count DESC 