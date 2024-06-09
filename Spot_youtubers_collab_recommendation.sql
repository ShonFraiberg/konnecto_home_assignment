-- video analysis starts here
WITH limited_videos AS (
    -- get limited videos
    SELECT
        *
    FROM
        assignment.public.youtube_video_data
    WHERE
        published_at >= DATEADD(YEAR, -1, CURRENT_DATE())
    LIMIT 1000000
),
most_recent_video_record AS (
    -- get the most recent youtube channel records from the db
    SELECT
        video_id AS v_id,
        channel_id AS channel_id_1,
        MAX(updated_time) AS most_recent_insertion_date_video
    FROM
        limited_videos
    GROUP BY
        (channel_id_1, v_id)
),
most_recent_video_data AS (
    -- get the most recent youtube channel records from the db
    SELECT
        *
    FROM
        limited_videos lv
        JOIN most_recent_video_record ri ON lv.channel_id = ri.channel_id_1
        AND lv.updated_time = ri.most_recent_insertion_date_video
),
flattened_table AS (
    -- flatten the tags columns
    SELECT
        t.*,
        f.value AS array_elem
    FROM
        most_recent_video_data t,
        LATERAL FLATTEN(INPUT => t.tags) f
),
sumed_pets_video_views AS (
    SELECT
        DISTINCT channel_id AS channel_id_2,
        sum(view_count) AS total_pets_video_views
    FROM
        flattened_table
    GROUP BY
        channel_id_2
),
final_video_table AS (
    SELECT
        *
    FROM
        flattened_table ft
        JOIN sumed_pets_video_views spvv ON ft.channel_id = spvv.channel_id_2
    WHERE
        view_count > 0
),
-- video analysis ends here
-- channel analysis starts here
most_recent_channel_record AS (
    -- get the most recent youtube channel records from the db
    SELECT
        channel_id,
        MAX(updated_time) AS most_recent_insertion_date
    FROM
        assignment.public.youtube_channel_data
    GROUP BY
        (channel_id)
),
limited_channels AS (
    -- join the relent data for the most updated records
    SELECT
        ycd.title,
        ycd.CUSTOM_URL,
        ycd.channel_id,
        ri.most_recent_insertion_date,
        ycd.view_count,
        ycd.subscriber_count
    FROM
        assignment.public.youtube_channel_data ycd
        JOIN most_recent_channel_record ri ON ycd.channel_id = ri.channel_id
        AND ycd.updated_time = ri.most_recent_insertion_date
)
-- channel analysis ends here
SELECT
    -- final selection and recommendation
    DISTINCT (limited_channels.channel_id),
    limited_channels.TITLE,
    limited_channels.CUSTOM_URL,
    final_video_table.total_pets_video_views / limited_channels.view_count as channel_pets_oriented_score,
    limited_channels.view_count as total_channel_views,
    final_video_table.total_pets_video_views,
    limited_channels.subscriber_count as subscribers
FROM
    final_video_table
    JOIN limited_channels ON final_video_table.channel_id = limited_channels.channel_id
WHERE
    (
        array_elem LIKE 'dog'
        OR array_elem LIKE 'kitten'
        OR array_elem LIKE 'puppy'
        OR array_elem LIKE 'cat'
    )
    AND channel_pets_oriented_score > 0.15 -- threshold
    AND subscribers > 20000 -- threshold
    AND total_channel_views > 0
ORDER BY
    channel_pets_oriented_score DESC
