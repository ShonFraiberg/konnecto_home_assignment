-- video starts here
WITH limited_videos AS (
    -- Get the video data which pertaining to pets and lifestyle from the last year
    SELECT
        *
    FROM
        assignment.public.youtube_video_data
    WHERE
        published_at >= DATEADD(YEAR, -1, CURRENT_DATE())
        AND (
            ARRAY_TO_STRING(topic_categories, ',') ILIKE '%pet%'
            AND ARRAY_TO_STRING(topic_categories, ',') ILIKE '%lifestyle%'
        )
        AND (
            description NOT LIKE '%rescue%'
            AND description NOT LIKE '%shelter%'
        )
        AND TAGS IS NOT NULL
        -- limit 100000 -- uncomment if a sample needed
),
most_recent_video_record AS (
    -- Get the most recent video record in each channel by video_id and channel_id
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
    -- Get the data regarding the most recent video record from "most_recent_video_record" table
    SELECT
        *
    FROM
        limited_videos lv
        JOIN most_recent_video_record ri ON lv.channel_id = ri.channel_id_1
        AND lv.updated_time = ri.most_recent_insertion_date_video
),
flattened_table AS (
    -- flatten the tags columns and derive the relevant data for the insurence company
    SELECT
        mrvd.*,
        f.value AS array_elem
    FROM
        most_recent_video_data mrvd,
        LATERAL FLATTEN(INPUT => mrvd.tags) f
    WHERE
        array_elem IN (
            'cat',
            'cats',
            'dog',
            'dogs',
            'kitten',
            'kittens',
            'puppy',
            'puppies'
        )
),
sumed_pets_video_views AS (
    -- Sum the views per channel for the relevant videos records only
    SELECT
        channel_id AS channel_id_2,
        SUM(view_count) AS total_pets_views
    FROM
        (
            SELECT
                DISTINCT channel_id,
                video_id,
                view_count
            FROM
                flattened_table
        ) AS unique_videos
    GROUP BY
        channel_id_2
),
final_video_table AS (
    -- Create the final video table after analysis
    SELECT
        *
    FROM
        flattened_table ft
        JOIN sumed_pets_video_views spvv ON ft.channel_id = spvv.channel_id_2
    WHERE
        view_count > 0
        AND made_for_kids != TRUE
),
-- video ends here
-- channel starts here
most_recent_channel_record AS (
    -- Get the most recent channel record
    SELECT
        channel_id,
        MAX(updated_time) AS most_recent_insertion_date
    FROM
        assignment.public.youtube_channel_data
    GROUP BY
        (channel_id)
),
limited_channels AS (
    -- Get the data regarding the most recent video record from "most_recent_channel_record" table
    SELECT
        ycd.channel_id,
        ycd.custom_url,
        ycd.title,
        ri.most_recent_insertion_date,
        ycd.view_count,
        ycd.subscriber_count
    FROM
        assignment.public.youtube_channel_data ycd
        JOIN most_recent_channel_record ri ON ycd.channel_id = ri.channel_id
        AND ycd.updated_time = ri.most_recent_insertion_date
    WHERE
        (
            ycd.description NOT LIKE '%rescue%'
            AND ycd.description NOT LIKE '%shelter%'
        )
)
SELECT
    -- Get the youtube channels for recommendation
    DISTINCT (limited_channels.channel_id),
    limited_channels.title,
    limited_channels.custom_url,
    final_video_table.total_pets_views / limited_channels.view_count AS channel_pets_oriented_score,
    limited_channels.view_count AS total_channel_views,
    final_video_table.total_pets_views,
    limited_channels.subscriber_count AS subscribers
FROM
    final_video_table
    JOIN limited_channels ON final_video_table.channel_id = limited_channels.channel_id
WHERE
    channel_pets_oriented_score > 0.10 -- threshold
    AND subscribers > 20000 -- threshold
    AND total_channel_views > 0
    AND limited_channels.title NOT LIKE '%Rescue%' -- not relevant
    AND limited_channels.title NOT LIKE '%Shelter%' -- not relevant
ORDER BY
    subscribers DESC,
    channel_pets_oriented_score DESC
