WITH customer_data AS (
    SELECT
        *,
        LAG(first_name) OVER window_spec AS prev_first_name,
        LAG(last_name) OVER window_spec AS prev_last_name,
        LAG(email) OVER window_spec AS prev_email
    FROM
        customer_raw
    WINDOW
        window_spec AS (PARTITION BY customer_id ORDER BY batch_date)
),
with_start_end_dates AS (
    SELECT
        *,
        FIRST_VALUE(batch_date) OVER window_spec AS start_date,
        LEAD(batch_date, 1, '9999-12-31'::DATE) OVER window_spec AS end_date
    FROM
        customer_data
    WINDOW
        window_spec AS (PARTITION BY customer_id ORDER BY batch_date)
),
change_detected AS (
    SELECT
        *,
        CASE
            WHEN first_name <> prev_first_name OR last_name <> prev_last_name OR email <> prev_email THEN 1
            ELSE 0
        END AS change_detected
    FROM
        with_start_end_dates
)
SELECT customer_id,first_name,last_name, email, batch_date,create_date,update_date,create_user,update_user, start_date, end_date
FROM change_detected
WHERE change_detected = 1 OR start_date = batch_date
