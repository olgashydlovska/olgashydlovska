-- Monthly Recurring Revenue (MRR)
SELECT DATE_TRUNC('month', gp.payment_date) AS month,
       SUM(gp.revenue_amount_usd) AS mrr
FROM games_payments gp
JOIN games_paid_users gpu ON gp.user_id = gpu.user_id AND gp.game_name = gpu.game_name
GROUP BY month;

-- Paid users
SELECT
    COUNT(DISTINCT user_id) AS paid_users,
    DATE_TRUNC('month', payment_date) AS month
FROM
    games_payments
GROUP BY month;

-- Average Revenue Per Paid User (ARPPU)
SELECT
    SUM(revenue_amount_usd) / COUNT(DISTINCT user_id) AS arppu,
    DATE_TRUNC('month', payment_date) AS month
FROM
    games_payments
GROUP BY month;

-- Churned users
WITH monthly_payments AS (
    SELECT
        user_id,
        DATE_TRUNC('month', payment_date) AS payment_month
    FROM
        games_payments
    GROUP BY
        user_id, payment_month
), previous_month AS (
    SELECT
        user_id,
        payment_month
    FROM
        monthly_payments
), current_month AS (
    SELECT
        user_id,
        payment_month
    FROM
        monthly_payments
), churned_users AS (
    SELECT
        p.user_id,
        p.payment_month AS previous_payment_month,
        c.payment_month AS current_payment_month
    FROM
        previous_month p
    LEFT JOIN current_month c ON p.user_id = c.user_id
        AND c.payment_month = (p.payment_month + INTERVAL '1 month')
    WHERE
        c.user_id IS NULL
)
SELECT
    previous_payment_month,
    COUNT(DISTINCT user_id) AS churned_users_count
FROM
    churned_users
GROUP BY
    previous_payment_month
ORDER BY
    previous_payment_month;
    
-- Expansion MRR and Contraction MRR
WITH monthly_revenue AS (
  SELECT
    user_id,
    game_name,
    DATE_TRUNC('month', payment_date) AS payment_month,
    SUM(revenue_amount_usd) AS monthly_revenue
  FROM
    games_payments
  GROUP BY
    user_id, game_name, DATE_TRUNC('month', payment_date)
), revenue_changes AS (
  SELECT
    user_id,
    game_name,
    payment_month,
    monthly_revenue,
    LAG(monthly_revenue) OVER (PARTITION BY user_id, game_name ORDER BY payment_month) AS prev_month_revenue
  FROM
    monthly_revenue
), mrr_changes AS (
  SELECT
    payment_month,
    CASE
      WHEN monthly_revenue > prev_month_revenue THEN monthly_revenue - prev_month_revenue
      ELSE 0
    END AS expansion_mrr,
    CASE
      WHEN monthly_revenue < prev_month_revenue THEN prev_month_revenue - monthly_revenue
      ELSE 0
    END AS contraction_mrr
  FROM
    revenue_changes
)
SELECT
  payment_month,
  SUM(expansion_mrr) AS total_expansion_mrr,
  SUM(contraction_mrr) AS total_contraction_mrr
FROM
  mrr_changes
GROUP BY
  payment_month
ORDER BY
  payment_month;

   
    SELECT 
        gp.user_id AS user_id,
        gp.game_name AS game_name,
        gp.payment_date AS payment_date,
        gp.revenue_amount_usd AS revenue_amount_usd,
        gpu.language AS language,
        gpu.has_older_device_model AS has_older_device_model,
        gpu.age AS age
    FROM 
        games_payments gp 
    LEFT JOIN 
        games_paid_users gpu ON gp.user_id = gpu.user_id AND gp.game_name = gpu.game_name
),
monthly_mrr AS (
    SELECT
        user_id,
        game_name,
        EXTRACT(YEAR FROM payment_date) AS year,
        EXTRACT(MONTH FROM payment_date) AS month,
        SUM(revenue_amount_usd) AS mrr
    FROM
        merge_data
    GROUP BY
        user_id, game_name, EXTRACT(YEAR FROM payment_date), EXTRACT(MONTH FROM payment_date)
),
mrr_changes AS (
    SELECT
        user_id,
        game_name,
        year,
        month,
        mrr,
        LAG(mrr) OVER (PARTITION BY user_id, game_name ORDER BY year, month) AS prev_mrr
    FROM
        monthly_mrr
)
SELECT
    user_id,
    game_name,
    year,
    month,
    CASE WHEN mrr > prev_mrr THEN mrr - prev_mrr ELSE 0 END AS mrr_expansion,
    CASE WHEN mrr < prev_mrr THEN prev_mrr - mrr ELSE 0 END AS mrr_contraction
FROM
    mrr_changes
WHERE
    mrr_expansion > 0 OR mrr_contraction > 0;

   
-- Customer Lifetime (LT)
WITH user_payments AS (
    SELECT
        user_id,
        MIN(payment_date) AS first_payment_date,
        MAX(payment_date) AS last_payment_date
    FROM
        games_payments
    GROUP BY
        user_id
),
user_lifetime AS (
    SELECT
        user_id,
        (last_payment_date - first_payment_date) AS lifetime
    FROM
        user_payments
)
SELECT
    AVG(lifetime) AS average_lifetime,
    user_id
FROM
    user_lifetime
GROUP BY user_id;

-- Customer Lifetime Value (LTV)
WITH total_revenue_per_user AS (
    SELECT
        user_id,
        SUM(revenue_amount_usd) AS total_revenue
    FROM
        games_payments
    GROUP BY
        user_id
)
SELECT
    AVG(total_revenue) AS average_ltv, user_id
FROM
    total_revenue_per_user
GROUP BY user_id;

-- Merge_data
WITH user_first_payment AS (
    SELECT
        gp.user_id,
        MIN(DATE_TRUNC('month', gp.payment_date)) AS first_payment_month
    FROM
        games_payments gp
    GROUP BY
        gp.user_id
),
user_payments_with_previous AS (
    SELECT
        gp.user_id,
        DATE_TRUNC('month', gp.payment_date) AS payment_month,
        LAG(DATE_TRUNC('month', gp.payment_date)) OVER(PARTITION BY gp.user_id ORDER BY gp.payment_date) AS previous_payment_month
    FROM
        games_payments gp
)
SELECT
    u.user_id,
    u.game_name,
    u.language,
    u.has_older_device_model,
    u.age,
    gp.payment_date, 
    f.first_payment_month,
    p.payment_month,
    p.previous_payment_month,
    gp.revenue_amount_usd
FROM
    games_paid_users u
INNER JOIN
    user_first_payment f ON u.user_id = f.user_id
INNER JOIN
    user_payments_with_previous p ON u.user_id = p.user_id
INNER JOIN
    games_payments gp ON u.user_id = gp.user_id AND DATE_TRUNC('month', gp.payment_date) = p.payment_month
ORDER BY
    u.user_id, p.payment_month;

