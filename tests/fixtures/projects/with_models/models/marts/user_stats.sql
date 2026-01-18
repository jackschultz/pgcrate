-- materialized: table
-- deps: public.users, public.posts
-- unique_key: user_id

SELECT
    u.id AS user_id,
    u.email,
    u.name,
    COUNT(p.id) AS post_count,
    MAX(p.created_at) AS last_post_at
FROM public.users u
LEFT JOIN public.posts p ON p.user_id = u.id
GROUP BY u.id, u.email, u.name
