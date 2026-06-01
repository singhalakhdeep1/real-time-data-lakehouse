select 'signup_completed' as event_type, 'u_1' as user_id, 's_1' as session_id, '2026-06-01T10:00:00Z' as occurred_at
union all
select 'checkout_started', 'u_1', 's_2', '2026-06-01T10:05:00Z'
union all
select 'payment_succeeded', 'u_1', 's_2', '2026-06-01T10:06:00Z'
