-- Reference tables for CSV seeds

-- up
CREATE TABLE public.task_statuses (
    code TEXT PRIMARY KEY,
    label TEXT NOT NULL,
    sort_order INTEGER NOT NULL,
    is_terminal BOOLEAN NOT NULL DEFAULT false
);

CREATE TABLE public.priorities (
    code TEXT PRIMARY KEY,
    label TEXT NOT NULL,
    sort_order INTEGER NOT NULL,
    color TEXT
);

-- down
DROP TABLE IF EXISTS public.priorities;
DROP TABLE IF EXISTS public.task_statuses;
