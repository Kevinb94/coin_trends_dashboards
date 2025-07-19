CREATE TABLE api_requests (
    id SERIAL PRIMARY KEY,
    api_name TEXT NOT NULL,
    requested_symbols TEXT NOT NULL,
    request_time TIMESTAMP DEFAULT now(),
    status TEXT NOT NULL
);
