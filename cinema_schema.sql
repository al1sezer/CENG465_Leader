-- ============================================================================
-- CENG 465 - Cinema Ticket Reservation System
-- Single-Leader Replication Schema
-- ============================================================================
-- This schema is designed in accordance with the assignment requirements:
--   - Each table contains version, last_updated, and operation_id fields.
--   - Relational structure is established with Foreign Key constraints.
--   - Replication log table tracks write operations.
-- ============================================================================

-- Required extension
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ============================================================================
-- CLEANUP EXISTING TABLES (in order of FK dependencies)
-- ============================================================================
DROP TABLE IF EXISTS reservations CASCADE;
DROP TABLE IF EXISTS seats CASCADE;
DROP TABLE IF EXISTS showtimes CASCADE;
DROP TABLE IF EXISTS movies CASCADE;
DROP TABLE IF EXISTS halls CASCADE;
DROP TABLE IF EXISTS replication_log CASCADE;

-- ============================================================================
-- TABLE 1: movies (Movies)
-- ============================================================================
CREATE TABLE movies (
    id          SERIAL PRIMARY KEY,
    title       VARCHAR(255) NOT NULL,
    genre       VARCHAR(100) NOT NULL,
    duration_min INTEGER NOT NULL,
    version     INTEGER DEFAULT 1,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    operation_id UUID DEFAULT uuid_generate_v4()
);

-- ============================================================================
-- TABLE 2: halls (Halls)
-- ============================================================================
CREATE TABLE halls (
    id          SERIAL PRIMARY KEY,
    name        VARCHAR(100) NOT NULL,
    capacity    INTEGER NOT NULL,
    version     INTEGER DEFAULT 1,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    operation_id UUID DEFAULT uuid_generate_v4()
);

-- ============================================================================
-- TABLE 3: seats (Seats)
-- ============================================================================
CREATE TABLE seats (
    id          SERIAL PRIMARY KEY,
    hall_id     INTEGER NOT NULL REFERENCES halls(id) ON DELETE CASCADE,
    row_label   CHAR(1) NOT NULL,
    seat_number INTEGER NOT NULL,
    version     INTEGER DEFAULT 1,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    operation_id UUID DEFAULT uuid_generate_v4(),
    UNIQUE(hall_id, row_label, seat_number)
);

-- ============================================================================
-- TABLE 4: showtimes (Showtimes)
-- ============================================================================
CREATE TABLE showtimes (
    id          SERIAL PRIMARY KEY,
    movie_id    INTEGER NOT NULL REFERENCES movies(id) ON DELETE CASCADE,
    hall_id     INTEGER NOT NULL REFERENCES halls(id) ON DELETE CASCADE,
    show_date   DATE NOT NULL,
    show_time   TIME NOT NULL,
    version     INTEGER DEFAULT 1,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    operation_id UUID DEFAULT uuid_generate_v4()
);

-- ============================================================================
-- TABLE 5: reservations (Reservations)
-- ============================================================================
CREATE TABLE reservations (
    id            SERIAL PRIMARY KEY,
    showtime_id   INTEGER NOT NULL REFERENCES showtimes(id) ON DELETE CASCADE,
    seat_id       INTEGER NOT NULL REFERENCES seats(id) ON DELETE CASCADE,
    customer_name VARCHAR(255) NOT NULL,
    status        VARCHAR(20) DEFAULT 'reserved' CHECK (status IN ('reserved', 'cancelled')),
    version       INTEGER DEFAULT 1,
    last_updated  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    operation_id  UUID DEFAULT uuid_generate_v4()
);

-- ============================================================================
-- TABLE 6: replication_log (Replication Log)
-- ============================================================================
CREATE TABLE replication_log (
    log_id          SERIAL PRIMARY KEY,
    operation_type  VARCHAR(10) NOT NULL CHECK (operation_type IN ('INSERT', 'UPDATE', 'DELETE')),
    table_name      VARCHAR(50) NOT NULL,
    record_id       INTEGER NOT NULL,
    details         JSONB DEFAULT '{}',
    timestamp       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    node            VARCHAR(20) NOT NULL DEFAULT 'Leader'
);

-- ============================================================================
-- INDEXES (For performance)
-- ============================================================================
CREATE INDEX idx_seats_hall ON seats(hall_id);
CREATE INDEX idx_showtimes_movie ON showtimes(movie_id);
CREATE INDEX idx_showtimes_hall ON showtimes(hall_id);
CREATE INDEX idx_reservations_showtime ON reservations(showtime_id);
CREATE INDEX idx_reservations_seat ON reservations(seat_id);
CREATE INDEX idx_replication_log_timestamp ON replication_log(timestamp);
CREATE INDEX idx_replication_log_table ON replication_log(table_name);

-- ============================================================================
-- SEED DATA (Initial Data)
-- ============================================================================

-- 3 Movies
INSERT INTO movies (title, genre, duration_min) VALUES
    ('Inception',       'Sci-Fi',    148),
    ('The Dark Knight',  'Action',    152),
    ('Interstellar',     'Sci-Fi',    169);

-- 2 Halls
INSERT INTO halls (name, capacity) VALUES
    ('Hall A', 25),
    ('Hall B', 20);

-- Hall A seats: 5 rows (A-E) x 5 seats = 25 seats
INSERT INTO seats (hall_id, row_label, seat_number)
SELECT 1, chr(64 + row_num), seat_num
FROM generate_series(1, 5) AS row_num,
     generate_series(1, 5) AS seat_num;

-- Hall B seats: 4 rows (A-D) x 5 seats = 20 seats
INSERT INTO seats (hall_id, row_label, seat_number)
SELECT 2, chr(64 + row_num), seat_num
FROM generate_series(1, 4) AS row_num,
     generate_series(1, 5) AS seat_num;

-- [NO INITIAL SHOWTIMES AND RESERVATIONS SEED - TABLES KEPT EMPTY BY REQUEST]

-- Log records for initial data
INSERT INTO replication_log (operation_type, table_name, record_id, details, node) VALUES
    ('INSERT', 'movies', 1, '{"title": "Inception"}', 'Leader'),
    ('INSERT', 'movies', 2, '{"title": "The Dark Knight"}', 'Leader'),
    ('INSERT', 'movies', 3, '{"title": "Interstellar"}', 'Leader'),
    ('INSERT', 'halls', 1, '{"name": "Hall A", "capacity": 25}', 'Leader'),
    ('INSERT', 'halls', 2, '{"name": "Hall B", "capacity": 20}', 'Leader');
