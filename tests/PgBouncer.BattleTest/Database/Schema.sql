-- Схема для battle test
-- Выполняется для каждой из 20 баз

-- Таблица для тестовых записей
CREATE TABLE IF NOT EXISTS battle_records (
    id UUID PRIMARY KEY,
    session_id UUID NOT NULL,
    wave_number INT NOT NULL,
    operation_number INT NOT NULL,
    client_id INT NOT NULL,
    database_id INT NOT NULL,
    payload TEXT NOT NULL,
    checksum VARCHAR(64) NOT NULL,
    operation_type VARCHAR(10) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP,
    deleted_at TIMESTAMP,
    is_deleted BOOLEAN DEFAULT FALSE
);

-- Индексы для проверки консистентности
CREATE INDEX idx_session_wave ON battle_records(session_id, wave_number);
CREATE INDEX idx_checksum ON battle_records(checksum);
CREATE INDEX idx_deleted ON battle_records(is_deleted) WHERE is_deleted = FALSE;
CREATE INDEX idx_wave_client ON battle_records(wave_number, client_id);

-- Таблица для метрик
CREATE TABLE IF NOT EXISTS battle_metrics (
    id SERIAL PRIMARY KEY,
    wave_number INT NOT NULL,
    client_id INT NOT NULL,
    operation_type VARCHAR(10) NOT NULL,
    latency_ms DECIMAL(10,3) NOT NULL,
    success BOOLEAN NOT NULL,
    error_message TEXT,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_metrics_wave ON battle_metrics(wave_number);
CREATE INDEX idx_metrics_client ON battle_metrics(client_id);

-- Функция для проверки консистентности
CREATE OR REPLACE FUNCTION check_wave_consistency(p_wave_number INT)
RETURNS TABLE (
    check_type VARCHAR(50),
    error_count BIGINT,
    details TEXT
) AS $$
BEGIN
    -- Проверка 1: Несоответствие checksum
    RETURN QUERY
    SELECT 
        'Invalid Checksum'::VARCHAR(50),
        COUNT(*)::BIGINT,
        'Records with mismatching checksum'::TEXT
    FROM battle_records
    WHERE wave_number = p_wave_number
    AND checksum != encode(digest(payload, 'sha256'), 'hex');
    
    -- Проверка 2: Не удаленные записи после DELETE
    RETURN QUERY
    SELECT 
        'Ghost Records'::VARCHAR(50),
        COUNT(*)::BIGINT,
        'DELETE operations with is_deleted = FALSE'::TEXT
    FROM battle_records
    WHERE wave_number = p_wave_number
    AND operation_type = 'DELETE'
    AND is_deleted = FALSE;
    
    -- Проверка 3: Потерянные INSERT (нет UPDATE/DELETE)
    RETURN QUERY
    SELECT 
        'Orphaned Inserts'::VARCHAR(50),
        COUNT(*)::BIGINT,
        'INSERT without subsequent UPDATE/DELETE'::TEXT
    FROM battle_records r1
    WHERE wave_number = p_wave_number
    AND operation_type = 'INSERT'
    AND NOT EXISTS (
        SELECT 1 FROM battle_records r2
        WHERE r2.session_id = r1.session_id
        AND r2.wave_number = r1.wave_number
        AND r2.operation_type IN ('UPDATE', 'DELETE')
    );
END;
$$ LANGUAGE plpgsql;
