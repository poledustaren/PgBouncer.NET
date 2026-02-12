-- Drop existing battle_test databases
DROP DATABASE IF EXISTS battle_test_001;
DROP DATABASE IF EXISTS battle_test_002;
DROP DATABASE IF EXISTS battle_test_003;
DROP DATABASE IF EXISTS battle_test_004;
DROP DATABASE IF EXISTS battle_test_005;
DROP DATABASE IF EXISTS battle_test_006;
DROP DATABASE IF EXISTS battle_test_007;
DROP DATABASE IF EXISTS battle_test_008;
DROP DATABASE IF EXISTS battle_test_009;
DROP DATABASE IF EXISTS battle_test_010;
DROP DATABASE IF EXISTS battle_test_011;
DROP DATABASE IF EXISTS battle_test_012;
DROP DATABASE IF EXISTS battle_test_013;
DROP DATABASE IF EXISTS battle_test_014;
DROP DATABASE IF EXISTS battle_test_015;
DROP DATABASE IF EXISTS battle_test_016;
DROP DATABASE IF EXISTS battle_test_017;
DROP DATABASE IF EXISTS battle_test_018;
DROP DATABASE IF EXISTS battle_test_019;
DROP DATABASE IF EXISTS battle_test_020;

-- Create battle_test databases
CREATE DATABASE battle_test_001;
CREATE DATABASE battle_test_002;
CREATE DATABASE battle_test_003;
CREATE DATABASE battle_test_004;
CREATE DATABASE battle_test_005;
CREATE DATABASE battle_test_006;
CREATE DATABASE battle_test_007;
CREATE DATABASE battle_test_008;
CREATE DATABASE battle_test_009;
CREATE DATABASE battle_test_010;
CREATE DATABASE battle_test_011;
CREATE DATABASE battle_test_012;
CREATE DATABASE battle_test_013;
CREATE DATABASE battle_test_014;
CREATE DATABASE battle_test_015;
CREATE DATABASE battle_test_016;
CREATE DATABASE battle_test_017;
CREATE DATABASE battle_test_018;
CREATE DATABASE battle_test_019;
CREATE DATABASE battle_test_020;

-- Output created databases
SELECT datname FROM pg_database WHERE datname LIKE 'battle_test_%' ORDER BY datname;
