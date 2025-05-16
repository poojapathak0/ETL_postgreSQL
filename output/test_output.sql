CREATE TABLE `test_table` (
    `id` INT,
    `name` VARCHAR(16),
    `email` VARCHAR(32),
    `age` INT,
    `address` JSON,
    `created_at` VARCHAR(38),
    `is_active` INT
);


-- Data insertion statements
INSERT INTO `test_table` (`id`, `name`, `email`, `age`, `address`, `created_at`, `is_active`) VALUES
(1, 'John Doe', 'john@example.com', 35, '{"street": "123 Main St", "city": "New York", "zip": "10001"}', '2023-01-15T10:30:00', True),
(2, 'Jane Smith', 'jane@example.com', 28, '{"street": "456 Park Ave", "city": "Boston", "zip": "02108"}', '2023-02-20T14:15:00', True),
(3, 'Bob Johnson', 'bob@example.com', 42, '{"street": "789 Oak Dr", "city": "San Francisco", "zip": "94102"}', '2023-03-10T09:45:00', False);
