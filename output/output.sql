CREATE TABLE `output` (
    `personid` INT,
    `firstname` VARCHAR(10),
    `lastname` VARCHAR(12),
    `address` VARCHAR(4),
    `city` VARCHAR(18),
    `contact_no` VARCHAR(255),
    `salary` VARCHAR(255)
);


-- Data insertion statements
INSERT INTO `output` (`personid`, `firstname`, `lastname`, `address`, `city`, `contact_no`, `salary`) VALUES
(20, 'pooja', 'pathak', 'ku', 'dhulikhel', NULL, NULL),
(21, 'prayxya', 'pathak', 'LA', 'USA', NULL, 1500000);
