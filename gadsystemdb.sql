-- phpMyAdmin SQL Dump
-- version 5.2.0
-- https://www.phpmyadmin.net/
--
-- Host: 127.0.0.1
-- Generation Time: Dec 21, 2023 at 02:06 AM
-- Server version: 10.4.27-MariaDB
-- PHP Version: 8.2.0

SET SQL_MODE = "NO_AUTO_VALUE_ON_ZERO";
START TRANSACTION;
SET time_zone = "+00:00";


/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8mb4 */;

--
-- Database: `gadsystemdb`
--

-- --------------------------------------------------------

--
-- Table structure for table `attendance`
--

CREATE TABLE `attendance` (
  `id` int(11) NOT NULL,
  `category` varchar(255) NOT NULL,
  `count` int(11) NOT NULL,
  `date_filed` datetime NOT NULL,
  `event_name` varchar(255) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

--
-- Dumping data for table `attendance`
--

INSERT INTO `attendance` (`id`, `category`, `count`, `date_filed`, `event_name`) VALUES
(1, 'Female', 578, '2023-06-14 16:00:00', 'nutrition month'),
(2, 'Male', 229, '2023-06-14 16:00:00', 'nutrition month'),
(3, 'Prefer not to say', 2, '2023-06-14 16:00:00', 'nutrition month'),
(4, 'Other', 1, '2023-06-14 16:00:00', 'nutrition month'),
(5, 'Female', 578, '2023-12-06 16:00:00', 'Kaibigan Party'),
(6, 'Male', 229, '2023-12-06 16:00:00', 'Kaibigan Party'),
(7, 'Prefer not to say', 2, '2023-12-06 16:00:00', 'Kaibigan Party'),
(8, 'Other', 1, '2023-12-06 16:00:00', 'Kaibigan Party'),
(9, 'Female', 578, '2023-08-08 16:00:00', 'one one one'),
(10, 'Male', 229, '2023-08-08 16:00:00', 'one one one'),
(11, 'Prefer not to say', 2, '2023-08-08 16:00:00', 'one one one'),
(12, 'Other', 1, '2023-08-08 16:00:00', 'one one one');

-- --------------------------------------------------------

--
-- Table structure for table `events`
--

CREATE TABLE `events` (
  `id` int(11) NOT NULL,
  `event_name` varchar(255) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

--
-- Dumping data for table `events`
--

INSERT INTO `events` (`id`, `event_name`) VALUES
(1, 'one one one'),
(2, 'another one'),
(3, 'Christmas Party'),
(4, 'Buwan ng Wika'),
(5, 'Sheesh Party'),
(6, 'event party');

-- --------------------------------------------------------

--
-- Table structure for table `history`
--

CREATE TABLE `history` (
  `id` int(11) NOT NULL,
  `Date` date NOT NULL,
  `Event` text NOT NULL,
  `Total Attendance` int(11) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

--
-- Indexes for dumped tables
--

--
-- Indexes for table `attendance`
--
ALTER TABLE `attendance`
  ADD PRIMARY KEY (`id`);

--
-- Indexes for table `events`
--
ALTER TABLE `events`
  ADD PRIMARY KEY (`id`);

--
-- Indexes for table `history`
--
ALTER TABLE `history`
  ADD PRIMARY KEY (`id`);

--
-- AUTO_INCREMENT for dumped tables
--

--
-- AUTO_INCREMENT for table `attendance`
--
ALTER TABLE `attendance`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=13;

--
-- AUTO_INCREMENT for table `events`
--
ALTER TABLE `events`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=11;

--
-- AUTO_INCREMENT for table `history`
--
ALTER TABLE `history`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT;
COMMIT;

/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
