create database if not exists cs122a;
use cs122a;
CREATE TABLE `person` (
  `card_id` int(11) NOT NULL,
  `name` varchar(255) NOT NULL,
  `dob` date NOT NULL,
  `gender` char(1) NOT NULL,
  PRIMARY KEY (`card_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

INSERT INTO `person` (`card_id`, `name`, `dob`, `gender`) VALUES (1, 'Jessica Flatley', '2000-06-25', 'm');
INSERT INTO `person` (`card_id`, `name`, `dob`, `gender`) VALUES (2, 'Hildegard Farrell', '1970-02-02', 'm');
INSERT INTO `person` (`card_id`, `name`, `dob`, `gender`) VALUES (3, 'Damon West', '2001-11-09', 'm');
INSERT INTO `person` (`card_id`, `name`, `dob`, `gender`) VALUES (4, 'Dillan Lockman', '1999-05-30', 'm');
INSERT INTO `person` (`card_id`, `name`, `dob`, `gender`) VALUES (5, 'Prof. Marilou Stamm', '1970-03-30', 'f');
INSERT INTO `person` (`card_id`, `name`, `dob`, `gender`) VALUES (6, 'Dr. Jaden Miller', '2014-03-27', 'f');
INSERT INTO `person` (`card_id`, `name`, `dob`, `gender`) VALUES (7, 'Darrion Lindgren', '1981-09-03', 'm');
INSERT INTO `person` (`card_id`, `name`, `dob`, `gender`) VALUES (8, 'Leif Kohler', '2008-11-01', 'f');
INSERT INTO `person` (`card_id`, `name`, `dob`, `gender`) VALUES (9, 'Ardella Casper DDS', '2000-10-24', 'm');
INSERT INTO `person` (`card_id`, `name`, `dob`, `gender`) VALUES (10, 'Angelita Carter', '2020-07-30', 'f');
INSERT INTO `person` (`card_id`, `name`, `dob`, `gender`) VALUES (11, 'Amya Cole', '1988-03-18', 'f');
INSERT INTO `person` (`card_id`, `name`, `dob`, `gender`) VALUES (12, 'Rhea Dare', '2012-08-20', 'f');
INSERT INTO `person` (`card_id`, `name`, `dob`, `gender`) VALUES (13, 'Izaiah Funk V', '2011-10-14', 'f');
INSERT INTO `person` (`card_id`, `name`, `dob`, `gender`) VALUES (14, 'Dr. Lilian Stark', '1986-05-17', 'f');
INSERT INTO `person` (`card_id`, `name`, `dob`, `gender`) VALUES (15, 'Eulah Reynolds DDS', '1977-02-28', 'f');
INSERT INTO `person` (`card_id`, `name`, `dob`, `gender`) VALUES (16, 'Louie Bins', '1987-10-05', 'm');
INSERT INTO `person` (`card_id`, `name`, `dob`, `gender`) VALUES (17, 'Nelda Stiedemann', '1970-02-02', 'm');
INSERT INTO `person` (`card_id`, `name`, `dob`, `gender`) VALUES (18, 'Merritt Romaguera', '1976-05-18', 'f');
INSERT INTO `person` (`card_id`, `name`, `dob`, `gender`) VALUES (19, 'Prof. Rowan Cronin', '2018-08-06', 'm');
INSERT INTO `person` (`card_id`, `name`, `dob`, `gender`) VALUES (20, 'Coleman Wiegand', '2011-11-15', 'm');
INSERT INTO `person` (`card_id`, `name`, `dob`, `gender`) VALUES (21, 'Johnny Hodkiewicz', '1993-02-10', 'm');
INSERT INTO `person` (`card_id`, `name`, `dob`, `gender`) VALUES (22, 'Leonora McLaughlin', '1994-05-17', 'f');
INSERT INTO `person` (`card_id`, `name`, `dob`, `gender`) VALUES (23, 'Miss Elfrieda McKenzie V', '1972-05-15', 'm');
INSERT INTO `person` (`card_id`, `name`, `dob`, `gender`) VALUES (24, 'Prof. Kacie Fritsch', '2007-10-23', 'f');
INSERT INTO `person` (`card_id`, `name`, `dob`, `gender`) VALUES (25, 'Flavie Gulgowski PhD', '1976-10-18', 'f');
INSERT INTO `person` (`card_id`, `name`, `dob`, `gender`) VALUES (26, 'Mrs. Dianna Connelly DVM', '1973-05-14', 'm');
INSERT INTO `person` (`card_id`, `name`, `dob`, `gender`) VALUES (27, 'Prudence Reinger', '2002-10-09', 'm');
INSERT INTO `person` (`card_id`, `name`, `dob`, `gender`) VALUES (28, 'Marlee Stanton', '1987-10-07', 'm');
INSERT INTO `person` (`card_id`, `name`, `dob`, `gender`) VALUES (29, 'Mr. Jovany Simonis', '1986-08-01', 'f');
INSERT INTO `person` (`card_id`, `name`, `dob`, `gender`) VALUES (30, 'Magnus Welch', '2018-05-14', 'm');
INSERT INTO `person` (`card_id`, `name`, `dob`, `gender`) VALUES (31, 'Raymundo Larkin', '1975-08-02', 'm');
INSERT INTO `person` (`card_id`, `name`, `dob`, `gender`) VALUES (32, 'Tania Purdy V', '2003-04-26', 'f');
INSERT INTO `person` (`card_id`, `name`, `dob`, `gender`) VALUES (33, 'Theresia Hintz', '1973-03-26', 'f');
INSERT INTO `person` (`card_id`, `name`, `dob`, `gender`) VALUES (34, 'Joel Wintheiser', '2001-01-31', 'f');
INSERT INTO `person` (`card_id`, `name`, `dob`, `gender`) VALUES (35, 'Maurine Nitzsche', '2013-02-28', 'f');
INSERT INTO `person` (`card_id`, `name`, `dob`, `gender`) VALUES (36, 'Providenci Feeney IV', '1997-09-07', 'f');
INSERT INTO `person` (`card_id`, `name`, `dob`, `gender`) VALUES (37, 'Mr. Stuart Ondricka IV', '2002-03-24', 'm');
INSERT INTO `person` (`card_id`, `name`, `dob`, `gender`) VALUES (38, 'Dr. Minerva Cormier', '2004-03-30', 'f');
INSERT INTO `person` (`card_id`, `name`, `dob`, `gender`) VALUES (39, 'Leon Nolan', '2013-05-26', 'f');
INSERT INTO `person` (`card_id`, `name`, `dob`, `gender`) VALUES (40, 'Adrien Spencer', '1983-03-27', 'f');
INSERT INTO `person` (`card_id`, `name`, `dob`, `gender`) VALUES (41, 'Dr. Jamarcus Hamill', '1991-06-04', 'f');
INSERT INTO `person` (`card_id`, `name`, `dob`, `gender`) VALUES (42, 'Jacinto Quigley', '1973-12-06', 'f');
INSERT INTO `person` (`card_id`, `name`, `dob`, `gender`) VALUES (43, 'Tyree Bogan', '2010-10-06', 'm');
INSERT INTO `person` (`card_id`, `name`, `dob`, `gender`) VALUES (44, 'Prof. Werner Torphy', '1985-03-20', 'f');
INSERT INTO `person` (`card_id`, `name`, `dob`, `gender`) VALUES (45, 'Colin McKenzie', '1987-01-21', 'f');
INSERT INTO `person` (`card_id`, `name`, `dob`, `gender`) VALUES (46, 'Jeanie Schaefer', '1996-02-14', 'm');
INSERT INTO `person` (`card_id`, `name`, `dob`, `gender`) VALUES (47, 'Terrell Maggio', '1986-09-11', 'm');
INSERT INTO `person` (`card_id`, `name`, `dob`, `gender`) VALUES (48, 'Antwan Stoltenberg', '2013-06-21', 'm');
INSERT INTO `person` (`card_id`, `name`, `dob`, `gender`) VALUES (49, 'Mellie Hermiston', '2009-12-31', 'f');
INSERT INTO `person` (`card_id`, `name`, `dob`, `gender`) VALUES (50, 'Clay Vandervort', '1998-12-27', 'm');
INSERT INTO `person` (`card_id`, `name`, `dob`, `gender`) VALUES (51, 'Prof. Rodrigo West V', '2003-06-24', 'f');
INSERT INTO `person` (`card_id`, `name`, `dob`, `gender`) VALUES (52, 'Sam Bergstrom', '2015-07-28', 'm');
INSERT INTO `person` (`card_id`, `name`, `dob`, `gender`) VALUES (53, 'Mr. Jamir McDermott MD', '2011-04-27', 'm');
INSERT INTO `person` (`card_id`, `name`, `dob`, `gender`) VALUES (54, 'Dr. Alexys Hand IV', '1998-05-06', 'm');
INSERT INTO `person` (`card_id`, `name`, `dob`, `gender`) VALUES (55, 'Prof. Hildegard Mayer V', '1977-10-11', 'f');
INSERT INTO `person` (`card_id`, `name`, `dob`, `gender`) VALUES (56, 'Elmira Daugherty II', '1979-02-21', 'f');
INSERT INTO `person` (`card_id`, `name`, `dob`, `gender`) VALUES (57, 'Prof. Orlo Mueller', '2009-05-22', 'f');
INSERT INTO `person` (`card_id`, `name`, `dob`, `gender`) VALUES (58, 'Earlene Greenfelder', '2019-12-16', 'f');
INSERT INTO `person` (`card_id`, `name`, `dob`, `gender`) VALUES (59, 'Dina McClure', '2019-06-22', 'm');
INSERT INTO `person` (`card_id`, `name`, `dob`, `gender`) VALUES (60, 'Dr. Kirsten Erdman', '2019-11-12', 'f');
INSERT INTO `person` (`card_id`, `name`, `dob`, `gender`) VALUES (61, 'Mariano Wilkinson', '2004-09-02', 'f');
INSERT INTO `person` (`card_id`, `name`, `dob`, `gender`) VALUES (62, 'Mrs. Antonietta Flatley', '2021-02-10', 'm');
INSERT INTO `person` (`card_id`, `name`, `dob`, `gender`) VALUES (63, 'Donny Barrows', '1982-07-07', 'f');
INSERT INTO `person` (`card_id`, `name`, `dob`, `gender`) VALUES (64, 'Kody Ferry Sr.', '1974-04-15', 'f');
INSERT INTO `person` (`card_id`, `name`, `dob`, `gender`) VALUES (65, 'Jensen Beahan', '1989-01-22', 'f');
INSERT INTO `person` (`card_id`, `name`, `dob`, `gender`) VALUES (66, 'Prof. Sylvester Williamson PhD', '2004-09-25', 'f');
INSERT INTO `person` (`card_id`, `name`, `dob`, `gender`) VALUES (67, 'Rosalyn Fisher', '2003-03-01', 'f');
INSERT INTO `person` (`card_id`, `name`, `dob`, `gender`) VALUES (68, 'Prof. Norbert Rath DDS', '1998-07-05', 'f');
INSERT INTO `person` (`card_id`, `name`, `dob`, `gender`) VALUES (69, 'Irma Schmidt', '2012-06-19', 'f');
INSERT INTO `person` (`card_id`, `name`, `dob`, `gender`) VALUES (70, 'Emilia Howe', '1980-06-12', 'm');
INSERT INTO `person` (`card_id`, `name`, `dob`, `gender`) VALUES (71, 'Juston Marks', '1989-12-09', 'f');
INSERT INTO `person` (`card_id`, `name`, `dob`, `gender`) VALUES (72, 'Sienna Raynor', '1974-11-14', 'm');
INSERT INTO `person` (`card_id`, `name`, `dob`, `gender`) VALUES (73, 'Hildegard Legros', '1976-08-23', 'm');
INSERT INTO `person` (`card_id`, `name`, `dob`, `gender`) VALUES (74, 'Miss Letitia Brown', '2008-04-18', 'f');
INSERT INTO `person` (`card_id`, `name`, `dob`, `gender`) VALUES (75, 'Ciara Brakus', '2023-08-06', 'f');
INSERT INTO `person` (`card_id`, `name`, `dob`, `gender`) VALUES (76, 'Dr. Marc Tillman', '2007-02-01', 'f');
INSERT INTO `person` (`card_id`, `name`, `dob`, `gender`) VALUES (77, 'Gilberto Fay', '2014-03-27', 'm');
INSERT INTO `person` (`card_id`, `name`, `dob`, `gender`) VALUES (78, 'Mr. Harold Fritsch DDS', '2009-10-23', 'm');
INSERT INTO `person` (`card_id`, `name`, `dob`, `gender`) VALUES (79, 'Mohammed Hilpert', '1985-10-01', 'f');
INSERT INTO `person` (`card_id`, `name`, `dob`, `gender`) VALUES (80, 'Drew Halvorson', '1971-03-17', 'm');
INSERT INTO `person` (`card_id`, `name`, `dob`, `gender`) VALUES (81, 'Zaria Hickle II', '1982-06-16', 'm');
INSERT INTO `person` (`card_id`, `name`, `dob`, `gender`) VALUES (82, 'Dina Fadel III', '1975-02-11', 'f');
INSERT INTO `person` (`card_id`, `name`, `dob`, `gender`) VALUES (83, 'Mary Volkman', '2020-06-01', 'm');
INSERT INTO `person` (`card_id`, `name`, `dob`, `gender`) VALUES (84, 'Dr. Monica Anderson MD', '1992-01-22', 'f');
INSERT INTO `person` (`card_id`, `name`, `dob`, `gender`) VALUES (85, 'Enola Hilpert', '2019-07-23', 'f');
INSERT INTO `person` (`card_id`, `name`, `dob`, `gender`) VALUES (86, 'Dr. Loy Crooks', '1995-05-15', 'm');
INSERT INTO `person` (`card_id`, `name`, `dob`, `gender`) VALUES (87, 'Prof. Ernestine Cummerata', '2007-04-04', 'f');
INSERT INTO `person` (`card_id`, `name`, `dob`, `gender`) VALUES (88, 'Bette Auer', '1987-11-12', 'f');
INSERT INTO `person` (`card_id`, `name`, `dob`, `gender`) VALUES (89, 'Cornelius Blick', '2009-07-21', 'f');
INSERT INTO `person` (`card_id`, `name`, `dob`, `gender`) VALUES (90, 'Prof. Clarissa Zieme V', '1992-09-08', 'm');
INSERT INTO `person` (`card_id`, `name`, `dob`, `gender`) VALUES (91, 'Ulises McGlynn', '1995-05-16', 'm');
INSERT INTO `person` (`card_id`, `name`, `dob`, `gender`) VALUES (92, 'Ms. Pearl Wilkinson', '1979-11-18', 'm');
INSERT INTO `person` (`card_id`, `name`, `dob`, `gender`) VALUES (93, 'Prof. Morton Morar', '1978-06-24', 'f');
INSERT INTO `person` (`card_id`, `name`, `dob`, `gender`) VALUES (94, 'Mr. Dillan Breitenberg IV', '2006-11-01', 'f');
INSERT INTO `person` (`card_id`, `name`, `dob`, `gender`) VALUES (95, 'Mustafa Bednar', '1992-02-18', 'm');
INSERT INTO `person` (`card_id`, `name`, `dob`, `gender`) VALUES (96, 'Mekhi Sporer', '1990-03-13', 'm');
INSERT INTO `person` (`card_id`, `name`, `dob`, `gender`) VALUES (97, 'Brook Spencer', '2017-06-21', 'm');
INSERT INTO `person` (`card_id`, `name`, `dob`, `gender`) VALUES (98, 'Richie Hagenes Sr.', '1985-06-14', 'm');
INSERT INTO `person` (`card_id`, `name`, `dob`, `gender`) VALUES (99, 'Trudie Heidenreich', '2006-04-04', 'm');
INSERT INTO `person` (`card_id`, `name`, `dob`, `gender`) VALUES (100, 'Jacinto Considine', '1991-08-12', 'f');


#
# TABLE STRUCTURE FOR: sensor
#


#
# TABLE STRUCTURE FOR: space
#

DROP TABLE IF EXISTS `space`;

CREATE TABLE `space` (
  `space_id` int(11) NOT NULL,
  `description` varchar(255) NOT NULL,
  `max_capacity` int(11) DEFAULT NULL,
  PRIMARY KEY (`space_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

INSERT INTO `space` (`space_id`, `description`, `max_capacity`) VALUES (1, 'swimming pool', 37);
INSERT INTO `space` (`space_id`, `description`, `max_capacity`) VALUES (2, 'weight room', 98);
INSERT INTO `space` (`space_id`, `description`, `max_capacity`) VALUES (3, 'front desk', 75);
INSERT INTO `space` (`space_id`, `description`, `max_capacity`) VALUES (4, 'yoga studio', 59);
INSERT INTO `space` (`space_id`, `description`, `max_capacity`) VALUES (5, 'weight room', 93);
INSERT INTO `space` (`space_id`, `description`, `max_capacity`) VALUES (6, 'front desk', 43);
INSERT INTO `space` (`space_id`, `description`, `max_capacity`) VALUES (7, 'cardio room', 63);

DROP TABLE IF EXISTS `employee`;

CREATE TABLE `employee` (
  `card_id` int(11) NOT NULL,
  `supervisor_card_id` int(11) ,
  `schedule` varchar(255) NOT NULL,
  `employee_type` varchar(255) NOT NULL,
  `salary_hour` decimal(10,2) NOT NULL,
  PRIMARY KEY (`card_id`) ,
 CONSTRAINT `employee_ibfk_1` FOREIGN KEY (`card_id`) REFERENCES `person` (`card_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
 
INSERT INTO `employee` (`card_id`, `supervisor_card_id`,`schedule`, `employee_type`, `salary_hour`) VALUES (1, NULL, '6am-2pm', 'student', '20.00');
INSERT INTO `employee` (`card_id`, `supervisor_card_id`,`schedule`, `employee_type`, `salary_hour`) VALUES (2, 1,'1pm-10pm', 'student', '19.00');
INSERT INTO `employee` (`card_id`, `supervisor_card_id`,`schedule`, `employee_type`, `salary_hour`) VALUES (3, 1,'9am-7pm', 'student', '19.00');
INSERT INTO `employee` (`card_id`, `supervisor_card_id`,`schedule`, `employee_type`, `salary_hour`) VALUES (4, 1,'9am-7pm', 'student', '20.00');
INSERT INTO `employee` (`card_id`, `supervisor_card_id`,`schedule`, `employee_type`, `salary_hour`) VALUES (5,1, '1pm-10pm', 'student', '23.00');
INSERT INTO `employee` (`card_id`, `supervisor_card_id`,`schedule`, `employee_type`, `salary_hour`) VALUES (6, 2,'1pm-10pm', 'student', '24.00');
INSERT INTO `employee` (`card_id`, `supervisor_card_id`,`schedule`, `employee_type`, `salary_hour`) VALUES (7,2, '8am-4pm', 'student', '22.00');
INSERT INTO `employee` (`card_id`, `supervisor_card_id`,`schedule`, `employee_type`, `salary_hour`) VALUES (8,2, '1pm-10pm', 'student', '23.00');
INSERT INTO `employee` (`card_id`, `supervisor_card_id`,`schedule`, `employee_type`, `salary_hour`) VALUES (9,2, '9am-7pm', 'full time', '20.00');
INSERT INTO `employee` (`card_id`, `supervisor_card_id`,`schedule`, `employee_type`, `salary_hour`) VALUES (10,2, '1pm-10pm', 'student', '25.00');
INSERT INTO `employee` (`card_id`, `supervisor_card_id`,`schedule`, `employee_type`, `salary_hour`) VALUES (11,3, '1pm-10pm', 'full time', '21.00');
INSERT INTO `employee` (`card_id`, `supervisor_card_id`,`schedule`, `employee_type`, `salary_hour`) VALUES (12,3, '1pm-10pm', 'student', '19.00');
INSERT INTO `employee` (`card_id`, `supervisor_card_id`,`schedule`, `employee_type`, `salary_hour`) VALUES (13,3, '11am-9pm', 'full time', '20.00');
INSERT INTO `employee` (`card_id`, `supervisor_card_id`,`schedule`, `employee_type`, `salary_hour`) VALUES (14,3, '6am-2pm', 'student', '22.00');
INSERT INTO `employee` (`card_id`, `supervisor_card_id`,`schedule`, `employee_type`, `salary_hour`) VALUES (15,3, '8am-4pm', 'full time', '20.00');
INSERT INTO `employee` (`card_id`, `supervisor_card_id`,`schedule`, `employee_type`, `salary_hour`) VALUES (16,4, '6am-2pm', 'full time', '21.00');
INSERT INTO `employee` (`card_id`, `supervisor_card_id`,`schedule`, `employee_type`, `salary_hour`) VALUES (17,4, '8am-4pm', 'full time', '20.00');
INSERT INTO `employee` (`card_id`, `supervisor_card_id`,`schedule`, `employee_type`, `salary_hour`) VALUES (18, 4,'9am-7pm', 'student', '20.00');
INSERT INTO `employee` (`card_id`, `supervisor_card_id`,`schedule`, `employee_type`, `salary_hour`) VALUES (19,4, '6am-2pm', 'full time', '24.00');
INSERT INTO `employee` (`card_id`, `supervisor_card_id`,`schedule`, `employee_type`, `salary_hour`) VALUES (20,4, '8am-4pm', 'student', '19.00');

#
# TABLE STRUCTURE FOR: equipment
#

DROP TABLE IF EXISTS `equipment`;

CREATE TABLE `equipment` (
  `equipment_id` int(11) NOT NULL,
  `space_id` int(11) NOT NULL,
  `equipment_type` varchar(255) DEFAULT NULL,
  `is_available` tinyint(1) NOT NULL,
  PRIMARY KEY (`equipment_id`),
  KEY `space_id` (`space_id`),
  CONSTRAINT `equipment_ibfk_1` FOREIGN KEY (`space_id`) REFERENCES `space` (`space_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

INSERT INTO `equipment` (`equipment_id`, `space_id`, `equipment_type`, `is_available`) VALUES (1, 2, 'Elliptical Trainer', 0);
INSERT INTO `equipment` (`equipment_id`, `space_id`, `equipment_type`, `is_available`) VALUES (2, 1, 'Abdominal Mat', 1);
INSERT INTO `equipment` (`equipment_id`, `space_id`, `equipment_type`, `is_available`) VALUES (3, 7, 'Weightlifting Belt', 0);
INSERT INTO `equipment` (`equipment_id`, `space_id`, `equipment_type`, `is_available`) VALUES (4, 4, 'Grip Strengthener', 0);
INSERT INTO `equipment` (`equipment_id`, `space_id`, `equipment_type`, `is_available`) VALUES (5, 6, 'Adjustable Bench', 0);
INSERT INTO `equipment` (`equipment_id`, `space_id`, `equipment_type`, `is_available`) VALUES (6, 6, 'Ab Roller', 0);
INSERT INTO `equipment` (`equipment_id`, `space_id`, `equipment_type`, `is_available`) VALUES (7, 1, 'Cable Crossover', 1);
INSERT INTO `equipment` (`equipment_id`, `space_id`, `equipment_type`, `is_available`) VALUES (8, 4, 'Resistance Loop Bands', 1);
INSERT INTO `equipment` (`equipment_id`, `space_id`, `equipment_type`, `is_available`) VALUES (9, 2, 'Bumper Plates', 0);
INSERT INTO `equipment` (`equipment_id`, `space_id`, `equipment_type`, `is_available`) VALUES (10, 4, 'Weightlifting Gloves', 0);
INSERT INTO `equipment` (`equipment_id`, `space_id`, `equipment_type`, `is_available`) VALUES (11, 1, 'Pull-up Assist Bands', 0);
INSERT INTO `equipment` (`equipment_id`, `space_id`, `equipment_type`, `is_available`) VALUES (12, 7, 'Suspension Trainer (TRX)', 0);
INSERT INTO `equipment` (`equipment_id`, `space_id`, `equipment_type`, `is_available`) VALUES (13, 5, 'Arm Curl Machine', 0);
INSERT INTO `equipment` (`equipment_id`, `space_id`, `equipment_type`, `is_available`) VALUES (14, 5, 'Roman Chair', 0);
INSERT INTO `equipment` (`equipment_id`, `space_id`, `equipment_type`, `is_available`) VALUES (15, 1, 'Adjustable Bench', 0);
INSERT INTO `equipment` (`equipment_id`, `space_id`, `equipment_type`, `is_available`) VALUES (16, 4, 'Treadmill', 0);
INSERT INTO `equipment` (`equipment_id`, `space_id`, `equipment_type`, `is_available`) VALUES (17, 5, 'Abdominal Mat', 0);
INSERT INTO `equipment` (`equipment_id`, `space_id`, `equipment_type`, `is_available`) VALUES (18, 4, 'Elliptical Trainer', 1);
INSERT INTO `equipment` (`equipment_id`, `space_id`, `equipment_type`, `is_available`) VALUES (19, 6, 'Weightlifting Gloves', 1);
INSERT INTO `equipment` (`equipment_id`, `space_id`, `equipment_type`, `is_available`) VALUES (20, 4, 'Stationary Bike', 0);
INSERT INTO `equipment` (`equipment_id`, `space_id`, `equipment_type`, `is_available`) VALUES (21, 5, 'Hand Weights', 0);
INSERT INTO `equipment` (`equipment_id`, `space_id`, `equipment_type`, `is_available`) VALUES (22, 4, 'Sandbags', 1);
INSERT INTO `equipment` (`equipment_id`, `space_id`, `equipment_type`, `is_available`) VALUES (23, 1, 'Power Tower', 1);
INSERT INTO `equipment` (`equipment_id`, `space_id`, `equipment_type`, `is_available`) VALUES (24, 7, 'Push-up Handles', 1);
INSERT INTO `equipment` (`equipment_id`, `space_id`, `equipment_type`, `is_available`) VALUES (25, 7, 'Hack Squat Machine', 0);
INSERT INTO `equipment` (`equipment_id`, `space_id`, `equipment_type`, `is_available`) VALUES (26, 3, 'Squat Rack', 0);
INSERT INTO `equipment` (`equipment_id`, `space_id`, `equipment_type`, `is_available`) VALUES (27, 3, 'Resistance Bands', 0);
INSERT INTO `equipment` (`equipment_id`, `space_id`, `equipment_type`, `is_available`) VALUES (28, 3, 'Hand Grippers', 1);
INSERT INTO `equipment` (`equipment_id`, `space_id`, `equipment_type`, `is_available`) VALUES (29, 1, 'Vibrating Fitness Roller', 1);
INSERT INTO `equipment` (`equipment_id`, `space_id`, `equipment_type`, `is_available`) VALUES (30, 4, 'Bench Press', 1);
INSERT INTO `equipment` (`equipment_id`, `space_id`, `equipment_type`, `is_available`) VALUES (31, 1, 'Elliptical Trainer', 0);
INSERT INTO `equipment` (`equipment_id`, `space_id`, `equipment_type`, `is_available`) VALUES (32, 2, 'Leg Press', 0);
INSERT INTO `equipment` (`equipment_id`, `space_id`, `equipment_type`, `is_available`) VALUES (33, 6, 'Pilates Reformer', 1);
INSERT INTO `equipment` (`equipment_id`, `space_id`, `equipment_type`, `is_available`) VALUES (34, 4, 'Stability Ball', 0);
INSERT INTO `equipment` (`equipment_id`, `space_id`, `equipment_type`, `is_available`) VALUES (35, 4, 'Prowler Sled', 0);
INSERT INTO `equipment` (`equipment_id`, `space_id`, `equipment_type`, `is_available`) VALUES (36, 7, 'Step Platform', 1);
INSERT INTO `equipment` (`equipment_id`, `space_id`, `equipment_type`, `is_available`) VALUES (37, 6, 'Pilates Reformer', 0);
INSERT INTO `equipment` (`equipment_id`, `space_id`, `equipment_type`, `is_available`) VALUES (38, 2, 'Grip Strengthener', 1);
INSERT INTO `equipment` (`equipment_id`, `space_id`, `equipment_type`, `is_available`) VALUES (39, 7, 'Sandbags', 1);
INSERT INTO `equipment` (`equipment_id`, `space_id`, `equipment_type`, `is_available`) VALUES (40, 1, 'Aerobic Stepper', 0);
INSERT INTO `equipment` (`equipment_id`, `space_id`, `equipment_type`, `is_available`) VALUES (41, 7, 'Balance Disc', 1);
INSERT INTO `equipment` (`equipment_id`, `space_id`, `equipment_type`, `is_available`) VALUES (42, 3, 'Suspension Trainer (TRX)', 0);
INSERT INTO `equipment` (`equipment_id`, `space_id`, `equipment_type`, `is_available`) VALUES (43, 4, 'Spin Bike', 1);
INSERT INTO `equipment` (`equipment_id`, `space_id`, `equipment_type`, `is_available`) VALUES (44, 3, 'Ankle Weights', 0);
INSERT INTO `equipment` (`equipment_id`, `space_id`, `equipment_type`, `is_available`) VALUES (45, 6, 'Rower', 0);
INSERT INTO `equipment` (`equipment_id`, `space_id`, `equipment_type`, `is_available`) VALUES (46, 3, 'Push-up Handles', 1);
INSERT INTO `equipment` (`equipment_id`, `space_id`, `equipment_type`, `is_available`) VALUES (47, 2, 'Step Platform', 1);
INSERT INTO `equipment` (`equipment_id`, `space_id`, `equipment_type`, `is_available`) VALUES (48, 5, 'Hex Bar', 1);
INSERT INTO `equipment` (`equipment_id`, `space_id`, `equipment_type`, `is_available`) VALUES (49, 5, 'Battle Ropes', 0);
INSERT INTO `equipment` (`equipment_id`, `space_id`, `equipment_type`, `is_available`) VALUES (50, 7, 'Weightlifting Gloves', 0);


#
# TABLE STRUCTURE FOR: equipment_sensor
#

DROP TABLE IF EXISTS `equipment_sensor`;

CREATE TABLE `equipment_sensor` (
  `sensor_id` int(11) NOT NULL,
  `coverage` varchar(255) NOT NULL,
  PRIMARY KEY (`sensor_id`) -- ,
 -- CONSTRAINT `equipment_sensor_ibfk_1` FOREIGN KEY (`sensor_id`) REFERENCES `sensor` (`sensor_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

INSERT INTO `equipment_sensor` (`sensor_id`, `coverage`) VALUES (11, 'front desk');
INSERT INTO `equipment_sensor` (`sensor_id`, `coverage`) VALUES (12, 'weight room');
INSERT INTO `equipment_sensor` (`sensor_id`, `coverage`) VALUES (13, 'basketball court');
INSERT INTO `equipment_sensor` (`sensor_id`, `coverage`) VALUES (14, 'swimming pool');
INSERT INTO `equipment_sensor` (`sensor_id`, `coverage`) VALUES (15, 'weight room');
INSERT INTO `equipment_sensor` (`sensor_id`, `coverage`) VALUES (16, 'basketball court');
INSERT INTO `equipment_sensor` (`sensor_id`, `coverage`) VALUES (17, 'yoga studio');
INSERT INTO `equipment_sensor` (`sensor_id`, `coverage`) VALUES (18, 'basketball court');
INSERT INTO `equipment_sensor` (`sensor_id`, `coverage`) VALUES (19, 'weight room');
INSERT INTO `equipment_sensor` (`sensor_id`, `coverage`) VALUES (20, 'yoga studio');


#
# TABLE STRUCTURE FOR: events



#
# TABLE STRUCTURE FOR: student
#

DROP TABLE IF EXISTS `member`;

CREATE TABLE `member` (
  `card_id` int(11) NOT NULL,
  `membership_id` int(11) NOT NULL,
  PRIMARY KEY (`card_id`),
  UNIQUE KEY `membership_id` (`membership_id`),
  CONSTRAINT `member_ibfk_1` FOREIGN KEY (`card_id`) REFERENCES `person` (`card_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

INSERT INTO `member` (`card_id`, `membership_id`) VALUES (86, 101);
INSERT INTO `member` (`card_id`, `membership_id`) VALUES (56, 102);
INSERT INTO `member` (`card_id`, `membership_id`) VALUES (83, 103);
INSERT INTO `member` (`card_id`, `membership_id`) VALUES (85, 104);
INSERT INTO `member` (`card_id`, `membership_id`) VALUES (46, 105);
INSERT INTO `member` (`card_id`, `membership_id`) VALUES (32, 106);
INSERT INTO `member` (`card_id`, `membership_id`) VALUES (60, 107);
INSERT INTO `member` (`card_id`, `membership_id`) VALUES (21, 108);
INSERT INTO `member` (`card_id`, `membership_id`) VALUES (97, 109);
INSERT INTO `member` (`card_id`, `membership_id`) VALUES (25, 110);
INSERT INTO `member` (`card_id`, `membership_id`) VALUES (82, 111);
INSERT INTO `member` (`card_id`, `membership_id`) VALUES (52, 112);
INSERT INTO `member` (`card_id`, `membership_id`) VALUES (70, 113);
INSERT INTO `member` (`card_id`, `membership_id`) VALUES (27, 114);
INSERT INTO `member` (`card_id`, `membership_id`) VALUES (29, 115);
INSERT INTO `member` (`card_id`, `membership_id`) VALUES (84, 116);
INSERT INTO `member` (`card_id`, `membership_id`) VALUES (24, 117);
INSERT INTO `member` (`card_id`, `membership_id`) VALUES (26, 118);
INSERT INTO `member` (`card_id`, `membership_id`) VALUES (67, 119);
INSERT INTO `member` (`card_id`, `membership_id`) VALUES (99, 120);
INSERT INTO `member` (`card_id`, `membership_id`) VALUES (75, 121);
INSERT INTO `member` (`card_id`, `membership_id`) VALUES (43, 122);
INSERT INTO `member` (`card_id`, `membership_id`) VALUES (51, 123);
INSERT INTO `member` (`card_id`, `membership_id`) VALUES (98, 124);
INSERT INTO `member` (`card_id`, `membership_id`) VALUES (73, 125);
INSERT INTO `member` (`card_id`, `membership_id`) VALUES (91, 126);
INSERT INTO `member` (`card_id`, `membership_id`) VALUES (77, 127);
INSERT INTO `member` (`card_id`, `membership_id`) VALUES (87, 128);
INSERT INTO `member` (`card_id`, `membership_id`) VALUES (80, 129);
INSERT INTO `member` (`card_id`, `membership_id`) VALUES (31, 130);
INSERT INTO `member` (`card_id`, `membership_id`) VALUES (62, 131);
INSERT INTO `member` (`card_id`, `membership_id`) VALUES (63, 132);
INSERT INTO `member` (`card_id`, `membership_id`) VALUES (44, 133);
INSERT INTO `member` (`card_id`, `membership_id`) VALUES (74, 134);
INSERT INTO `member` (`card_id`, `membership_id`) VALUES (66, 135);
INSERT INTO `member` (`card_id`, `membership_id`) VALUES (55, 136);
INSERT INTO `member` (`card_id`, `membership_id`) VALUES (47, 137);
INSERT INTO `member` (`card_id`, `membership_id`) VALUES (95, 138);
INSERT INTO `member` (`card_id`, `membership_id`) VALUES (58, 139);
INSERT INTO `member` (`card_id`, `membership_id`) VALUES (23, 140);
INSERT INTO `member` (`card_id`, `membership_id`) VALUES (78, 141);
INSERT INTO `member` (`card_id`, `membership_id`) VALUES (28, 142);
INSERT INTO `member` (`card_id`, `membership_id`) VALUES (42, 143);
INSERT INTO `member` (`card_id`, `membership_id`) VALUES (34, 144);
INSERT INTO `member` (`card_id`, `membership_id`) VALUES (89, 145);
INSERT INTO `member` (`card_id`, `membership_id`) VALUES (59, 146);
INSERT INTO `member` (`card_id`, `membership_id`) VALUES (37, 147);
INSERT INTO `member` (`card_id`, `membership_id`) VALUES (53, 148);
INSERT INTO `member` (`card_id`, `membership_id`) VALUES (40, 149);
INSERT INTO `member` (`card_id`, `membership_id`) VALUES (64, 150);
INSERT INTO `member` (`card_id`, `membership_id`) VALUES (50, 151);
INSERT INTO `member` (`card_id`, `membership_id`) VALUES (92, 152);
INSERT INTO `member` (`card_id`, `membership_id`) VALUES (30, 153);
INSERT INTO `member` (`card_id`, `membership_id`) VALUES (41, 154);
INSERT INTO `member` (`card_id`, `membership_id`) VALUES (100, 155);
INSERT INTO `member` (`card_id`, `membership_id`) VALUES (88, 156);
INSERT INTO `member` (`card_id`, `membership_id`) VALUES (22, 157);
INSERT INTO `member` (`card_id`, `membership_id`) VALUES (94, 158);
INSERT INTO `member` (`card_id`, `membership_id`) VALUES (48, 159);
INSERT INTO `member` (`card_id`, `membership_id`) VALUES (35, 160);
INSERT INTO `member` (`card_id`, `membership_id`) VALUES (79, 161);
INSERT INTO `member` (`card_id`, `membership_id`) VALUES (49, 162);
INSERT INTO `member` (`card_id`, `membership_id`) VALUES (57, 163);
INSERT INTO `member` (`card_id`, `membership_id`) VALUES (72, 164);
INSERT INTO `member` (`card_id`, `membership_id`) VALUES (76, 165);
INSERT INTO `member` (`card_id`, `membership_id`) VALUES (45, 166);
INSERT INTO `member` (`card_id`, `membership_id`) VALUES (90, 167);
INSERT INTO `member` (`card_id`, `membership_id`) VALUES (38, 168);
INSERT INTO `member` (`card_id`, `membership_id`) VALUES (93, 169);
INSERT INTO `member` (`card_id`, `membership_id`) VALUES (71, 170);
INSERT INTO `member` (`card_id`, `membership_id`) VALUES (61, 171);
INSERT INTO `member` (`card_id`, `membership_id`) VALUES (68, 172);
INSERT INTO `member` (`card_id`, `membership_id`) VALUES (96, 173);
INSERT INTO `member` (`card_id`, `membership_id`) VALUES (81, 174);
INSERT INTO `member` (`card_id`, `membership_id`) VALUES (39, 175);
INSERT INTO `member` (`card_id`, `membership_id`) VALUES (54, 176);
INSERT INTO `member` (`card_id`, `membership_id`) VALUES (65, 177);
INSERT INTO `member` (`card_id`, `membership_id`) VALUES (69, 178);
INSERT INTO `member` (`card_id`, `membership_id`) VALUES (36, 179);
INSERT INTO `member` (`card_id`, `membership_id`) VALUES (33, 180);


#
# 




#
# TABLE STRUCTURE FOR: person
#



DROP TABLE IF EXISTS `university_affiliate`;

CREATE TABLE `university_affiliate` (
  `card_id` int(11) NOT NULL,
  `department` varchar(255) NOT NULL,
  PRIMARY KEY (`card_id`),
  CONSTRAINT `university_affiliate_ibfk_1` FOREIGN KEY (`card_id`) REFERENCES `member` (`card_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

INSERT INTO `university_affiliate` (`card_id`, `department`) VALUES (21, 'Chemistry');
INSERT INTO `university_affiliate` (`card_id`, `department`) VALUES (22, 'Chemistry');
INSERT INTO `university_affiliate` (`card_id`, `department`) VALUES (23, 'Economics');
INSERT INTO `university_affiliate` (`card_id`, `department`) VALUES (24, 'Political Science');
INSERT INTO `university_affiliate` (`card_id`, `department`) VALUES (25, 'Political Science');
INSERT INTO `university_affiliate` (`card_id`, `department`) VALUES (26, 'Computer Science');
INSERT INTO `university_affiliate` (`card_id`, `department`) VALUES (27, 'Chemistry');
INSERT INTO `university_affiliate` (`card_id`, `department`) VALUES (28, 'Biology');
INSERT INTO `university_affiliate` (`card_id`, `department`) VALUES (29, 'Economics');
INSERT INTO `university_affiliate` (`card_id`, `department`) VALUES (30, 'Economics');
INSERT INTO `university_affiliate` (`card_id`, `department`) VALUES (31, 'Engineering');
INSERT INTO `university_affiliate` (`card_id`, `department`) VALUES (32, 'Economics');
INSERT INTO `university_affiliate` (`card_id`, `department`) VALUES (33, 'History');
INSERT INTO `university_affiliate` (`card_id`, `department`) VALUES (34, 'Physics');
INSERT INTO `university_affiliate` (`card_id`, `department`) VALUES (35, 'Computer Science');
INSERT INTO `university_affiliate` (`card_id`, `department`) VALUES (36, 'Biology');
INSERT INTO `university_affiliate` (`card_id`, `department`) VALUES (37, 'Economics');
INSERT INTO `university_affiliate` (`card_id`, `department`) VALUES (38, 'Political Science');
INSERT INTO `university_affiliate` (`card_id`, `department`) VALUES (39, 'Mathematics');
INSERT INTO `university_affiliate` (`card_id`, `department`) VALUES (40, 'Computer Science');
INSERT INTO `university_affiliate` (`card_id`, `department`) VALUES (41, 'History');
INSERT INTO `university_affiliate` (`card_id`, `department`) VALUES (42, 'Computer Science');
INSERT INTO `university_affiliate` (`card_id`, `department`) VALUES (43, 'Engineering');
INSERT INTO `university_affiliate` (`card_id`, `department`) VALUES (44, 'Physics');
INSERT INTO `university_affiliate` (`card_id`, `department`) VALUES (45, 'Physics');
INSERT INTO `university_affiliate` (`card_id`, `department`) VALUES (46, 'Physics');
INSERT INTO `university_affiliate` (`card_id`, `department`) VALUES (47, 'Economics');
INSERT INTO `university_affiliate` (`card_id`, `department`) VALUES (48, 'Economics');
INSERT INTO `university_affiliate` (`card_id`, `department`) VALUES (49, 'Mathematics');
INSERT INTO `university_affiliate` (`card_id`, `department`) VALUES (50, 'Political Science');
INSERT INTO `university_affiliate` (`card_id`, `department`) VALUES (51, 'Computer Science');
INSERT INTO `university_affiliate` (`card_id`, `department`) VALUES (52, 'History');
INSERT INTO `university_affiliate` (`card_id`, `department`) VALUES (53, 'Engineering');
INSERT INTO `university_affiliate` (`card_id`, `department`) VALUES (54, 'History');
INSERT INTO `university_affiliate` (`card_id`, `department`) VALUES (55, 'Engineering');
INSERT INTO `university_affiliate` (`card_id`, `department`) VALUES (56, 'English Literature');
INSERT INTO `university_affiliate` (`card_id`, `department`) VALUES (57, 'Mathematics');
INSERT INTO `university_affiliate` (`card_id`, `department`) VALUES (58, 'Physics');
INSERT INTO `university_affiliate` (`card_id`, `department`) VALUES (59, 'Biology');
INSERT INTO `university_affiliate` (`card_id`, `department`) VALUES (60, 'Engineering');
INSERT INTO `university_affiliate` (`card_id`, `department`) VALUES (61, 'Political Science');
INSERT INTO `university_affiliate` (`card_id`, `department`) VALUES (62, 'Biology');
INSERT INTO `university_affiliate` (`card_id`, `department`) VALUES (63, 'History');
INSERT INTO `university_affiliate` (`card_id`, `department`) VALUES (64, 'Engineering');
INSERT INTO `university_affiliate` (`card_id`, `department`) VALUES (65, 'Biology');
INSERT INTO `university_affiliate` (`card_id`, `department`) VALUES (66, 'Biology');
INSERT INTO `university_affiliate` (`card_id`, `department`) VALUES (67, 'Biology');
INSERT INTO `university_affiliate` (`card_id`, `department`) VALUES (68, 'Political Science');
INSERT INTO `university_affiliate` (`card_id`, `department`) VALUES (69, 'Economics');
INSERT INTO `university_affiliate` (`card_id`, `department`) VALUES (70, 'Chemistry');
INSERT INTO `university_affiliate` (`card_id`, `department`) VALUES (71, 'Engineering');
INSERT INTO `university_affiliate` (`card_id`, `department`) VALUES (72, 'Biology');
INSERT INTO `university_affiliate` (`card_id`, `department`) VALUES (73, 'History');
INSERT INTO `university_affiliate` (`card_id`, `department`) VALUES (74, 'Political Science');
INSERT INTO `university_affiliate` (`card_id`, `department`) VALUES (75, 'Physics');
INSERT INTO `university_affiliate` (`card_id`, `department`) VALUES (76, 'Computer Science');
INSERT INTO `university_affiliate` (`card_id`, `department`) VALUES (77, 'History');
INSERT INTO `university_affiliate` (`card_id`, `department`) VALUES (78, 'Chemistry');
INSERT INTO `university_affiliate` (`card_id`, `department`) VALUES (79, 'History');
INSERT INTO `university_affiliate` (`card_id`, `department`) VALUES (80, 'Political Science');


#
# TABLE STRUCTURE FOR: usage_reading
#

DROP TABLE IF EXISTS `non_student`;

CREATE TABLE `non_student` (
  `card_id` int(11) NOT NULL,
  `member_type` varchar(255) NOT NULL,
  `credit_card` varchar(255) NOT NULL,
  PRIMARY KEY (`card_id`),
  CONSTRAINT `non_student_ibfk_1` FOREIGN KEY (`card_id`) REFERENCES `university_affiliate` (`card_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

INSERT INTO `non_student` (`card_id`, `member_type`, `credit_card`) VALUES (61, 'Faculty', '4532230467090');
INSERT INTO `non_student` (`card_id`, `member_type`, `credit_card`) VALUES (62, 'Administrative Staff', '4532033968581897');
INSERT INTO `non_student` (`card_id`, `member_type`, `credit_card`) VALUES (63, 'Financial Officer', '375659613152022');
INSERT INTO `non_student` (`card_id`, `member_type`, `credit_card`) VALUES (64, 'Technician', '5336439948780890');
INSERT INTO `non_student` (`card_id`, `member_type`, `credit_card`) VALUES (65, 'Security Officer', '6011544230431200');
INSERT INTO `non_student` (`card_id`, `member_type`, `credit_card`) VALUES (66, 'Research Assistant', '344434699407938');
INSERT INTO `non_student` (`card_id`, `member_type`, `credit_card`) VALUES (67, 'Financial Officer', '5233204591300093');
INSERT INTO `non_student` (`card_id`, `member_type`, `credit_card`) VALUES (68, 'Administrative Staff', '5336474966294452');
INSERT INTO `non_student` (`card_id`, `member_type`, `credit_card`) VALUES (69, 'IT Support', '5595771312430758');
INSERT INTO `non_student` (`card_id`, `member_type`, `credit_card`) VALUES (70, 'Security Officer', '6011148460474387');
INSERT INTO `non_student` (`card_id`, `member_type`, `credit_card`) VALUES (71, 'Financial Officer', '5254913190910262');
INSERT INTO `non_student` (`card_id`, `member_type`, `credit_card`) VALUES (72, 'IT Support', '4556104619531279');
INSERT INTO `non_student` (`card_id`, `member_type`, `credit_card`) VALUES (73, 'Technician', '4532711496364');
INSERT INTO `non_student` (`card_id`, `member_type`, `credit_card`) VALUES (74, 'Administrative Staff', '6011683458755529');
INSERT INTO `non_student` (`card_id`, `member_type`, `credit_card`) VALUES (75, 'Librarian', '5597723864694818');
INSERT INTO `non_student` (`card_id`, `member_type`, `credit_card`) VALUES (76, 'Administrative Staff', '5539485292980598');
INSERT INTO `non_student` (`card_id`, `member_type`, `credit_card`) VALUES (77, 'IT Support', '6011210302632546');
INSERT INTO `non_student` (`card_id`, `member_type`, `credit_card`) VALUES (78, 'Librarian', '4556182598007399');
INSERT INTO `non_student` (`card_id`, `member_type`, `credit_card`) VALUES (79, 'Financial Officer', '340055446272969');
INSERT INTO `non_student` (`card_id`, `member_type`, `credit_card`) VALUES (80, 'Security Officer', '6011714080661138');

DROP TABLE IF EXISTS `student`;

CREATE TABLE `student` (
  `card_id` int(11) NOT NULL,
  `student_type` varchar(255) NOT NULL,
  PRIMARY KEY (`card_id`),
  CONSTRAINT `student_ibfk_1` FOREIGN KEY (`card_id`) REFERENCES `university_affiliate` (`card_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

INSERT INTO `student` (`card_id`, `student_type`) VALUES (21, 'Engineering');
INSERT INTO `student` (`card_id`, `student_type`) VALUES (22, 'History');
INSERT INTO `student` (`card_id`, `student_type`) VALUES (23, 'Engineering');
INSERT INTO `student` (`card_id`, `student_type`) VALUES (24, 'Biology');
INSERT INTO `student` (`card_id`, `student_type`) VALUES (25, 'Economics');
INSERT INTO `student` (`card_id`, `student_type`) VALUES (26, 'Physics');
INSERT INTO `student` (`card_id`, `student_type`) VALUES (27, 'Engineering');
INSERT INTO `student` (`card_id`, `student_type`) VALUES (28, 'Physics');
INSERT INTO `student` (`card_id`, `student_type`) VALUES (29, 'Political Science');
INSERT INTO `student` (`card_id`, `student_type`) VALUES (30, 'Chemistry');
INSERT INTO `student` (`card_id`, `student_type`) VALUES (31, 'Political Science');
INSERT INTO `student` (`card_id`, `student_type`) VALUES (32, 'Economics');
INSERT INTO `student` (`card_id`, `student_type`) VALUES (33, 'Economics');
INSERT INTO `student` (`card_id`, `student_type`) VALUES (34, 'Chemistry');
INSERT INTO `student` (`card_id`, `student_type`) VALUES (35, 'Mathematics');
INSERT INTO `student` (`card_id`, `student_type`) VALUES (36, 'Economics');
INSERT INTO `student` (`card_id`, `student_type`) VALUES (37, 'Political Science');
INSERT INTO `student` (`card_id`, `student_type`) VALUES (38, 'History');
INSERT INTO `student` (`card_id`, `student_type`) VALUES (39, 'Biology');
INSERT INTO `student` (`card_id`, `student_type`) VALUES (40, 'Physics');
INSERT INTO `student` (`card_id`, `student_type`) VALUES (41, 'Biology');
INSERT INTO `student` (`card_id`, `student_type`) VALUES (42, 'Economics');
INSERT INTO `student` (`card_id`, `student_type`) VALUES (43, 'Mathematics');
INSERT INTO `student` (`card_id`, `student_type`) VALUES (44, 'Physics');
INSERT INTO `student` (`card_id`, `student_type`) VALUES (45, 'Political Science');
INSERT INTO `student` (`card_id`, `student_type`) VALUES (46, 'Physics');
INSERT INTO `student` (`card_id`, `student_type`) VALUES (47, 'Computer Science');
INSERT INTO `student` (`card_id`, `student_type`) VALUES (48, 'Computer Science');
INSERT INTO `student` (`card_id`, `student_type`) VALUES (49, 'History');
INSERT INTO `student` (`card_id`, `student_type`) VALUES (50, 'Economics');
INSERT INTO `student` (`card_id`, `student_type`) VALUES (51, 'Economics');
INSERT INTO `student` (`card_id`, `student_type`) VALUES (52, 'Engineering');
INSERT INTO `student` (`card_id`, `student_type`) VALUES (53, 'Physics');
INSERT INTO `student` (`card_id`, `student_type`) VALUES (54, 'Mathematics');
INSERT INTO `student` (`card_id`, `student_type`) VALUES (55, 'English Literature');
INSERT INTO `student` (`card_id`, `student_type`) VALUES (56, 'Computer Science');
INSERT INTO `student` (`card_id`, `student_type`) VALUES (57, 'Computer Science');
INSERT INTO `student` (`card_id`, `student_type`) VALUES (58, 'Physics');
INSERT INTO `student` (`card_id`, `student_type`) VALUES (59, 'Political Science');
INSERT INTO `student` (`card_id`, `student_type`) VALUES (60, 'History');




DROP TABLE IF EXISTS `Desk_Employee`;

CREATE TABLE `Desk_Employee` (
  `person_id` int(11) NOT NULL,
  PRIMARY KEY (`person_id`),
  CONSTRAINT `Desk_Employee_ibfk_1` FOREIGN KEY (`person_id`) REFERENCES `employee` (`card_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

INSERT INTO `Desk_Employee` (`person_id`) VALUES (1);
INSERT INTO `Desk_Employee` (`person_id`) VALUES (2);
INSERT INTO `Desk_Employee` (`person_id`) VALUES (3);
INSERT INTO `Desk_Employee` (`person_id`) VALUES (4);
INSERT INTO `Desk_Employee` (`person_id`) VALUES (5);
INSERT INTO `Desk_Employee` (`person_id`) VALUES (6);
INSERT INTO `Desk_Employee` (`person_id`) VALUES (7);
INSERT INTO `Desk_Employee` (`person_id`) VALUES (8);
INSERT INTO `Desk_Employee` (`person_id`) VALUES (9);
INSERT INTO `Desk_Employee` (`person_id`) VALUES (10);


#
# TABLE STRUCTURE FOR: Entry_Log
#

DROP TABLE IF EXISTS `Entry_Log`;

CREATE TABLE `Entry_Log` (
  `person_id` int(11) NOT NULL,
  `timestamp` timestamp NOT NULL DEFAULT current_timestamp() ON UPDATE current_timestamp(),
  PRIMARY KEY (`person_id`,`timestamp`),
  CONSTRAINT `Entry_Log_ibfk_1` FOREIGN KEY (`person_id`) REFERENCES `person` (`card_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (1, '2023-02-11 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (1, '2023-04-21 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (1, '2023-06-10 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (2, '2023-01-22 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (2, '2023-01-27 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (3, '2023-01-07 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (3, '2023-02-01 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (3, '2023-02-11 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (3, '2023-02-26 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (3, '2023-04-11 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (3, '2023-05-21 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (4, '2023-01-02 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (4, '2023-01-12 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (4, '2023-03-03 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (4, '2023-03-17 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (4, '2023-04-11 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (4, '2023-05-06 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (4, '2023-05-26 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (4, '2023-06-10 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (5, '2023-01-07 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (5, '2023-01-17 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (5, '2023-03-03 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (5, '2023-03-17 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (5, '2023-04-06 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (5, '2023-06-05 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (6, '2023-02-21 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (6, '2023-03-22 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (6, '2023-04-01 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (6, '2023-05-21 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (6, '2023-05-31 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (7, '2023-01-27 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (7, '2023-04-01 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (7, '2023-06-10 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (8, '2023-06-10 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (9, '2023-02-11 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (9, '2023-03-17 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (9, '2023-03-22 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (9, '2023-03-27 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (9, '2023-05-16 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (9, '2023-05-21 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (9, '2023-06-10 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (10, '2023-01-27 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (10, '2023-02-01 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (10, '2023-02-06 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (10, '2023-03-03 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (10, '2023-05-01 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (11, '2023-01-27 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (11, '2023-05-06 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (11, '2023-05-16 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (11, '2023-05-31 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (12, '2023-01-02 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (12, '2023-01-17 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (12, '2023-03-27 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (12, '2023-05-01 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (12, '2023-06-05 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (13, '2023-01-27 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (13, '2023-02-21 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (13, '2023-03-22 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (13, '2023-04-21 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (13, '2023-05-01 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (14, '2023-03-03 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (14, '2023-05-11 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (14, '2023-05-26 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (14, '2023-06-10 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (15, '2023-01-12 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (15, '2023-01-27 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (15, '2023-03-07 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (15, '2023-04-26 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (16, '2023-01-02 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (16, '2023-02-06 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (16, '2023-04-26 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (16, '2023-05-11 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (17, '2023-01-07 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (17, '2023-01-12 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (17, '2023-01-17 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (17, '2023-02-01 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (17, '2023-04-21 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (17, '2023-05-21 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (18, '2023-01-02 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (18, '2023-02-26 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (19, '2023-01-02 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (19, '2023-02-16 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (19, '2023-04-01 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (19, '2023-05-11 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (19, '2023-05-16 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (19, '2023-05-21 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (19, '2023-05-26 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (19, '2023-06-05 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (19, '2023-06-10 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (20, '2023-02-26 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (20, '2023-03-07 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (20, '2023-03-27 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (20, '2023-04-26 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (20, '2023-05-06 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (20, '2023-05-31 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (21, '2023-04-06 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (21, '2023-05-01 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (22, '2023-02-21 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (22, '2023-03-17 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (22, '2023-03-22 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (22, '2023-04-21 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (22, '2023-05-01 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (22, '2023-05-31 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (23, '2023-01-12 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (23, '2023-02-06 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (23, '2023-04-16 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (23, '2023-05-01 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (23, '2023-05-21 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (23, '2023-06-10 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (24, '2023-01-22 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (25, '2023-01-07 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (25, '2023-01-27 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (25, '2023-02-16 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (25, '2023-03-22 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (25, '2023-03-27 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (25, '2023-04-11 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (25, '2023-06-10 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (26, '2023-01-02 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (26, '2023-02-06 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (26, '2023-04-06 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (26, '2023-04-11 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (26, '2023-04-26 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (27, '2023-02-01 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (27, '2023-02-26 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (27, '2023-03-03 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (27, '2023-04-06 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (27, '2023-04-21 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (28, '2023-03-22 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (28, '2023-04-16 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (28, '2023-04-21 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (29, '2023-01-07 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (29, '2023-02-01 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (29, '2023-02-16 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (29, '2023-03-07 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (29, '2023-03-27 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (29, '2023-04-21 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (30, '2023-03-07 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (30, '2023-03-12 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (30, '2023-03-27 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (30, '2023-04-11 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (30, '2023-05-16 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (30, '2023-05-21 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (31, '2023-01-12 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (31, '2023-02-16 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (31, '2023-03-03 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (31, '2023-04-21 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (31, '2023-05-26 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (32, '2023-01-12 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (32, '2023-02-11 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (32, '2023-02-16 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (32, '2023-05-01 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (32, '2023-05-26 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (33, '2023-01-07 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (33, '2023-02-16 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (33, '2023-04-06 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (33, '2023-05-16 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (33, '2023-05-31 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (34, '2023-02-26 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (34, '2023-03-03 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (34, '2023-03-17 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (34, '2023-05-16 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (35, '2023-01-07 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (35, '2023-01-27 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (35, '2023-02-01 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (35, '2023-02-11 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (35, '2023-04-06 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (35, '2023-04-16 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (35, '2023-04-26 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (36, '2023-03-03 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (36, '2023-05-16 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (36, '2023-05-26 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (36, '2023-06-10 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (37, '2023-02-01 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (37, '2023-05-01 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (37, '2023-05-26 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (37, '2023-05-31 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (38, '2023-01-17 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (38, '2023-01-27 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (38, '2023-02-26 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (38, '2023-03-22 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (38, '2023-04-06 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (38, '2023-04-21 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (38, '2023-05-21 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (38, '2023-05-31 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (39, '2023-01-27 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (39, '2023-02-01 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (40, '2023-02-16 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (40, '2023-05-26 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (40, '2023-06-10 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (41, '2023-05-16 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (41, '2023-05-26 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (42, '2023-01-02 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (42, '2023-01-12 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (42, '2023-02-16 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (42, '2023-03-27 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (42, '2023-05-21 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (42, '2023-05-26 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (42, '2023-06-10 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (43, '2023-01-22 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (43, '2023-04-01 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (43, '2023-04-26 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (43, '2023-05-01 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (44, '2023-01-17 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (44, '2023-03-12 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (44, '2023-03-17 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (44, '2023-04-26 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (44, '2023-05-16 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (44, '2023-05-26 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (44, '2023-06-10 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (45, '2023-02-16 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (45, '2023-02-26 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (45, '2023-03-12 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (45, '2023-04-01 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (45, '2023-04-06 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (45, '2023-05-21 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (45, '2023-05-31 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (46, '2023-01-02 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (46, '2023-01-07 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (46, '2023-03-27 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (47, '2023-02-26 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (47, '2023-03-22 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (47, '2023-04-01 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (47, '2023-04-06 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (47, '2023-05-31 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (48, '2023-04-16 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (48, '2023-05-21 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (48, '2023-06-05 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (49, '2023-02-11 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (49, '2023-03-27 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (49, '2023-04-21 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (49, '2023-05-21 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (50, '2023-01-27 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (50, '2023-02-11 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (50, '2023-03-22 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (50, '2023-05-06 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (50, '2023-05-26 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (50, '2023-06-10 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (51, '2023-02-01 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (51, '2023-02-11 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (51, '2023-04-11 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (51, '2023-04-16 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (52, '2023-01-27 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (52, '2023-02-21 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (52, '2023-03-22 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (52, '2023-04-16 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (52, '2023-05-16 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (52, '2023-06-10 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (53, '2023-02-16 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (53, '2023-03-17 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (53, '2023-04-01 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (53, '2023-04-06 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (53, '2023-05-06 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (53, '2023-06-10 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (54, '2023-01-22 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (54, '2023-02-16 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (54, '2023-02-21 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (54, '2023-03-12 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (54, '2023-05-11 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (55, '2023-01-22 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (55, '2023-04-21 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (55, '2023-05-16 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (56, '2023-01-07 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (56, '2023-01-12 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (56, '2023-01-27 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (56, '2023-04-06 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (56, '2023-04-21 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (56, '2023-05-06 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (57, '2023-01-17 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (57, '2023-02-11 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (57, '2023-03-12 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (57, '2023-05-26 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (57, '2023-06-05 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (57, '2023-06-10 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (58, '2023-01-22 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (58, '2023-03-03 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (58, '2023-04-21 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (58, '2023-04-26 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (58, '2023-05-06 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (58, '2023-05-11 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (59, '2023-01-17 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (59, '2023-02-06 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (59, '2023-03-12 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (60, '2023-01-07 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (60, '2023-01-22 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (60, '2023-03-17 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (60, '2023-03-22 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (60, '2023-04-11 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (60, '2023-05-06 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (60, '2023-05-11 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (61, '2023-02-01 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (61, '2023-02-06 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (61, '2023-03-12 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (61, '2023-04-06 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (62, '2023-01-22 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (62, '2023-02-21 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (62, '2023-03-27 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (63, '2023-02-16 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (63, '2023-03-07 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (63, '2023-03-17 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (63, '2023-05-11 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (63, '2023-05-21 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (64, '2023-01-07 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (64, '2023-02-06 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (64, '2023-04-11 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (64, '2023-04-21 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (64, '2023-05-21 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (65, '2023-02-26 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (65, '2023-03-17 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (65, '2023-04-06 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (65, '2023-04-21 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (65, '2023-04-26 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (65, '2023-05-31 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (66, '2023-01-17 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (66, '2023-02-16 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (66, '2023-03-07 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (66, '2023-03-12 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (66, '2023-04-01 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (66, '2023-04-21 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (66, '2023-05-16 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (67, '2023-01-22 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (67, '2023-02-06 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (67, '2023-04-21 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (67, '2023-05-21 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (68, '2023-01-17 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (68, '2023-02-06 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (68, '2023-03-22 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (68, '2023-05-11 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (69, '2023-01-02 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (69, '2023-01-12 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (69, '2023-02-06 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (69, '2023-02-11 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (69, '2023-04-16 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (69, '2023-05-01 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (69, '2023-06-05 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (70, '2023-02-06 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (70, '2023-03-12 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (70, '2023-03-22 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (70, '2023-05-16 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (70, '2023-06-05 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (71, '2023-01-02 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (71, '2023-01-22 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (71, '2023-02-21 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (71, '2023-03-17 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (71, '2023-03-27 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (71, '2023-04-16 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (71, '2023-05-16 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (71, '2023-06-10 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (72, '2023-03-12 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (72, '2023-04-06 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (73, '2023-02-06 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (73, '2023-03-12 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (73, '2023-04-26 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (73, '2023-05-21 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (74, '2023-02-01 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (74, '2023-03-27 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (74, '2023-04-06 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (74, '2023-04-26 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (75, '2023-02-16 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (75, '2023-04-21 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (75, '2023-05-11 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (75, '2023-06-05 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (76, '2023-01-17 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (76, '2023-02-21 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (76, '2023-04-11 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (76, '2023-05-11 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (77, '2023-03-03 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (77, '2023-03-27 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (78, '2023-01-07 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (78, '2023-03-17 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (78, '2023-05-16 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (78, '2023-06-10 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (79, '2023-02-01 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (79, '2023-05-16 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (79, '2023-05-26 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (80, '2023-05-31 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (81, '2023-02-01 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (82, '2023-02-16 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (82, '2023-02-21 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (82, '2023-03-12 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (82, '2023-04-26 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (82, '2023-06-10 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (83, '2023-02-01 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (83, '2023-05-16 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (83, '2023-06-05 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (84, '2023-02-11 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (84, '2023-03-27 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (84, '2023-05-31 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (85, '2023-01-17 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (85, '2023-02-21 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (85, '2023-03-17 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (85, '2023-05-01 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (85, '2023-05-26 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (85, '2023-05-31 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (86, '2023-02-21 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (86, '2023-03-07 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (86, '2023-03-17 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (87, '2023-02-06 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (87, '2023-02-16 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (87, '2023-03-22 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (87, '2023-04-01 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (87, '2023-05-01 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (88, '2023-01-17 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (88, '2023-02-16 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (88, '2023-03-07 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (88, '2023-03-22 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (88, '2023-04-11 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (88, '2023-05-06 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (89, '2023-01-02 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (89, '2023-01-17 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (89, '2023-01-22 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (89, '2023-02-01 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (89, '2023-03-03 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (89, '2023-03-07 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (89, '2023-03-27 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (89, '2023-05-16 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (89, '2023-06-10 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (90, '2023-03-27 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (90, '2023-04-06 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (90, '2023-05-11 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (91, '2023-01-27 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (91, '2023-02-06 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (91, '2023-05-21 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (91, '2023-06-05 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (92, '2023-01-17 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (93, '2023-01-12 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (93, '2023-03-17 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (93, '2023-04-21 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (94, '2023-01-07 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (94, '2023-02-11 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (94, '2023-02-16 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (94, '2023-04-11 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (94, '2023-04-16 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (94, '2023-06-05 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (95, '2023-03-03 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (95, '2023-03-07 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (95, '2023-04-16 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (95, '2023-05-16 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (96, '2023-01-17 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (96, '2023-04-11 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (96, '2023-04-21 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (97, '2023-03-03 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (97, '2023-03-22 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (98, '2023-03-03 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (98, '2023-03-27 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (98, '2023-04-01 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (98, '2023-04-26 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (98, '2023-05-06 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (99, '2023-05-06 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (99, '2023-06-10 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (100, '2023-02-01 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (100, '2023-02-26 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (100, '2023-03-07 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (100, '2023-03-22 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (100, '2023-05-06 00:00:00');
INSERT INTO `Entry_Log` (`person_id`, `timestamp`) VALUES (100, '2023-05-26 00:00:00');


#
# TABLE STRUCTURE FOR: Trainer
#

DROP TABLE IF EXISTS `Trainer`;

CREATE TABLE `Trainer` (
  `person_id` int(11) NOT NULL,
  `credentials` varchar(255) NOT NULL,
  PRIMARY KEY (`person_id`),
  CONSTRAINT `Trainer_ibfk_1` FOREIGN KEY (`person_id`) REFERENCES `employee` (`card_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

INSERT INTO `Trainer` (`person_id`, `credentials`) VALUES (11, 'CNS');
INSERT INTO `Trainer` (`person_id`, `credentials`) VALUES (12, 'AEA');
INSERT INTO `Trainer` (`person_id`, `credentials`) VALUES (13, 'CAT');
INSERT INTO `Trainer` (`person_id`, `credentials`) VALUES (14, 'AHFS');
INSERT INTO `Trainer` (`person_id`, `credentials`) VALUES (15, 'CSI');
INSERT INTO `Trainer` (`person_id`, `credentials`) VALUES (16, 'CPT');
INSERT INTO `Trainer` (`person_id`, `credentials`) VALUES (17, 'TSAC-F');
INSERT INTO `Trainer` (`person_id`, `credentials`) VALUES (18, 'AEA');
INSERT INTO `Trainer` (`person_id`, `credentials`) VALUES (19, 'AHFS');
INSERT INTO `Trainer` (`person_id`, `credentials`) VALUES (20, 'CAT');


#
# TABLE STRUCTURE FOR: attends
#




#
# TABLE STRUCTURE FOR: employee
#

#

DROP TABLE IF EXISTS `events`;

CREATE TABLE `events` (
  `event_id` int(11) NOT NULL,
  `description` varchar(255) DEFAULT NULL,
  `space_id` int(11) NOT NULL,
  `start_time` timestamp NOT NULL DEFAULT current_timestamp() ON UPDATE current_timestamp(),
  `end_time` timestamp NOT NULL DEFAULT current_timestamp() ON UPDATE current_timestamp(),
  `capacity` int(11) NOT NULL,
  PRIMARY KEY (`event_id`),
  KEY `space_id` (`space_id`),
  CONSTRAINT `events_ibfk_1` FOREIGN KEY (`space_id`) REFERENCES `space` (`space_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

INSERT INTO `events` (`event_id`, `description`, `space_id`, `start_time`, `end_time`, `capacity`) VALUES (1, 'High-intensity Interval Training (HIIT) Bootcamp', 2, '2023-02-26 00:00:00', '2023-06-05 00:00:00', 33);
INSERT INTO `events` (`event_id`, `description`, `space_id`, `start_time`, `end_time`, `capacity`) VALUES (2, 'Water Volleyball', 6, '2023-02-23 00:00:00', '2023-03-12 00:00:00', 20);
INSERT INTO `events` (`event_id`, `description`, `space_id`, `start_time`, `end_time`, `capacity`) VALUES (3, 'Aqua Aerobics Classes', 3, '2023-02-05 00:00:00', '2023-03-27 00:00:00', 40);
INSERT INTO `events` (`event_id`, `description`, `space_id`, `start_time`, `end_time`, `capacity`) VALUES (4, 'Kids\' Pool Party', 2, '2023-01-24 00:00:00', '2023-03-17 00:00:00', 28);
INSERT INTO `events` (`event_id`, `description`, `space_id`, `start_time`, `end_time`, `capacity`) VALUES (5, 'Aqua Aerobics Classes', 1, '2023-01-09 00:00:00', '2023-05-06 00:00:00', 36);
INSERT INTO `events` (`event_id`, `description`, `space_id`, `start_time`, `end_time`, `capacity`) VALUES (6, 'Martial Arts Demonstration', 7, '2023-02-02 00:00:00', '2023-04-26 00:00:00', 24);
INSERT INTO `events` (`event_id`, `description`, `space_id`, `start_time`, `end_time`, `capacity`) VALUES (7, 'Spin Bike Marathon', 3, '2023-02-02 00:00:00', '2023-05-26 00:00:00', 31);
INSERT INTO `events` (`event_id`, `description`, `space_id`, `start_time`, `end_time`, `capacity`) VALUES (8, 'Basketball Freestyle Show', 7, '2023-01-01 00:00:00', '2023-04-26 00:00:00', 39);
INSERT INTO `events` (`event_id`, `description`, `space_id`, `start_time`, `end_time`, `capacity`) VALUES (9, 'Aqua Yoga', 1, '2023-01-03 00:00:00', '2023-05-26 00:00:00', 29);
INSERT INTO `events` (`event_id`, `description`, `space_id`, `start_time`, `end_time`, `capacity`) VALUES (10, 'Slam Ball Challenges', 4, '2023-01-27 00:00:00', '2023-04-26 00:00:00', 29);
INSERT INTO `events` (`event_id`, `description`, `space_id`, `start_time`, `end_time`, `capacity`) VALUES (11, 'Pool Float Day', 1, '2023-02-02 00:00:00', '2023-04-21 00:00:00', 23);
INSERT INTO `events` (`event_id`, `description`, `space_id`, `start_time`, `end_time`, `capacity`) VALUES (12, 'Zumba Dance Party', 6, '2023-01-03 00:00:00', '2023-05-26 00:00:00', 40);
INSERT INTO `events` (`event_id`, `description`, `space_id`, `start_time`, `end_time`, `capacity`) VALUES (13, 'High-intensity Interval Training (HIIT) Bootcamp', 7, '2023-02-23 00:00:00', '2023-06-10 00:00:00', 23);
INSERT INTO `events` (`event_id`, `description`, `space_id`, `start_time`, `end_time`, `capacity`) VALUES (14, 'Basketball Freestyle Show', 7, '2023-01-21 00:00:00', '2023-05-16 00:00:00', 36);
INSERT INTO `events` (`event_id`, `description`, `space_id`, `start_time`, `end_time`, `capacity`) VALUES (15, 'Martial Arts Demonstration', 5, '2023-01-03 00:00:00', '2023-04-06 00:00:00', 34);
INSERT INTO `events` (`event_id`, `description`, `space_id`, `start_time`, `end_time`, `capacity`) VALUES (16, 'Diving Board Contests', 6, '2023-02-02 00:00:00', '2023-05-26 00:00:00', 28);
INSERT INTO `events` (`event_id`, `description`, `space_id`, `start_time`, `end_time`, `capacity`) VALUES (17, 'Strength Training Seminars', 4, '2023-01-27 00:00:00', '2023-04-26 00:00:00', 29);
INSERT INTO `events` (`event_id`, `description`, `space_id`, `start_time`, `end_time`, `capacity`) VALUES (18, 'Dive-in Movie Night', 6, '2023-01-18 00:00:00', '2023-03-22 00:00:00', 35);
INSERT INTO `events` (`event_id`, `description`, `space_id`, `start_time`, `end_time`, `capacity`) VALUES (19, 'Midnight Basketball', 4, '2023-02-08 00:00:00', '2023-03-12 00:00:00', 27);
INSERT INTO `events` (`event_id`, `description`, `space_id`, `start_time`, `end_time`, `capacity`) VALUES (20, 'Diving Board Contests', 1, '2023-01-03 00:00:00', '2023-04-21 00:00:00', 22);
INSERT INTO `events` (`event_id`, `description`, `space_id`, `start_time`, `end_time`, `capacity`) VALUES (21, 'Hydrotherapy Sessions', 5, '2023-02-08 00:00:00', '2023-06-05 00:00:00', 23);
INSERT INTO `events` (`event_id`, `description`, `space_id`, `start_time`, `end_time`, `capacity`) VALUES (22, 'Midnight Basketball', 3, '2023-02-26 00:00:00', '2023-03-22 00:00:00', 38);
INSERT INTO `events` (`event_id`, `description`, `space_id`, `start_time`, `end_time`, `capacity`) VALUES (23, 'Pilates in the Pool', 1, '2023-02-08 00:00:00', '2023-04-21 00:00:00', 38);
INSERT INTO `events` (`event_id`, `description`, `space_id`, `start_time`, `end_time`, `capacity`) VALUES (24, 'Martial Arts Demonstration', 3, '2023-01-01 00:00:00', '2023-04-01 00:00:00', 24);
INSERT INTO `events` (`event_id`, `description`, `space_id`, `start_time`, `end_time`, `capacity`) VALUES (25, 'Stretch and Flexibility Workshop', 3, '2023-02-17 00:00:00', '2023-04-11 00:00:00', 22);
INSERT INTO `events` (`event_id`, `description`, `space_id`, `start_time`, `end_time`, `capacity`) VALUES (26, 'Strength Training Seminars', 4, '2023-01-27 00:00:00', '2023-05-26 00:00:00', 20);
INSERT INTO `events` (`event_id`, `description`, `space_id`, `start_time`, `end_time`, `capacity`) VALUES (27, 'Pilates in the Pool', 7, '2023-02-23 00:00:00', '2023-04-11 00:00:00', 38);
INSERT INTO `events` (`event_id`, `description`, `space_id`, `start_time`, `end_time`, `capacity`) VALUES (28, 'Water Polo Matches', 5, '2023-02-11 00:00:00', '2023-05-16 00:00:00', 30);
INSERT INTO `events` (`event_id`, `description`, `space_id`, `start_time`, `end_time`, `capacity`) VALUES (29, 'High-intensity Interval Training (HIIT) Bootcamp', 5, '2023-01-15 00:00:00', '2023-05-31 00:00:00', 40);
INSERT INTO `events` (`event_id`, `description`, `space_id`, `start_time`, `end_time`, `capacity`) VALUES (30, 'Nutrition and Fitness Workshop', 4, '2023-02-23 00:00:00', '2023-04-06 00:00:00', 32);
INSERT INTO `events` (`event_id`, `description`, `space_id`, `start_time`, `end_time`, `capacity`) VALUES (31, 'Basketball Shooting Contests', 5, '2023-01-21 00:00:00', '2023-03-12 00:00:00', 26);
INSERT INTO `events` (`event_id`, `description`, `space_id`, `start_time`, `end_time`, `capacity`) VALUES (32, 'Circuit Training Class', 5, '2023-01-03 00:00:00', '2023-06-10 00:00:00', 32);
INSERT INTO `events` (`event_id`, `description`, `space_id`, `start_time`, `end_time`, `capacity`) VALUES (33, 'Stretch and Flexibility Workshop', 2, '2023-02-05 00:00:00', '2023-04-16 00:00:00', 20);
INSERT INTO `events` (`event_id`, `description`, `space_id`, `start_time`, `end_time`, `capacity`) VALUES (34, 'Senior Swim Sessions', 4, '2023-01-01 00:00:00', '2023-03-22 00:00:00', 27);
INSERT INTO `events` (`event_id`, `description`, `space_id`, `start_time`, `end_time`, `capacity`) VALUES (35, 'Hydrotherapy Sessions', 3, '2023-01-15 00:00:00', '2023-06-05 00:00:00', 27);
INSERT INTO `events` (`event_id`, `description`, `space_id`, `start_time`, `end_time`, `capacity`) VALUES (36, 'Sauna Relaxation Evening', 2, '2023-02-26 00:00:00', '2023-04-06 00:00:00', 36);
INSERT INTO `events` (`event_id`, `description`, `space_id`, `start_time`, `end_time`, `capacity`) VALUES (37, 'TRX Training Workshop', 2, '2023-02-20 00:00:00', '2023-04-16 00:00:00', 20);
INSERT INTO `events` (`event_id`, `description`, `space_id`, `start_time`, `end_time`, `capacity`) VALUES (38, 'Aqua Aerobics Classes', 3, '2023-01-06 00:00:00', '2023-04-26 00:00:00', 26);
INSERT INTO `events` (`event_id`, `description`, `space_id`, `start_time`, `end_time`, `capacity`) VALUES (39, 'Cardio Kickboxing Class', 5, '2023-01-03 00:00:00', '2023-04-01 00:00:00', 21);
INSERT INTO `events` (`event_id`, `description`, `space_id`, `start_time`, `end_time`, `capacity`) VALUES (40, 'Basketball Freestyle Show', 1, '2023-01-18 00:00:00', '2023-04-16 00:00:00', 27);
INSERT INTO `events` (`event_id`, `description`, `space_id`, `start_time`, `end_time`, `capacity`) VALUES (41, 'Diving Board Contests', 3, '2023-01-18 00:00:00', '2023-03-22 00:00:00', 22);
INSERT INTO `events` (`event_id`, `description`, `space_id`, `start_time`, `end_time`, `capacity`) VALUES (42, 'High-intensity Interval Training (HIIT) Bootcamp', 1, '2023-01-03 00:00:00', '2023-04-16 00:00:00', 28);
INSERT INTO `events` (`event_id`, `description`, `space_id`, `start_time`, `end_time`, `capacity`) VALUES (43, 'Summer Splash Fest', 1, '2023-02-11 00:00:00', '2023-05-06 00:00:00', 30);
INSERT INTO `events` (`event_id`, `description`, `space_id`, `start_time`, `end_time`, `capacity`) VALUES (44, 'Nutrition and Fitness Workshop', 1, '2023-02-20 00:00:00', '2023-04-21 00:00:00', 32);
INSERT INTO `events` (`event_id`, `description`, `space_id`, `start_time`, `end_time`, `capacity`) VALUES (45, 'Strength Training Seminars', 7, '2023-02-26 00:00:00', '2023-05-26 00:00:00', 23);
INSERT INTO `events` (`event_id`, `description`, `space_id`, `start_time`, `end_time`, `capacity`) VALUES (46, 'Sauna Relaxation Evening', 2, '2023-01-27 00:00:00', '2023-05-11 00:00:00', 35);
INSERT INTO `events` (`event_id`, `description`, `space_id`, `start_time`, `end_time`, `capacity`) VALUES (47, 'Kids\' Pool Party', 1, '2023-01-12 00:00:00', '2023-05-11 00:00:00', 22);
INSERT INTO `events` (`event_id`, `description`, `space_id`, `start_time`, `end_time`, `capacity`) VALUES (48, 'Senior Swim Sessions', 4, '2023-01-15 00:00:00', '2023-04-16 00:00:00', 30);
INSERT INTO `events` (`event_id`, `description`, `space_id`, `start_time`, `end_time`, `capacity`) VALUES (49, 'CrossFit Intro Session', 7, '2023-02-02 00:00:00', '2023-03-12 00:00:00', 39);
INSERT INTO `events` (`event_id`, `description`, `space_id`, `start_time`, `end_time`, `capacity`) VALUES (50, 'TRX Training Workshop', 5, '2023-02-11 00:00:00', '2023-04-16 00:00:00', 35);


#
# TABLE STRUCTURE FOR: exit_log
#

DROP TABLE IF EXISTS `exit_log`;

CREATE TABLE `exit_log` (
  `person_id` int(11) NOT NULL,
  `timestamp` timestamp NOT NULL DEFAULT current_timestamp() ON UPDATE current_timestamp(),
  PRIMARY KEY (`person_id`,`timestamp`),
  CONSTRAINT `exit_log_ibfk_1` FOREIGN KEY (`person_id`) REFERENCES `employee` (`card_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

INSERT INTO `exit_log` (`person_id`, `timestamp`) VALUES (1, '2023-02-16 00:00:00');
INSERT INTO `exit_log` (`person_id`, `timestamp`) VALUES (2, '2023-01-07 00:00:00');
INSERT INTO `exit_log` (`person_id`, `timestamp`) VALUES (2, '2023-02-21 00:00:00');
INSERT INTO `exit_log` (`person_id`, `timestamp`) VALUES (2, '2023-03-17 00:00:00');
INSERT INTO `exit_log` (`person_id`, `timestamp`) VALUES (3, '2023-02-26 00:00:00');
INSERT INTO `exit_log` (`person_id`, `timestamp`) VALUES (3, '2023-03-12 00:00:00');
INSERT INTO `exit_log` (`person_id`, `timestamp`) VALUES (3, '2023-04-06 00:00:00');
INSERT INTO `exit_log` (`person_id`, `timestamp`) VALUES (3, '2023-05-21 00:00:00');
INSERT INTO `exit_log` (`person_id`, `timestamp`) VALUES (4, '2023-01-12 00:00:00');
INSERT INTO `exit_log` (`person_id`, `timestamp`) VALUES (4, '2023-02-16 00:00:00');
INSERT INTO `exit_log` (`person_id`, `timestamp`) VALUES (4, '2023-04-16 00:00:00');
INSERT INTO `exit_log` (`person_id`, `timestamp`) VALUES (5, '2023-05-06 00:00:00');
INSERT INTO `exit_log` (`person_id`, `timestamp`) VALUES (6, '2023-01-07 00:00:00');
INSERT INTO `exit_log` (`person_id`, `timestamp`) VALUES (6, '2023-03-27 00:00:00');
INSERT INTO `exit_log` (`person_id`, `timestamp`) VALUES (6, '2023-04-06 00:00:00');
INSERT INTO `exit_log` (`person_id`, `timestamp`) VALUES (6, '2023-05-16 00:00:00');
INSERT INTO `exit_log` (`person_id`, `timestamp`) VALUES (7, '2023-01-17 00:00:00');
INSERT INTO `exit_log` (`person_id`, `timestamp`) VALUES (7, '2023-05-06 00:00:00');
INSERT INTO `exit_log` (`person_id`, `timestamp`) VALUES (8, '2023-01-12 00:00:00');
INSERT INTO `exit_log` (`person_id`, `timestamp`) VALUES (8, '2023-02-21 00:00:00');
INSERT INTO `exit_log` (`person_id`, `timestamp`) VALUES (8, '2023-06-05 00:00:00');
INSERT INTO `exit_log` (`person_id`, `timestamp`) VALUES (9, '2023-03-03 00:00:00');
INSERT INTO `exit_log` (`person_id`, `timestamp`) VALUES (10, '2023-01-02 00:00:00');
INSERT INTO `exit_log` (`person_id`, `timestamp`) VALUES (10, '2023-01-07 00:00:00');
INSERT INTO `exit_log` (`person_id`, `timestamp`) VALUES (10, '2023-01-27 00:00:00');
INSERT INTO `exit_log` (`person_id`, `timestamp`) VALUES (11, '2023-04-16 00:00:00');
INSERT INTO `exit_log` (`person_id`, `timestamp`) VALUES (12, '2023-02-01 00:00:00');
INSERT INTO `exit_log` (`person_id`, `timestamp`) VALUES (12, '2023-02-11 00:00:00');
INSERT INTO `exit_log` (`person_id`, `timestamp`) VALUES (12, '2023-03-17 00:00:00');
INSERT INTO `exit_log` (`person_id`, `timestamp`) VALUES (13, '2023-02-26 00:00:00');
INSERT INTO `exit_log` (`person_id`, `timestamp`) VALUES (13, '2023-05-06 00:00:00');
INSERT INTO `exit_log` (`person_id`, `timestamp`) VALUES (13, '2023-05-11 00:00:00');
INSERT INTO `exit_log` (`person_id`, `timestamp`) VALUES (14, '2023-04-06 00:00:00');
INSERT INTO `exit_log` (`person_id`, `timestamp`) VALUES (14, '2023-05-06 00:00:00');
INSERT INTO `exit_log` (`person_id`, `timestamp`) VALUES (15, '2023-05-31 00:00:00');
INSERT INTO `exit_log` (`person_id`, `timestamp`) VALUES (15, '2023-06-05 00:00:00');
INSERT INTO `exit_log` (`person_id`, `timestamp`) VALUES (16, '2023-02-26 00:00:00');
INSERT INTO `exit_log` (`person_id`, `timestamp`) VALUES (16, '2023-04-21 00:00:00');
INSERT INTO `exit_log` (`person_id`, `timestamp`) VALUES (16, '2023-05-31 00:00:00');
INSERT INTO `exit_log` (`person_id`, `timestamp`) VALUES (17, '2023-03-12 00:00:00');
INSERT INTO `exit_log` (`person_id`, `timestamp`) VALUES (17, '2023-05-01 00:00:00');
INSERT INTO `exit_log` (`person_id`, `timestamp`) VALUES (18, '2023-02-01 00:00:00');
INSERT INTO `exit_log` (`person_id`, `timestamp`) VALUES (18, '2023-03-07 00:00:00');
INSERT INTO `exit_log` (`person_id`, `timestamp`) VALUES (18, '2023-03-12 00:00:00');
INSERT INTO `exit_log` (`person_id`, `timestamp`) VALUES (18, '2023-05-06 00:00:00');
INSERT INTO `exit_log` (`person_id`, `timestamp`) VALUES (19, '2023-02-01 00:00:00');
INSERT INTO `exit_log` (`person_id`, `timestamp`) VALUES (19, '2023-04-26 00:00:00');
INSERT INTO `exit_log` (`person_id`, `timestamp`) VALUES (19, '2023-05-11 00:00:00');
INSERT INTO `exit_log` (`person_id`, `timestamp`) VALUES (20, '2023-06-10 00:00:00');


#
# TABLE STRUCTURE FOR: family
#

DROP TABLE IF EXISTS `family`;

CREATE TABLE `family` (
  `card_id` int(11) NOT NULL,
  `credit_card` varchar(255) NOT NULL,
  `familyof` int(11) NOT NULL,
  PRIMARY KEY (`card_id`),
  KEY `familyof` (`familyof`),
  CONSTRAINT `family_ibfk_1` FOREIGN KEY (`card_id`) REFERENCES `member` (`card_id`),
  CONSTRAINT `family_ibfk_2` FOREIGN KEY (`familyof`) REFERENCES `university_affiliate` (`card_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

INSERT INTO `family` (`card_id`, `credit_card`, `familyof`) VALUES (81, '374911691893082', 56);
INSERT INTO `family` (`card_id`, `credit_card`, `familyof`) VALUES (82, '5156145510443087', 26);
INSERT INTO `family` (`card_id`, `credit_card`, `familyof`) VALUES (83, '5585573276858420', 43);
INSERT INTO `family` (`card_id`, `credit_card`, `familyof`) VALUES (84, '379375561049012', 26);
INSERT INTO `family` (`card_id`, `credit_card`, `familyof`) VALUES (85, '6011388014248671', 71);
INSERT INTO `family` (`card_id`, `credit_card`, `familyof`) VALUES (86, '6011146589392521', 27);
INSERT INTO `family` (`card_id`, `credit_card`, `familyof`) VALUES (87, '4485750011512667', 49);
INSERT INTO `family` (`card_id`, `credit_card`, `familyof`) VALUES (88, '6011892608515265', 27);
INSERT INTO `family` (`card_id`, `credit_card`, `familyof`) VALUES (89, '4024007129686308', 75);
INSERT INTO `family` (`card_id`, `credit_card`, `familyof`) VALUES (90, '4539142308420', 54);
INSERT INTO `family` (`card_id`, `credit_card`, `familyof`) VALUES (91, '4485793656935748', 25);
INSERT INTO `family` (`card_id`, `credit_card`, `familyof`) VALUES (92, '5547880577475354', 71);
INSERT INTO `family` (`card_id`, `credit_card`, `familyof`) VALUES (93, '4716374473881760', 76);
INSERT INTO `family` (`card_id`, `credit_card`, `familyof`) VALUES (94, '4556076273279', 70);
INSERT INTO `family` (`card_id`, `credit_card`, `familyof`) VALUES (95, '4916517137823213', 29);
INSERT INTO `family` (`card_id`, `credit_card`, `familyof`) VALUES (96, '6011701604067677', 55);
INSERT INTO `family` (`card_id`, `credit_card`, `familyof`) VALUES (97, '4716770692793', 66);
INSERT INTO `family` (`card_id`, `credit_card`, `familyof`) VALUES (98, '5570419192923298', 36);
INSERT INTO `family` (`card_id`, `credit_card`, `familyof`) VALUES (99, '4485274337312', 77);
INSERT INTO `family` (`card_id`, `credit_card`, `familyof`) VALUES (100, '4532735373972', 79);


#
# TABLE STRUCTURE FOR: location_reading
#



DROP TABLE IF EXISTS `location_sensor`;

CREATE TABLE `location_sensor` (
  `sensor_id` int(11) NOT NULL,
  `coverage` varchar(255) NOT NULL,
  PRIMARY KEY (`sensor_id`)-- ,
--   CONSTRAINT `location_sensor_ibfk_1` FOREIGN KEY (`sensor_id`) REFERENCES `sensor` (`sensor_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

INSERT INTO `location_sensor` (`sensor_id`, `coverage`) VALUES (1, 'basketball court');
INSERT INTO `location_sensor` (`sensor_id`, `coverage`) VALUES (2, 'cardio room');
INSERT INTO `location_sensor` (`sensor_id`, `coverage`) VALUES (3, 'swimming pool');
INSERT INTO `location_sensor` (`sensor_id`, `coverage`) VALUES (4, 'basketball court');
INSERT INTO `location_sensor` (`sensor_id`, `coverage`) VALUES (5, 'cardio room');
INSERT INTO `location_sensor` (`sensor_id`, `coverage`) VALUES (6, 'other');
INSERT INTO `location_sensor` (`sensor_id`, `coverage`) VALUES (7, 'front desk');
INSERT INTO `location_sensor` (`sensor_id`, `coverage`) VALUES (8, 'basketball court');
INSERT INTO `location_sensor` (`sensor_id`, `coverage`) VALUES (9, 'front desk');
INSERT INTO `location_sensor` (`sensor_id`, `coverage`) VALUES (10, 'other');


#
# TABLE STRUCTURE FOR: member
#



DROP TABLE IF EXISTS `usage_reading`;

CREATE TABLE `usage_reading` (
  `sensor_id` int(11) NOT NULL,
  `equipment_id` int(11) NOT NULL,
  `card_id` int(11) NOT NULL,
  `timestamp` timestamp NOT NULL DEFAULT current_timestamp() ON UPDATE current_timestamp(),
  PRIMARY KEY (`sensor_id`,`equipment_id`,`card_id`,`timestamp`),
  KEY `equipment_id` (`equipment_id`),
  KEY `card_id` (`card_id`),
  CONSTRAINT `usage_reading_ibfk_1` FOREIGN KEY (`equipment_id`) REFERENCES `equipment` (`equipment_id`),
  CONSTRAINT `usage_reading_ibfk_2` FOREIGN KEY (`sensor_id`) REFERENCES `equipment_sensor` (`sensor_id`),
  CONSTRAINT `usage_reading_ibfk_3` FOREIGN KEY (`card_id`) REFERENCES `member` (`card_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (11, 7, 49, '2023-02-01 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (11, 8, 54, '2023-04-06 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (11, 9, 85, '2023-06-05 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (11, 10, 35, '2023-03-03 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (11, 10, 57, '2023-04-21 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (11, 10, 86, '2023-03-27 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (11, 11, 28, '2023-02-16 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (11, 13, 27, '2023-03-27 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (11, 15, 98, '2023-02-11 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (11, 16, 76, '2023-06-05 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (11, 17, 27, '2023-05-26 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (11, 17, 38, '2023-04-11 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (11, 18, 86, '2023-05-06 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (11, 19, 38, '2023-03-03 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (11, 19, 46, '2023-03-17 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (11, 19, 75, '2023-03-27 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (11, 21, 94, '2023-05-06 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (11, 23, 61, '2023-04-06 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (11, 24, 91, '2023-04-01 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (11, 25, 54, '2023-03-27 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (11, 26, 53, '2023-03-27 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (11, 26, 55, '2023-04-21 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (11, 27, 52, '2023-03-12 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (11, 28, 86, '2023-02-21 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (11, 29, 40, '2023-05-11 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (11, 29, 79, '2023-04-26 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (11, 31, 44, '2023-04-06 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (11, 31, 73, '2023-04-26 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (11, 34, 45, '2023-02-06 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (11, 34, 49, '2023-04-11 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (11, 34, 63, '2023-03-12 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (11, 34, 69, '2023-03-07 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (11, 36, 50, '2023-02-06 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (11, 36, 52, '2023-05-01 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (11, 36, 52, '2023-05-06 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (11, 36, 79, '2023-04-01 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (11, 40, 73, '2023-04-16 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (11, 45, 39, '2023-01-22 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (11, 45, 60, '2023-05-21 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (11, 45, 61, '2023-04-16 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (11, 45, 71, '2023-02-11 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (11, 46, 51, '2023-04-06 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (11, 46, 96, '2023-04-11 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (12, 1, 75, '2023-05-11 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (12, 1, 100, '2023-04-11 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (12, 2, 87, '2023-05-16 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (12, 2, 97, '2023-04-11 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (12, 5, 51, '2023-01-22 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (12, 7, 30, '2023-02-16 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (12, 8, 21, '2023-03-17 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (12, 9, 93, '2023-01-22 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (12, 10, 71, '2023-02-11 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (12, 11, 98, '2023-03-07 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (12, 14, 57, '2023-04-11 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (12, 15, 37, '2023-02-01 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (12, 15, 65, '2023-04-21 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (12, 17, 40, '2023-02-11 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (12, 20, 49, '2023-04-16 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (12, 25, 27, '2023-01-17 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (12, 26, 45, '2023-03-12 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (12, 26, 71, '2023-01-02 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (12, 27, 30, '2023-02-11 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (12, 29, 52, '2023-02-01 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (12, 30, 36, '2023-02-26 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (12, 31, 54, '2023-03-07 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (12, 33, 30, '2023-01-07 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (12, 34, 23, '2023-05-01 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (12, 35, 81, '2023-03-12 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (12, 35, 99, '2023-03-07 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (12, 36, 96, '2023-04-21 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (12, 38, 28, '2023-03-17 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (12, 39, 81, '2023-04-16 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (12, 40, 67, '2023-02-01 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (12, 41, 33, '2023-02-21 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (12, 42, 73, '2023-04-16 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (12, 45, 37, '2023-04-11 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (12, 45, 72, '2023-06-10 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (12, 46, 66, '2023-02-01 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (12, 46, 67, '2023-05-21 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (12, 48, 52, '2023-05-11 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (12, 48, 88, '2023-03-17 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (12, 49, 90, '2023-03-03 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (12, 50, 51, '2023-05-06 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (13, 1, 48, '2023-02-21 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (13, 1, 88, '2023-04-06 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (13, 2, 22, '2023-05-21 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (13, 2, 44, '2023-01-22 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (13, 2, 47, '2023-02-26 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (13, 2, 62, '2023-02-06 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (13, 4, 49, '2023-03-12 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (13, 7, 33, '2023-04-11 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (13, 13, 44, '2023-03-17 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (13, 17, 33, '2023-03-12 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (13, 17, 39, '2023-04-01 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (13, 18, 30, '2023-01-12 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (13, 19, 49, '2023-03-22 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (13, 20, 92, '2023-01-27 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (13, 20, 98, '2023-04-06 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (13, 25, 47, '2023-06-05 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (13, 27, 36, '2023-05-31 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (13, 28, 31, '2023-04-16 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (13, 29, 90, '2023-03-22 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (13, 30, 59, '2023-03-27 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (13, 31, 39, '2023-01-22 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (13, 31, 54, '2023-03-12 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (13, 31, 88, '2023-02-11 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (13, 33, 65, '2023-04-01 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (13, 33, 84, '2023-05-06 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (13, 35, 34, '2023-04-06 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (13, 35, 70, '2023-06-10 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (13, 37, 91, '2023-02-01 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (13, 41, 33, '2023-02-06 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (13, 42, 57, '2023-03-27 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (13, 43, 22, '2023-04-16 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (13, 43, 37, '2023-05-16 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (13, 43, 81, '2023-01-02 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (13, 44, 56, '2023-02-06 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (13, 45, 27, '2023-03-12 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (13, 45, 56, '2023-02-16 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (13, 48, 41, '2023-03-17 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (13, 48, 60, '2023-01-17 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (13, 49, 59, '2023-01-22 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (14, 6, 79, '2023-05-11 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (14, 8, 76, '2023-04-21 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (14, 11, 87, '2023-01-27 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (14, 12, 73, '2023-05-31 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (14, 13, 61, '2023-06-10 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (14, 14, 54, '2023-02-01 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (14, 15, 74, '2023-01-17 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (14, 16, 44, '2023-03-17 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (14, 17, 42, '2023-06-10 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (14, 19, 89, '2023-02-01 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (14, 20, 32, '2023-02-01 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (14, 20, 33, '2023-01-22 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (14, 20, 74, '2023-02-11 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (14, 23, 51, '2023-01-27 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (14, 23, 73, '2023-05-26 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (14, 27, 59, '2023-05-21 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (14, 28, 41, '2023-01-22 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (14, 29, 31, '2023-04-01 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (14, 29, 59, '2023-04-21 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (14, 29, 78, '2023-03-17 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (14, 31, 34, '2023-02-01 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (14, 31, 55, '2023-03-12 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (14, 31, 79, '2023-04-16 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (14, 31, 88, '2023-04-06 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (14, 33, 34, '2023-05-31 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (14, 38, 68, '2023-02-01 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (14, 41, 28, '2023-02-01 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (14, 43, 94, '2023-03-07 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (14, 46, 42, '2023-03-07 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (14, 46, 46, '2023-05-31 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (14, 46, 68, '2023-03-12 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (14, 46, 98, '2023-05-11 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (14, 48, 42, '2023-05-26 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (14, 48, 67, '2023-04-21 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (14, 49, 57, '2023-02-06 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (14, 49, 70, '2023-02-21 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (14, 49, 89, '2023-03-03 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (14, 50, 32, '2023-04-16 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (14, 50, 92, '2023-05-01 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (15, 1, 79, '2023-02-26 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (15, 2, 87, '2023-02-16 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (15, 3, 86, '2023-04-06 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (15, 6, 57, '2023-03-12 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (15, 8, 84, '2023-04-26 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (15, 10, 36, '2023-03-17 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (15, 11, 21, '2023-02-26 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (15, 14, 42, '2023-05-11 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (15, 15, 44, '2023-06-10 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (15, 15, 83, '2023-02-11 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (15, 15, 88, '2023-02-26 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (15, 16, 67, '2023-05-06 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (15, 19, 97, '2023-03-22 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (15, 20, 97, '2023-03-27 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (15, 21, 40, '2023-05-31 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (15, 22, 23, '2023-04-01 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (15, 22, 38, '2023-05-01 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (15, 24, 50, '2023-02-21 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (15, 25, 23, '2023-02-06 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (15, 28, 79, '2023-02-21 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (15, 28, 90, '2023-02-16 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (15, 28, 98, '2023-06-05 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (15, 30, 46, '2023-01-12 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (15, 33, 61, '2023-03-17 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (15, 33, 93, '2023-05-16 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (15, 34, 82, '2023-05-06 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (15, 35, 78, '2023-02-26 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (15, 35, 82, '2023-04-01 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (15, 37, 27, '2023-04-11 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (15, 37, 64, '2023-02-16 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (15, 38, 87, '2023-02-11 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (15, 39, 50, '2023-05-21 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (15, 41, 81, '2023-03-17 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (15, 42, 53, '2023-02-26 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (15, 43, 31, '2023-02-26 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (15, 44, 66, '2023-01-17 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (15, 45, 92, '2023-04-11 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (15, 47, 84, '2023-02-01 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (15, 50, 22, '2023-04-21 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (15, 50, 50, '2023-04-06 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (16, 4, 31, '2023-02-11 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (16, 4, 54, '2023-01-27 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (16, 9, 83, '2023-05-11 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (16, 10, 75, '2023-05-21 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (16, 11, 48, '2023-04-11 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (16, 11, 84, '2023-04-01 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (16, 11, 95, '2023-04-01 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (16, 14, 70, '2023-03-12 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (16, 17, 52, '2023-01-17 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (16, 18, 47, '2023-01-02 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (16, 19, 39, '2023-05-26 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (16, 19, 49, '2023-03-22 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (16, 21, 75, '2023-01-17 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (16, 21, 75, '2023-03-22 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (16, 21, 77, '2023-05-31 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (16, 21, 99, '2023-02-26 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (16, 22, 97, '2023-01-27 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (16, 25, 83, '2023-06-05 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (16, 28, 30, '2023-01-02 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (16, 28, 60, '2023-06-05 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (16, 28, 65, '2023-06-10 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (16, 29, 67, '2023-01-27 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (16, 31, 60, '2023-01-27 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (16, 32, 22, '2023-01-07 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (16, 35, 30, '2023-02-11 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (16, 36, 26, '2023-05-01 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (16, 36, 79, '2023-03-17 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (16, 37, 46, '2023-04-11 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (16, 37, 77, '2023-05-06 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (16, 37, 85, '2023-04-06 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (16, 38, 89, '2023-03-12 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (16, 39, 22, '2023-02-11 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (16, 39, 52, '2023-04-11 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (16, 39, 86, '2023-01-22 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (16, 42, 68, '2023-03-07 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (16, 44, 72, '2023-02-06 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (16, 45, 91, '2023-02-06 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (16, 46, 57, '2023-02-21 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (17, 1, 72, '2023-04-21 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (17, 1, 76, '2023-03-12 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (17, 3, 66, '2023-01-27 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (17, 3, 100, '2023-05-06 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (17, 4, 61, '2023-04-06 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (17, 6, 91, '2023-05-11 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (17, 7, 65, '2023-01-07 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (17, 9, 94, '2023-02-01 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (17, 11, 83, '2023-03-22 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (17, 13, 81, '2023-05-21 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (17, 13, 96, '2023-01-02 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (17, 15, 29, '2023-05-26 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (17, 15, 36, '2023-03-22 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (17, 15, 56, '2023-04-01 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (17, 16, 63, '2023-02-06 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (17, 16, 78, '2023-02-21 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (17, 19, 85, '2023-01-17 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (17, 20, 95, '2023-02-26 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (17, 21, 40, '2023-05-26 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (17, 21, 46, '2023-02-11 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (17, 21, 48, '2023-01-12 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (17, 21, 57, '2023-03-22 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (17, 25, 37, '2023-05-16 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (17, 25, 40, '2023-02-01 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (17, 28, 42, '2023-02-06 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (17, 30, 35, '2023-03-27 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (17, 30, 36, '2023-01-27 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (17, 31, 81, '2023-03-22 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (17, 35, 82, '2023-01-27 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (17, 39, 77, '2023-05-16 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (17, 41, 48, '2023-02-26 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (17, 41, 48, '2023-04-11 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (17, 45, 38, '2023-02-26 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (17, 47, 46, '2023-02-16 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (17, 47, 50, '2023-02-01 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (17, 49, 67, '2023-03-27 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (17, 50, 47, '2023-03-07 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (18, 3, 56, '2023-05-21 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (18, 5, 40, '2023-05-01 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (18, 7, 75, '2023-03-07 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (18, 11, 30, '2023-02-01 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (18, 13, 70, '2023-06-10 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (18, 14, 45, '2023-06-10 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (18, 15, 22, '2023-04-11 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (18, 15, 80, '2023-06-05 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (18, 15, 96, '2023-04-11 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (18, 17, 32, '2023-05-26 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (18, 17, 99, '2023-01-07 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (18, 19, 82, '2023-01-02 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (18, 22, 44, '2023-01-12 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (18, 22, 58, '2023-04-16 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (18, 23, 30, '2023-05-26 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (18, 23, 33, '2023-03-17 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (18, 28, 38, '2023-01-17 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (18, 29, 70, '2023-05-26 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (18, 29, 89, '2023-02-06 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (18, 30, 55, '2023-05-26 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (18, 34, 39, '2023-04-11 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (18, 35, 28, '2023-02-16 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (18, 35, 29, '2023-04-26 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (18, 36, 85, '2023-03-03 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (18, 37, 54, '2023-03-27 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (18, 37, 56, '2023-04-11 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (18, 37, 60, '2023-05-06 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (18, 38, 87, '2023-05-21 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (18, 39, 72, '2023-03-27 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (18, 42, 37, '2023-03-03 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (18, 42, 58, '2023-03-07 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (18, 42, 69, '2023-01-12 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (18, 43, 57, '2023-02-26 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (18, 44, 21, '2023-02-26 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (18, 44, 39, '2023-02-16 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (18, 46, 39, '2023-04-26 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (18, 47, 50, '2023-01-12 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (18, 47, 92, '2023-04-16 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (18, 47, 100, '2023-01-02 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (18, 48, 26, '2023-01-07 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (18, 48, 65, '2023-01-27 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (18, 48, 81, '2023-04-26 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (18, 49, 33, '2023-02-16 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (18, 49, 70, '2023-02-21 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (19, 1, 44, '2023-03-22 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (19, 1, 93, '2023-02-21 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (19, 2, 40, '2023-05-16 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (19, 8, 54, '2023-06-10 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (19, 8, 77, '2023-02-06 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (19, 9, 55, '2023-05-26 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (19, 9, 78, '2023-03-17 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (19, 10, 33, '2023-01-02 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (19, 11, 84, '2023-03-12 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (19, 15, 23, '2023-02-01 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (19, 15, 57, '2023-05-06 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (19, 15, 91, '2023-01-17 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (19, 16, 51, '2023-01-27 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (19, 16, 64, '2023-05-21 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (19, 17, 48, '2023-05-31 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (19, 18, 53, '2023-04-11 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (19, 21, 91, '2023-05-31 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (19, 24, 59, '2023-03-27 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (19, 31, 32, '2023-02-11 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (19, 31, 38, '2023-05-16 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (19, 31, 47, '2023-01-12 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (19, 31, 73, '2023-04-21 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (19, 31, 81, '2023-05-11 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (19, 32, 45, '2023-06-05 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (19, 33, 94, '2023-01-12 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (19, 35, 28, '2023-04-21 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (19, 35, 31, '2023-02-16 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (19, 38, 55, '2023-05-16 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (19, 39, 23, '2023-02-16 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (19, 40, 56, '2023-04-01 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (19, 41, 91, '2023-03-03 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (19, 42, 25, '2023-04-11 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (19, 42, 60, '2023-04-16 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (19, 42, 77, '2023-01-02 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (19, 44, 29, '2023-02-11 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (19, 44, 56, '2023-01-17 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (19, 44, 77, '2023-02-16 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (19, 44, 100, '2023-02-16 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (19, 47, 23, '2023-02-01 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (19, 49, 52, '2023-04-26 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (19, 49, 72, '2023-04-16 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (19, 50, 37, '2023-06-10 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (19, 50, 98, '2023-02-01 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (20, 6, 27, '2023-04-26 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (20, 6, 69, '2023-05-26 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (20, 7, 66, '2023-03-03 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (20, 12, 22, '2023-01-12 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (20, 12, 94, '2023-03-17 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (20, 13, 69, '2023-03-27 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (20, 13, 77, '2023-04-16 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (20, 16, 49, '2023-05-31 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (20, 17, 96, '2023-04-21 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (20, 18, 78, '2023-01-22 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (20, 19, 55, '2023-02-21 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (20, 19, 100, '2023-05-06 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (20, 20, 68, '2023-04-16 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (20, 22, 28, '2023-01-02 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (20, 23, 23, '2023-01-22 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (20, 24, 24, '2023-05-01 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (20, 24, 83, '2023-02-21 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (20, 25, 72, '2023-04-06 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (20, 25, 92, '2023-05-16 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (20, 26, 44, '2023-05-06 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (20, 27, 29, '2023-05-01 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (20, 27, 79, '2023-01-12 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (20, 28, 64, '2023-06-05 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (20, 30, 55, '2023-03-12 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (20, 30, 68, '2023-04-01 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (20, 30, 98, '2023-03-27 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (20, 31, 94, '2023-04-11 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (20, 32, 91, '2023-05-16 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (20, 33, 79, '2023-05-01 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (20, 34, 24, '2023-01-22 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (20, 34, 65, '2023-03-07 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (20, 36, 50, '2023-04-06 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (20, 37, 38, '2023-06-05 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (20, 39, 56, '2023-05-06 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (20, 44, 73, '2023-03-22 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (20, 45, 23, '2023-01-27 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (20, 46, 77, '2023-04-26 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (20, 48, 33, '2023-04-01 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (20, 49, 76, '2023-02-16 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (20, 49, 88, '2023-02-11 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (20, 50, 50, '2023-01-27 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (20, 50, 94, '2023-05-31 00:00:00');

DROP TABLE IF EXISTS `location_reading`;

CREATE TABLE `location_reading` (
  `sensor_id` int(11) NOT NULL,
  `space_id` int(11) NOT NULL,
  `person_id` int(11) NOT NULL,
  `timestamp` timestamp NOT NULL DEFAULT current_timestamp() ON UPDATE current_timestamp(),
  PRIMARY KEY (`sensor_id`,`space_id`,`person_id`,`timestamp`),
  KEY `space_id` (`space_id`),
  KEY `person_id` (`person_id`),
  CONSTRAINT `location_reading_ibfk_1` FOREIGN KEY (`space_id`) REFERENCES `space` (`space_id`),
  CONSTRAINT `location_reading_ibfk_2` FOREIGN KEY (`sensor_id`) REFERENCES `location_sensor` (`sensor_id`),
  CONSTRAINT `location_reading_ibfk_3` FOREIGN KEY (`person_id`) REFERENCES `person` (`card_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

INSERT INTO `location_reading` (`sensor_id`, `space_id`, `person_id`, `timestamp`) VALUES (1, 1, 37, '2023-05-21 00:00:00');
INSERT INTO `location_reading` (`sensor_id`, `space_id`, `person_id`, `timestamp`) VALUES (1, 1, 61, '2023-06-10 00:00:00');
INSERT INTO `location_reading` (`sensor_id`, `space_id`, `person_id`, `timestamp`) VALUES (1, 1, 72, '2023-05-16 00:00:00');
INSERT INTO `location_reading` (`sensor_id`, `space_id`, `person_id`, `timestamp`) VALUES (1, 2, 33, '2023-03-27 00:00:00');
INSERT INTO `location_reading` (`sensor_id`, `space_id`, `person_id`, `timestamp`) VALUES (1, 3, 63, '2023-04-26 00:00:00');
INSERT INTO `location_reading` (`sensor_id`, `space_id`, `person_id`, `timestamp`) VALUES (1, 3, 77, '2023-01-02 00:00:00');
INSERT INTO `location_reading` (`sensor_id`, `space_id`, `person_id`, `timestamp`) VALUES (1, 5, 28, '2023-05-26 00:00:00');
INSERT INTO `location_reading` (`sensor_id`, `space_id`, `person_id`, `timestamp`) VALUES (2, 1, 27, '2023-02-11 00:00:00');
INSERT INTO `location_reading` (`sensor_id`, `space_id`, `person_id`, `timestamp`) VALUES (2, 1, 61, '2023-02-11 00:00:00');
INSERT INTO `location_reading` (`sensor_id`, `space_id`, `person_id`, `timestamp`) VALUES (2, 1, 76, '2023-01-17 00:00:00');
INSERT INTO `location_reading` (`sensor_id`, `space_id`, `person_id`, `timestamp`) VALUES (2, 2, 59, '2023-02-01 00:00:00');
INSERT INTO `location_reading` (`sensor_id`, `space_id`, `person_id`, `timestamp`) VALUES (2, 2, 96, '2023-04-11 00:00:00');
INSERT INTO `location_reading` (`sensor_id`, `space_id`, `person_id`, `timestamp`) VALUES (2, 2, 98, '2023-03-07 00:00:00');
INSERT INTO `location_reading` (`sensor_id`, `space_id`, `person_id`, `timestamp`) VALUES (2, 4, 2, '2023-02-16 00:00:00');
INSERT INTO `location_reading` (`sensor_id`, `space_id`, `person_id`, `timestamp`) VALUES (2, 4, 77, '2023-02-11 00:00:00');
INSERT INTO `location_reading` (`sensor_id`, `space_id`, `person_id`, `timestamp`) VALUES (2, 5, 46, '2023-02-16 00:00:00');
INSERT INTO `location_reading` (`sensor_id`, `space_id`, `person_id`, `timestamp`) VALUES (2, 6, 19, '2023-02-01 00:00:00');
INSERT INTO `location_reading` (`sensor_id`, `space_id`, `person_id`, `timestamp`) VALUES (2, 6, 41, '2023-06-10 00:00:00');
INSERT INTO `location_reading` (`sensor_id`, `space_id`, `person_id`, `timestamp`) VALUES (2, 6, 95, '2023-03-12 00:00:00');
INSERT INTO `location_reading` (`sensor_id`, `space_id`, `person_id`, `timestamp`) VALUES (2, 7, 39, '2023-05-16 00:00:00');
INSERT INTO `location_reading` (`sensor_id`, `space_id`, `person_id`, `timestamp`) VALUES (2, 7, 62, '2023-05-11 00:00:00');
INSERT INTO `location_reading` (`sensor_id`, `space_id`, `person_id`, `timestamp`) VALUES (3, 2, 100, '2023-05-06 00:00:00');
INSERT INTO `location_reading` (`sensor_id`, `space_id`, `person_id`, `timestamp`) VALUES (3, 3, 1, '2023-01-07 00:00:00');
INSERT INTO `location_reading` (`sensor_id`, `space_id`, `person_id`, `timestamp`) VALUES (3, 3, 29, '2023-02-21 00:00:00');
INSERT INTO `location_reading` (`sensor_id`, `space_id`, `person_id`, `timestamp`) VALUES (3, 4, 90, '2023-02-16 00:00:00');
INSERT INTO `location_reading` (`sensor_id`, `space_id`, `person_id`, `timestamp`) VALUES (3, 5, 48, '2023-05-16 00:00:00');
INSERT INTO `location_reading` (`sensor_id`, `space_id`, `person_id`, `timestamp`) VALUES (3, 6, 70, '2023-04-01 00:00:00');
INSERT INTO `location_reading` (`sensor_id`, `space_id`, `person_id`, `timestamp`) VALUES (3, 6, 81, '2023-05-26 00:00:00');
INSERT INTO `location_reading` (`sensor_id`, `space_id`, `person_id`, `timestamp`) VALUES (3, 7, 29, '2023-04-06 00:00:00');
INSERT INTO `location_reading` (`sensor_id`, `space_id`, `person_id`, `timestamp`) VALUES (3, 7, 94, '2023-01-27 00:00:00');
INSERT INTO `location_reading` (`sensor_id`, `space_id`, `person_id`, `timestamp`) VALUES (3, 7, 97, '2023-01-12 00:00:00');
INSERT INTO `location_reading` (`sensor_id`, `space_id`, `person_id`, `timestamp`) VALUES (4, 1, 37, '2023-04-26 00:00:00');
INSERT INTO `location_reading` (`sensor_id`, `space_id`, `person_id`, `timestamp`) VALUES (4, 2, 20, '2023-04-26 00:00:00');
INSERT INTO `location_reading` (`sensor_id`, `space_id`, `person_id`, `timestamp`) VALUES (4, 2, 78, '2023-05-31 00:00:00');
INSERT INTO `location_reading` (`sensor_id`, `space_id`, `person_id`, `timestamp`) VALUES (4, 2, 90, '2023-03-22 00:00:00');
INSERT INTO `location_reading` (`sensor_id`, `space_id`, `person_id`, `timestamp`) VALUES (4, 3, 52, '2023-05-16 00:00:00');
INSERT INTO `location_reading` (`sensor_id`, `space_id`, `person_id`, `timestamp`) VALUES (4, 3, 83, '2023-05-06 00:00:00');
INSERT INTO `location_reading` (`sensor_id`, `space_id`, `person_id`, `timestamp`) VALUES (4, 4, 40, '2023-02-06 00:00:00');
INSERT INTO `location_reading` (`sensor_id`, `space_id`, `person_id`, `timestamp`) VALUES (4, 4, 43, '2023-04-21 00:00:00');
INSERT INTO `location_reading` (`sensor_id`, `space_id`, `person_id`, `timestamp`) VALUES (4, 4, 77, '2023-02-06 00:00:00');
INSERT INTO `location_reading` (`sensor_id`, `space_id`, `person_id`, `timestamp`) VALUES (4, 7, 16, '2023-05-11 00:00:00');
INSERT INTO `location_reading` (`sensor_id`, `space_id`, `person_id`, `timestamp`) VALUES (4, 7, 75, '2023-04-21 00:00:00');
INSERT INTO `location_reading` (`sensor_id`, `space_id`, `person_id`, `timestamp`) VALUES (4, 7, 81, '2023-04-21 00:00:00');
INSERT INTO `location_reading` (`sensor_id`, `space_id`, `person_id`, `timestamp`) VALUES (5, 1, 37, '2023-03-17 00:00:00');
INSERT INTO `location_reading` (`sensor_id`, `space_id`, `person_id`, `timestamp`) VALUES (5, 3, 85, '2023-05-31 00:00:00');
INSERT INTO `location_reading` (`sensor_id`, `space_id`, `person_id`, `timestamp`) VALUES (5, 4, 93, '2023-02-11 00:00:00');
INSERT INTO `location_reading` (`sensor_id`, `space_id`, `person_id`, `timestamp`) VALUES (5, 5, 64, '2023-01-27 00:00:00');
INSERT INTO `location_reading` (`sensor_id`, `space_id`, `person_id`, `timestamp`) VALUES (5, 7, 29, '2023-05-01 00:00:00');
INSERT INTO `location_reading` (`sensor_id`, `space_id`, `person_id`, `timestamp`) VALUES (6, 1, 97, '2023-01-22 00:00:00');
INSERT INTO `location_reading` (`sensor_id`, `space_id`, `person_id`, `timestamp`) VALUES (6, 2, 23, '2023-05-16 00:00:00');
INSERT INTO `location_reading` (`sensor_id`, `space_id`, `person_id`, `timestamp`) VALUES (6, 2, 69, '2023-05-21 00:00:00');
INSERT INTO `location_reading` (`sensor_id`, `space_id`, `person_id`, `timestamp`) VALUES (6, 3, 4, '2023-05-11 00:00:00');
INSERT INTO `location_reading` (`sensor_id`, `space_id`, `person_id`, `timestamp`) VALUES (6, 3, 36, '2023-05-16 00:00:00');
INSERT INTO `location_reading` (`sensor_id`, `space_id`, `person_id`, `timestamp`) VALUES (6, 4, 14, '2023-03-12 00:00:00');
INSERT INTO `location_reading` (`sensor_id`, `space_id`, `person_id`, `timestamp`) VALUES (6, 4, 84, '2023-01-17 00:00:00');
INSERT INTO `location_reading` (`sensor_id`, `space_id`, `person_id`, `timestamp`) VALUES (6, 5, 56, '2023-02-11 00:00:00');
INSERT INTO `location_reading` (`sensor_id`, `space_id`, `person_id`, `timestamp`) VALUES (6, 6, 20, '2023-01-27 00:00:00');
INSERT INTO `location_reading` (`sensor_id`, `space_id`, `person_id`, `timestamp`) VALUES (6, 6, 83, '2023-01-07 00:00:00');
INSERT INTO `location_reading` (`sensor_id`, `space_id`, `person_id`, `timestamp`) VALUES (6, 7, 53, '2023-01-27 00:00:00');
INSERT INTO `location_reading` (`sensor_id`, `space_id`, `person_id`, `timestamp`) VALUES (7, 2, 25, '2023-03-03 00:00:00');
INSERT INTO `location_reading` (`sensor_id`, `space_id`, `person_id`, `timestamp`) VALUES (7, 4, 26, '2023-04-21 00:00:00');
INSERT INTO `location_reading` (`sensor_id`, `space_id`, `person_id`, `timestamp`) VALUES (7, 5, 12, '2023-01-02 00:00:00');
INSERT INTO `location_reading` (`sensor_id`, `space_id`, `person_id`, `timestamp`) VALUES (7, 5, 42, '2023-04-01 00:00:00');
INSERT INTO `location_reading` (`sensor_id`, `space_id`, `person_id`, `timestamp`) VALUES (7, 5, 42, '2023-05-11 00:00:00');
INSERT INTO `location_reading` (`sensor_id`, `space_id`, `person_id`, `timestamp`) VALUES (7, 5, 80, '2023-01-12 00:00:00');
INSERT INTO `location_reading` (`sensor_id`, `space_id`, `person_id`, `timestamp`) VALUES (7, 6, 13, '2023-05-06 00:00:00');
INSERT INTO `location_reading` (`sensor_id`, `space_id`, `person_id`, `timestamp`) VALUES (7, 6, 58, '2023-05-26 00:00:00');
INSERT INTO `location_reading` (`sensor_id`, `space_id`, `person_id`, `timestamp`) VALUES (7, 6, 62, '2023-04-26 00:00:00');
INSERT INTO `location_reading` (`sensor_id`, `space_id`, `person_id`, `timestamp`) VALUES (7, 7, 75, '2023-02-16 00:00:00');
INSERT INTO `location_reading` (`sensor_id`, `space_id`, `person_id`, `timestamp`) VALUES (8, 3, 85, '2023-03-27 00:00:00');
INSERT INTO `location_reading` (`sensor_id`, `space_id`, `person_id`, `timestamp`) VALUES (8, 3, 100, '2023-03-17 00:00:00');
INSERT INTO `location_reading` (`sensor_id`, `space_id`, `person_id`, `timestamp`) VALUES (8, 4, 23, '2023-03-17 00:00:00');
INSERT INTO `location_reading` (`sensor_id`, `space_id`, `person_id`, `timestamp`) VALUES (8, 4, 38, '2023-03-17 00:00:00');
INSERT INTO `location_reading` (`sensor_id`, `space_id`, `person_id`, `timestamp`) VALUES (8, 4, 41, '2023-05-31 00:00:00');
INSERT INTO `location_reading` (`sensor_id`, `space_id`, `person_id`, `timestamp`) VALUES (8, 5, 33, '2023-04-26 00:00:00');
INSERT INTO `location_reading` (`sensor_id`, `space_id`, `person_id`, `timestamp`) VALUES (8, 6, 10, '2023-03-12 00:00:00');
INSERT INTO `location_reading` (`sensor_id`, `space_id`, `person_id`, `timestamp`) VALUES (8, 6, 49, '2023-04-21 00:00:00');
INSERT INTO `location_reading` (`sensor_id`, `space_id`, `person_id`, `timestamp`) VALUES (8, 7, 8, '2023-03-22 00:00:00');
INSERT INTO `location_reading` (`sensor_id`, `space_id`, `person_id`, `timestamp`) VALUES (8, 7, 62, '2023-01-27 00:00:00');
INSERT INTO `location_reading` (`sensor_id`, `space_id`, `person_id`, `timestamp`) VALUES (8, 7, 78, '2023-02-26 00:00:00');
INSERT INTO `location_reading` (`sensor_id`, `space_id`, `person_id`, `timestamp`) VALUES (8, 7, 98, '2023-01-22 00:00:00');
INSERT INTO `location_reading` (`sensor_id`, `space_id`, `person_id`, `timestamp`) VALUES (9, 1, 54, '2023-02-21 00:00:00');
INSERT INTO `location_reading` (`sensor_id`, `space_id`, `person_id`, `timestamp`) VALUES (9, 1, 65, '2023-04-26 00:00:00');
INSERT INTO `location_reading` (`sensor_id`, `space_id`, `person_id`, `timestamp`) VALUES (9, 2, 28, '2023-05-06 00:00:00');
INSERT INTO `location_reading` (`sensor_id`, `space_id`, `person_id`, `timestamp`) VALUES (9, 2, 81, '2023-01-02 00:00:00');
INSERT INTO `location_reading` (`sensor_id`, `space_id`, `person_id`, `timestamp`) VALUES (9, 3, 23, '2023-05-16 00:00:00');
INSERT INTO `location_reading` (`sensor_id`, `space_id`, `person_id`, `timestamp`) VALUES (9, 3, 54, '2023-03-22 00:00:00');
INSERT INTO `location_reading` (`sensor_id`, `space_id`, `person_id`, `timestamp`) VALUES (9, 6, 69, '2023-01-12 00:00:00');
INSERT INTO `location_reading` (`sensor_id`, `space_id`, `person_id`, `timestamp`) VALUES (9, 6, 96, '2023-03-07 00:00:00');
INSERT INTO `location_reading` (`sensor_id`, `space_id`, `person_id`, `timestamp`) VALUES (9, 6, 100, '2023-05-21 00:00:00');
INSERT INTO `location_reading` (`sensor_id`, `space_id`, `person_id`, `timestamp`) VALUES (10, 2, 25, '2023-02-01 00:00:00');
INSERT INTO `location_reading` (`sensor_id`, `space_id`, `person_id`, `timestamp`) VALUES (10, 2, 92, '2023-05-16 00:00:00');
INSERT INTO `location_reading` (`sensor_id`, `space_id`, `person_id`, `timestamp`) VALUES (10, 3, 75, '2023-03-12 00:00:00');
INSERT INTO `location_reading` (`sensor_id`, `space_id`, `person_id`, `timestamp`) VALUES (10, 4, 24, '2023-02-26 00:00:00');
INSERT INTO `location_reading` (`sensor_id`, `space_id`, `person_id`, `timestamp`) VALUES (10, 4, 93, '2023-05-26 00:00:00');
INSERT INTO `location_reading` (`sensor_id`, `space_id`, `person_id`, `timestamp`) VALUES (10, 5, 48, '2023-02-11 00:00:00');
INSERT INTO `location_reading` (`sensor_id`, `space_id`, `person_id`, `timestamp`) VALUES (10, 5, 95, '2023-02-06 00:00:00');
INSERT INTO `location_reading` (`sensor_id`, `space_id`, `person_id`, `timestamp`) VALUES (10, 6, 63, '2023-05-31 00:00:00');
INSERT INTO `location_reading` (`sensor_id`, `space_id`, `person_id`, `timestamp`) VALUES (10, 7, 92, '2023-04-16 00:00:00');
INSERT INTO `location_reading` (`sensor_id`, `space_id`, `person_id`, `timestamp`) VALUES (10, 7, 94, '2023-05-11 00:00:00');
INSERT INTO `location_reading` (`sensor_id`, `space_id`, `person_id`, `timestamp`) VALUES (8, 5, 37, '2023-04-01 00:00:00');
INSERT INTO `location_reading` (`sensor_id`, `space_id`, `person_id`, `timestamp`) VALUES (8, 2, 37, '2023-04-01 00:00:00');
INSERT INTO `location_reading` (`sensor_id`, `space_id`, `person_id`, `timestamp`) VALUES (8, 7, 37, '2023-04-01 00:00:00');




#
# TABLE STRUCTURE FOR: location_sensor
#


DROP TABLE IF EXISTS `attends`;

CREATE TABLE `attends` (
  `card_id` int(11) NOT NULL,
  `event_id` int(11) NOT NULL,
  PRIMARY KEY (`card_id`,`event_id`),
  KEY `event_id` (`event_id`),
  CONSTRAINT `attends_ibfk_1` FOREIGN KEY (`card_id`) REFERENCES `member` (`card_id`),
  CONSTRAINT `attends_ibfk_2` FOREIGN KEY (`event_id`) REFERENCES `events` (`event_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

INSERT INTO `attends` (`card_id`, `event_id`) VALUES (22,	1	);
INSERT INTO `attends` (`card_id`, `event_id`) VALUES (22,	2	);
INSERT INTO `attends` (`card_id`, `event_id`) VALUES (22,	3	);
INSERT INTO `attends` (`card_id`, `event_id`) VALUES (22,	4	);
INSERT INTO `attends` (`card_id`, `event_id`) VALUES (22,	5	);
INSERT INTO `attends` (`card_id`, `event_id`) VALUES (22,	6	);
INSERT INTO `attends` (`card_id`, `event_id`) VALUES (22,	7	);
INSERT INTO `attends` (`card_id`, `event_id`) VALUES (22,	8	);
INSERT INTO `attends` (`card_id`, `event_id`) VALUES (22,	9	);
INSERT INTO `attends` (`card_id`, `event_id`) VALUES (22,	10	);
INSERT INTO `attends` (`card_id`, `event_id`) VALUES (22,	11	);
INSERT INTO `attends` (`card_id`, `event_id`) VALUES (22,	12	);
INSERT INTO `attends` (`card_id`, `event_id`) VALUES (22,	13	);
INSERT INTO `attends` (`card_id`, `event_id`) VALUES (22,	14	);
INSERT INTO `attends` (`card_id`, `event_id`) VALUES (22,	15	);
INSERT INTO `attends` (`card_id`, `event_id`) VALUES (22,	16	);
INSERT INTO `attends` (`card_id`, `event_id`) VALUES (22,	17	);
INSERT INTO `attends` (`card_id`, `event_id`) VALUES (22,	18	);
INSERT INTO `attends` (`card_id`, `event_id`) VALUES (22,	19	);
INSERT INTO `attends` (`card_id`, `event_id`) VALUES (22,	20	);
INSERT INTO `attends` (`card_id`, `event_id`) VALUES (22,	21	);
INSERT INTO `attends` (`card_id`, `event_id`) VALUES (22,	22	);
INSERT INTO `attends` (`card_id`, `event_id`) VALUES (22,	23	);
INSERT INTO `attends` (`card_id`, `event_id`) VALUES (22,	24	);
INSERT INTO `attends` (`card_id`, `event_id`) VALUES (22,	25	);
INSERT INTO `attends` (`card_id`, `event_id`) VALUES (22,	26	);
INSERT INTO `attends` (`card_id`, `event_id`) VALUES (22,	27	);
INSERT INTO `attends` (`card_id`, `event_id`) VALUES (22,	28	);
INSERT INTO `attends` (`card_id`, `event_id`) VALUES (22,	29	);
INSERT INTO `attends` (`card_id`, `event_id`) VALUES (22,	30	);
INSERT INTO `attends` (`card_id`, `event_id`) VALUES (22,	31	);
INSERT INTO `attends` (`card_id`, `event_id`) VALUES (22,	32	);
INSERT INTO `attends` (`card_id`, `event_id`) VALUES (22,	33	);
INSERT INTO `attends` (`card_id`, `event_id`) VALUES (22,	34	);
INSERT INTO `attends` (`card_id`, `event_id`) VALUES (22,	35	);
INSERT INTO `attends` (`card_id`, `event_id`) VALUES (22,	36	);
INSERT INTO `attends` (`card_id`, `event_id`) VALUES (22,	37	);
INSERT INTO `attends` (`card_id`, `event_id`) VALUES (22,	38	);
INSERT INTO `attends` (`card_id`, `event_id`) VALUES (22,	39	);
INSERT INTO `attends` (`card_id`, `event_id`) VALUES (22,	40	);
INSERT INTO `attends` (`card_id`, `event_id`) VALUES (22,	41	);
INSERT INTO `attends` (`card_id`, `event_id`) VALUES (22,	42	);
INSERT INTO `attends` (`card_id`, `event_id`) VALUES (22,	43	);
INSERT INTO `attends` (`card_id`, `event_id`) VALUES (22,	44	);
INSERT INTO `attends` (`card_id`, `event_id`) VALUES (22,	45	);
INSERT INTO `attends` (`card_id`, `event_id`) VALUES (22,	46	);
INSERT INTO `attends` (`card_id`, `event_id`) VALUES (22,	47	);
INSERT INTO `attends` (`card_id`, `event_id`) VALUES (22,	48	);
INSERT INTO `attends` (`card_id`, `event_id`) VALUES (22,	49	);
INSERT INTO `attends` (`card_id`, `event_id`) VALUES (22,	50	);

INSERT INTO `attends` (`card_id`, `event_id`) VALUES (25, 30);
INSERT INTO `attends` (`card_id`, `event_id`) VALUES (26, 23);
INSERT INTO `attends` (`card_id`, `event_id`) VALUES (26, 34);
INSERT INTO `attends` (`card_id`, `event_id`) VALUES (26, 44);
INSERT INTO `attends` (`card_id`, `event_id`) VALUES (27, 8);
INSERT INTO `attends` (`card_id`, `event_id`) VALUES (27, 47);
INSERT INTO `attends` (`card_id`, `event_id`) VALUES (30, 24);
INSERT INTO `attends` (`card_id`, `event_id`) VALUES (31, 29);
INSERT INTO `attends` (`card_id`, `event_id`) VALUES (32, 13);
INSERT INTO `attends` (`card_id`, `event_id`) VALUES (33, 29);
INSERT INTO `attends` (`card_id`, `event_id`) VALUES (33, 38);
INSERT INTO `attends` (`card_id`, `event_id`) VALUES (34, 2);
INSERT INTO `attends` (`card_id`, `event_id`) VALUES (34, 11);
INSERT INTO `attends` (`card_id`, `event_id`) VALUES (35, 19);
INSERT INTO `attends` (`card_id`, `event_id`) VALUES (35, 27);
INSERT INTO `attends` (`card_id`, `event_id`) VALUES (35, 36);
INSERT INTO `attends` (`card_id`, `event_id`) VALUES (36, 1);
INSERT INTO `attends` (`card_id`, `event_id`) VALUES (36, 35);
INSERT INTO `attends` (`card_id`, `event_id`) VALUES (37, 1);
INSERT INTO `attends` (`card_id`, `event_id`) VALUES (39, 49);
INSERT INTO `attends` (`card_id`, `event_id`) VALUES (40, 30);
INSERT INTO `attends` (`card_id`, `event_id`) VALUES (41, 38);
INSERT INTO `attends` (`card_id`, `event_id`) VALUES (42, 4);
INSERT INTO `attends` (`card_id`, `event_id`) VALUES (42, 7);
INSERT INTO `attends` (`card_id`, `event_id`) VALUES (42, 16);
INSERT INTO `attends` (`card_id`, `event_id`) VALUES (44, 40);
INSERT INTO `attends` (`card_id`, `event_id`) VALUES (44, 48);
INSERT INTO `attends` (`card_id`, `event_id`) VALUES (47, 34);
INSERT INTO `attends` (`card_id`, `event_id`) VALUES (48, 33);
INSERT INTO `attends` (`card_id`, `event_id`) VALUES (50, 39);
INSERT INTO `attends` (`card_id`, `event_id`) VALUES (50, 46);
INSERT INTO `attends` (`card_id`, `event_id`) VALUES (52, 14);
INSERT INTO `attends` (`card_id`, `event_id`) VALUES (52, 31);
INSERT INTO `attends` (`card_id`, `event_id`) VALUES (53, 39);
INSERT INTO `attends` (`card_id`, `event_id`) VALUES (53, 43);
INSERT INTO `attends` (`card_id`, `event_id`) VALUES (54, 13);
INSERT INTO `attends` (`card_id`, `event_id`) VALUES (54, 26);
INSERT INTO `attends` (`card_id`, `event_id`) VALUES (56, 47);
INSERT INTO `attends` (`card_id`, `event_id`) VALUES (58, 7);
INSERT INTO `attends` (`card_id`, `event_id`) VALUES (58, 32);
INSERT INTO `attends` (`card_id`, `event_id`) VALUES (59, 40);
INSERT INTO `attends` (`card_id`, `event_id`) VALUES (60, 9);
INSERT INTO `attends` (`card_id`, `event_id`) VALUES (61, 2);
INSERT INTO `attends` (`card_id`, `event_id`) VALUES (64, 7);
INSERT INTO `attends` (`card_id`, `event_id`) VALUES (64, 28);
INSERT INTO `attends` (`card_id`, `event_id`) VALUES (66, 6);
INSERT INTO `attends` (`card_id`, `event_id`) VALUES (66, 46);
INSERT INTO `attends` (`card_id`, `event_id`) VALUES (67, 23);
INSERT INTO `attends` (`card_id`, `event_id`) VALUES (67, 30);
INSERT INTO `attends` (`card_id`, `event_id`) VALUES (68, 6);
INSERT INTO `attends` (`card_id`, `event_id`) VALUES (68, 7);
INSERT INTO `attends` (`card_id`, `event_id`) VALUES (68, 15);
INSERT INTO `attends` (`card_id`, `event_id`) VALUES (68, 34);
INSERT INTO `attends` (`card_id`, `event_id`) VALUES (70, 18);
INSERT INTO `attends` (`card_id`, `event_id`) VALUES (71, 44);
INSERT INTO `attends` (`card_id`, `event_id`) VALUES (72, 9);
INSERT INTO `attends` (`card_id`, `event_id`) VALUES (72, 31);
INSERT INTO `attends` (`card_id`, `event_id`) VALUES (72, 34);
INSERT INTO `attends` (`card_id`, `event_id`) VALUES (74, 25);
INSERT INTO `attends` (`card_id`, `event_id`) VALUES (74, 32);
INSERT INTO `attends` (`card_id`, `event_id`) VALUES (74, 33);
INSERT INTO `attends` (`card_id`, `event_id`) VALUES (75, 8);
INSERT INTO `attends` (`card_id`, `event_id`) VALUES (75, 18);
INSERT INTO `attends` (`card_id`, `event_id`) VALUES (77, 11);
INSERT INTO `attends` (`card_id`, `event_id`) VALUES (77, 14);
INSERT INTO `attends` (`card_id`, `event_id`) VALUES (77, 38);
INSERT INTO `attends` (`card_id`, `event_id`) VALUES (77, 39);
INSERT INTO `attends` (`card_id`, `event_id`) VALUES (77, 47);
INSERT INTO `attends` (`card_id`, `event_id`) VALUES (78, 9);
INSERT INTO `attends` (`card_id`, `event_id`) VALUES (79, 22);
INSERT INTO `attends` (`card_id`, `event_id`) VALUES (80, 7);
INSERT INTO `attends` (`card_id`, `event_id`) VALUES (80, 24);
INSERT INTO `attends` (`card_id`, `event_id`) VALUES (80, 26);
INSERT INTO `attends` (`card_id`, `event_id`) VALUES (80, 30);
INSERT INTO `attends` (`card_id`, `event_id`) VALUES (83, 7);
INSERT INTO `attends` (`card_id`, `event_id`) VALUES (83, 31);
INSERT INTO `attends` (`card_id`, `event_id`) VALUES (83, 34);
INSERT INTO `attends` (`card_id`, `event_id`) VALUES (85, 28);
INSERT INTO `attends` (`card_id`, `event_id`) VALUES (85, 42);
INSERT INTO `attends` (`card_id`, `event_id`) VALUES (86, 11);
INSERT INTO `attends` (`card_id`, `event_id`) VALUES (86, 12);
INSERT INTO `attends` (`card_id`, `event_id`) VALUES (86, 27);
INSERT INTO `attends` (`card_id`, `event_id`) VALUES (86, 38);
INSERT INTO `attends` (`card_id`, `event_id`) VALUES (88, 29);
INSERT INTO `attends` (`card_id`, `event_id`) VALUES (90, 29);
INSERT INTO `attends` (`card_id`, `event_id`) VALUES (90, 38);
INSERT INTO `attends` (`card_id`, `event_id`) VALUES (91, 4);
INSERT INTO `attends` (`card_id`, `event_id`) VALUES (92, 25);
INSERT INTO `attends` (`card_id`, `event_id`) VALUES (92, 38);
INSERT INTO `attends` (`card_id`, `event_id`) VALUES (93, 30);
INSERT INTO `attends` (`card_id`, `event_id`) VALUES (93, 44);
INSERT INTO `attends` (`card_id`, `event_id`) VALUES (94, 29);
INSERT INTO `attends` (`card_id`, `event_id`) VALUES (94, 42);
INSERT INTO `attends` (`card_id`, `event_id`) VALUES (95, 5);
INSERT INTO `attends` (`card_id`, `event_id`) VALUES (99, 16);
INSERT INTO `attends` (`card_id`, `event_id`) VALUES (100, 8);
INSERT INTO `attends` (`card_id`, `event_id`) VALUES (100, 20);
INSERT INTO `attends` (`card_id`, `event_id`) VALUES (100, 21);
INSERT INTO `attends` (`card_id`, `event_id`) VALUES (91, 43);
INSERT INTO `attends` (`card_id`, `event_id`) VALUES (83, 43);

INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (11, 3, 49, '2023-02-02 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (11, 12, 49, '2023-02-03 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (11, 24, 49, '2023-02-04 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (11, 25, 49, '2023-02-05 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (11, 36, 49, '2023-02-06 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (11, 39, 49, '2023-02-07 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (11, 41, 49, '2023-02-08 00:00:00');
INSERT INTO `usage_reading` (`sensor_id`, `equipment_id`, `card_id`, `timestamp`) VALUES (11, 50, 49, '2023-02-09 00:00:00');

CREATE USER 'tester'@'%' IDENTIFIED BY 'test123';
GRANT SELECT ON cs122a.* TO 'tester'@'%';
FLUSH PRIVILEGES;
