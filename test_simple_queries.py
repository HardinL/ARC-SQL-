import unittest
from gradescope_utils.autograder_utils.decorators import weight, number
import mysql.connector
from decimal import Decimal
from queries import Queries
from constants import Constants


class TestSQLQueries(unittest.TestCase):
    def setUp(self):
        self.connection = mysql.connector.connect(user=Constants.USER, password=Constants.PASSWORD,
                                                  database=Constants.DATABASE)
        self.cursor = self.connection.cursor()
        self.queries = Queries()

    def tearDown(self):
        self.cursor.close()
        self.connection.close()

    @weight(2)
    @number(1)
    def test_query_one(self):
        """Retrieve the names and genders of all people associated with ARC (i.e., members, employees, etc.)"""
        self.cursor.execute(self.queries.query1)
        result_intermediate = [('Jessica Flatley', 'm'), ('Hildegard Farrell', 'm'), ('Damon West', 'm'),
                               ('Dillan Lockman', 'm'), ('Prof. Marilou Stamm', 'f'), ('Dr. Jaden Miller', 'f'),
                               ('Darrion Lindgren', 'm'), ('Leif Kohler', 'f'), ('Ardella Casper DDS', 'm'),
                               ('Angelita Carter', 'f'), ('Amya Cole', 'f'), ('Rhea Dare', 'f'), ('Izaiah Funk V', 'f'),
                               ('Dr. Lilian Stark', 'f'), ('Eulah Reynolds DDS', 'f'), ('Louie Bins', 'm'),
                               ('Nelda Stiedemann', 'm'), ('Merritt Romaguera', 'f'), ('Prof. Rowan Cronin', 'm'),
                               ('Coleman Wiegand', 'm'), ('Johnny Hodkiewicz', 'm'), ('Leonora McLaughlin', 'f'),
                               ('Miss Elfrieda McKenzie V', 'm'), ('Prof. Kacie Fritsch', 'f'),
                               ('Flavie Gulgowski PhD', 'f'), ('Mrs. Dianna Connelly DVM', 'm'),
                               ('Prudence Reinger', 'm'), ('Marlee Stanton', 'm'), ('Mr. Jovany Simonis', 'f'),
                               ('Magnus Welch', 'm'), ('Raymundo Larkin', 'm'), ('Tania Purdy V', 'f'),
                               ('Theresia Hintz', 'f'), ('Joel Wintheiser', 'f'), ('Maurine Nitzsche', 'f'),
                               ('Providenci Feeney IV', 'f'), ('Mr. Stuart Ondricka IV', 'm'),
                               ('Dr. Minerva Cormier', 'f'), ('Leon Nolan', 'f'), ('Adrien Spencer', 'f'),
                               ('Dr. Jamarcus Hamill', 'f'), ('Jacinto Quigley', 'f'), ('Tyree Bogan', 'm'),
                               ('Prof. Werner Torphy', 'f'), ('Colin McKenzie', 'f'), ('Jeanie Schaefer', 'm'),
                               ('Terrell Maggio', 'm'), ('Antwan Stoltenberg', 'm'), ('Mellie Hermiston', 'f'),
                               ('Clay Vandervort', 'm'), ('Prof. Rodrigo West V', 'f'), ('Sam Bergstrom', 'm'),
                               ('Mr. Jamir McDermott MD', 'm'), ('Dr. Alexys Hand IV', 'm'),
                               ('Prof. Hildegard Mayer V', 'f'), ('Elmira Daugherty II', 'f'),
                               ('Prof. Orlo Mueller', 'f'), ('Earlene Greenfelder', 'f'), ('Dina McClure', 'm'),
                               ('Dr. Kirsten Erdman', 'f'), ('Mariano Wilkinson', 'f'),
                               ('Mrs. Antonietta Flatley', 'm'), ('Donny Barrows', 'f'), ('Kody Ferry Sr.', 'f'),
                               ('Jensen Beahan', 'f'), ('Prof. Sylvester Williamson PhD', 'f'), ('Rosalyn Fisher', 'f'),
                               ('Prof. Norbert Rath DDS', 'f'), ('Irma Schmidt', 'f'), ('Emilia Howe', 'm'),
                               ('Juston Marks', 'f'), ('Sienna Raynor', 'm'), ('Hildegard Legros', 'm'),
                               ('Miss Letitia Brown', 'f'), ('Ciara Brakus', 'f'), ('Dr. Marc Tillman', 'f'),
                               ('Gilberto Fay', 'm'), ('Mr. Harold Fritsch DDS', 'm'), ('Mohammed Hilpert', 'f'),
                               ('Drew Halvorson', 'm'), ('Zaria Hickle II', 'm'), ('Dina Fadel III', 'f'),
                               ('Mary Volkman', 'm'), ('Dr. Monica Anderson MD', 'f'), ('Enola Hilpert', 'f'),
                               ('Dr. Loy Crooks', 'm'), ('Prof. Ernestine Cummerata', 'f'), ('Bette Auer', 'f'),
                               ('Cornelius Blick', 'f'), ('Prof. Clarissa Zieme V', 'm'), ('Ulises McGlynn', 'm'),
                               ('Ms. Pearl Wilkinson', 'm'), ('Prof. Morton Morar', 'f'),
                               ('Mr. Dillan Breitenberg IV', 'f'), ('Mustafa Bednar', 'm'), ('Mekhi Sporer', 'm'),
                               ('Brook Spencer', 'm'), ('Richie Hagenes Sr.', 'm'), ('Trudie Heidenreich', 'm'),
                               ('Jacinto Considine', 'f')]
        result = self.cursor.fetchall()

        try:
            self.assertCountEqual(result, result_intermediate, "Incorrect Query")
        except:
            raise Exception("Incorrect Query")

    @weight(7)
    @number(2)
    def test_query_two(self):
        """List the names and departments of all “Faculty” members who are also members of ARC. """
        self.cursor.execute(self.queries.query2)

        result_intermediate = [('Mariano Wilkinson', 'Political Science')]

        result = self.cursor.fetchall()

        try:
            self.assertCountEqual(result, result_intermediate, "Incorrect Query")
        except:
            raise Exception("Incorrect Query")

    @weight(7)
    @number(3)
    def test_query_three(self):
        """Find the names of the people who were present in either the weight room or the cardio room on 2023-04-01."""
        self.cursor.execute(self.queries.query3)

        result_intermediate = [('Jacinto Quigley',), ('Mr. Stuart Ondricka IV',)]

        result = self.cursor.fetchall()

        try:
            self.assertCountEqual(result, result_intermediate, "Incorrect Query")
        except:
            raise Exception("Incorrect Query")

    @weight(7)
    @number(4)
    def test_query_four(self):
        """Find the names of the people who have attended all events."""
        self.cursor.execute(self.queries.query4)

        result_intermediate = [('Leonora McLaughlin',)]

        result = self.cursor.fetchall()

        try:
            self.assertCountEqual(result, result_intermediate, "Incorrect Query")
        except:
            raise Exception("Incorrect Query")

    @weight(7)
    @number(5)
    def test_query_five(self):
        """List the ID of events whose capacity have reached the maximum capacity of their associated space."""
        self.cursor.execute(self.queries.query5)

        result_intermediate = [(23,)]

        result = self.cursor.fetchall()

        try:
            self.assertCountEqual(result, result_intermediate, "Incorrect Query")
        except:
            raise Exception("Incorrect Query")

    @weight(7)
    @number(6)
    def test_query_six(self):
        """Find the names of students who have used all the equipment located in the cardio room. """
        self.cursor.execute(self.queries.query6)

        result_intermediate = [('Mellie Hermiston',)]

        result = self.cursor.fetchall()

        try:
            self.assertCountEqual(result, result_intermediate, "Incorrect Query")
        except:
            raise Exception("Incorrect Query")

    @weight(7)
    @number(7)
    def test_query_seven(self):
        """List the equipment ids and types for equipment that is currently available."""
        self.cursor.execute(self.queries.query7)

        result_intermediate = [(2, 'Abdominal Mat'), (7, 'Cable Crossover'), (8, 'Resistance Loop Bands'),
                               (18, 'Elliptical Trainer'), (19, 'Weightlifting Gloves'), (22, 'Sandbags'),
                               (23, 'Power Tower'), (24, 'Push-up Handles'), (28, 'Hand Grippers'),
                               (29, 'Vibrating Fitness Roller'), (30, 'Bench Press'), (33, 'Pilates Reformer'),
                               (36, 'Step Platform'), (38, 'Grip Strengthener'), (39, 'Sandbags'), (41, 'Balance Disc'),
                               (43, 'Spin Bike'), (46, 'Push-up Handles'), (47, 'Step Platform'), (48, 'Hex Bar')]

        result = self.cursor.fetchall()

        try:
            self.assertCountEqual(result, result_intermediate, "Incorrect Query")
        except:
            raise Exception("Incorrect Query")

    @weight(7)
    @number(8)
    def test_query_eight(self):
        """Find names of all employees in ARC."""
        self.cursor.execute(self.queries.query8)

        result_intermediate = [('Jessica Flatley',), ('Hildegard Farrell',), ('Damon West',), ('Dillan Lockman',),
                               ('Prof. Marilou Stamm',), ('Dr. Jaden Miller',), ('Darrion Lindgren',), ('Leif Kohler',),
                               ('Ardella Casper DDS',), ('Angelita Carter',), ('Amya Cole',), ('Rhea Dare',),
                               ('Izaiah Funk V',), ('Dr. Lilian Stark',), ('Eulah Reynolds DDS',), ('Louie Bins',),
                               ('Nelda Stiedemann',), ('Merritt Romaguera',), ('Prof. Rowan Cronin',),
                               ('Coleman Wiegand',)]

        result = self.cursor.fetchall()

        try:
            self.assertCountEqual(result, result_intermediate, "Incorrect Query")
        except:
            raise Exception("Incorrect Query")

    @weight(7)
    @number(9)
    def test_query_nine(self):
        """Retrieve the names of people who have attended an event in the yoga studio"""
        self.cursor.execute(self.queries.query9)

        result_intermediate = [('Leonora McLaughlin',), ('Maurine Nitzsche',), ('Dr. Alexys Hand IV',),
                               ('Drew Halvorson',), ('Flavie Gulgowski PhD',), ('Adrien Spencer',), ('Rosalyn Fisher',),
                               ('Prof. Morton Morar',), ('Mrs. Dianna Connelly DVM',), ('Terrell Maggio',),
                               ('Prof. Norbert Rath DDS',), ('Sienna Raynor',), ('Mary Volkman',),
                               ('Prof. Werner Torphy',)]

        result = self.cursor.fetchall()

        try:
            self.assertCountEqual(result, result_intermediate, "Incorrect Query")
        except:
            raise Exception("Incorrect Query")

    @weight(7)
    @number(10)
    def test_query_ten(self):
        """Find all family members who have attended ‘Summer Splash Fest’. """
        self.cursor.execute(self.queries.query10)

        result_intermediate = [('Mary Volkman',), ('Ulises McGlynn',)]

        result = self.cursor.fetchall()

        try:
            self.assertCountEqual(result, result_intermediate, "Incorrect Query")
        except:
            raise Exception("Incorrect Query")

    @weight(7)
    @number(11)
    def test_query_eleven(self):
        """Calculate the average hourly rate paid to all employees who are of student type at ARC"""
        self.cursor.execute(self.queries.query11)

        result_intermediate = [(Decimal('22.333333'),)]

        result = self.cursor.fetchall()

        try:
            self.assertCountEqual(result, result_intermediate, "Incorrect Query")
        except:
            raise Exception("Incorrect Query")

    @weight(7)
    @number(12)
    def test_query_twelve(self):
        """Find the name of the Trainer(s) with the 2nd highest average hourly rate"""
        self.cursor.execute(self.queries.query12)

        result_intermediate = [('Eulah Reynolds DDS',)]

        result = self.cursor.fetchall()

        try:
            self.assertCountEqual(result, result_intermediate, "Incorrect Query")
        except:
            raise Exception("Incorrect Query")

    @weight(7)
    @number(13)
    def test_query_thirteen(self):
        """Find the ID of university affiliate(s) that have the highest number of family members that are ARC’s members."""
        self.cursor.execute(self.queries.query13)

        result_intermediate = [(26,), (27,), (71,)]

        result = self.cursor.fetchall()

        try:
            self.assertCountEqual(result, result_intermediate, "Incorrect Query")
        except:
            raise Exception("Incorrect Query")

    @weight(7)
    @number(14)
    def test_query_fourteen(self):
        """Find the ID of university affiliate(s) that attends the most events"""
        self.cursor.execute(self.queries.query14)

        result_intermediate = [(22,)]

        result = self.cursor.fetchall()
        try:
            self.assertCountEqual(result, result_intermediate, "Incorrect Query")
        except:
            raise Exception("Incorrect Query")

    @weight(7)
    @number(15)
    def test_query_fifteen(self):
        """Find the ID of space(s) that contains the least number of equipment"""
        self.cursor.execute(self.queries.query15)

        result_intermediate = [(4,)]

        result = self.cursor.fetchall()
        try:
            self.assertCountEqual(result, result_intermediate, "Incorrect Query")
        except:
            raise Exception("Incorrect Query")

    @weight(5)
    @number(16)
    def test_query_sixteen(self):
        """Calculate the total number of days  Mekhi Sporer visited the weight room."""
        self.cursor.execute(self.queries.query16)

        result_intermediate = [(1,)]

        result = self.cursor.fetchall()

        try:
            self.assertCountEqual(result, result_intermediate, "Incorrect Query")
        except:
            raise Exception("Incorrect Query")

    @weight(5)
    @number(17)
    def test_query_seventeen(self):
        """Find the names of member(s) who spent the most of the time(in days) in the cardio room in the month of May"""
        self.cursor.execute(self.queries.query17)

        result_intermediate = [('Leon Nolan',), ('Mrs. Antonietta Flatley',), ('Mr. Jovany Simonis',),
                               ('Mr. Dillan Breitenberg IV',)]

        result = self.cursor.fetchall()

        try:
            self.assertCountEqual(result, result_intermediate, "Incorrect Query")
        except:
            raise Exception("Incorrect Query")

    @weight(5)
    @number(18)
    def test_query_eighteen(self):
        """Find the name and the average occupancy of the space which has the lowest average occupancy per event."""
        self.cursor.execute(self.queries.query18)

        result_intermediate = [('weight room', Decimal('2.5000'))]

        result = self.cursor.fetchall()

        try:
            self.assertCountEqual(result, result_intermediate, "Incorrect Query")
        except:
            raise Exception("Incorrect Query")
