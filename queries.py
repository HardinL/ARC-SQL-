import mysql.connector
from constants import Constants

class Queries(object):
    """Database queries"""
    connection = mysql.connector.connect(user=Constants.USER, password=Constants.PASSWORD, database=Constants.DATABASE)
    cursor = connection.cursor()
    
    """Retrieve the names and genders of all people associated with ARC (i.e., members, employees, etc.)"""
    query1 = """
        SELECT 
            name, gender 
        FROM 
            person;
    """

    """List the names and departments of all “Faculty” members who are also members of ARC. """
    # query2 = 

    """Find the names of the people who were present in either the weight room or the cardio room on 2023-04-01."""
    # query3 = 

    """Find the names of the people who have attended all events."""
    # query4 = 

    """List the ID of events whose capacity have reached the maximum capacity of their associated space."""
    # query5 = 

    """Find the names of students who have used all the equipment located in the cardio room. """
    # query6 = 

    """List the equipment ids and types for equipment that is currently available."""
    # query7 = 

    """Find names of all employees in ARC."""
    # query8 = 

    """Retrieve the names of people who have attended an event in the yoga studio"""
    # query9 = 

    """Find all family members who have attended ‘Summer Splash Fest’. """
    # query10 = 

    """Calculate the average hourly rate paid to all employees who are of student type at ARC"""
    # query11 = 

    """Find the name of the Trainer(s) with the 2nd highest average hourly rate"""
    # query12 = 

    """Find the ID of university affiliate(s) that have the highest number of family members that are ARC’s members."""
    # query13 = 

    """Find the ID of university affiliate(s) that attends the most events"""
    # query14 = 

    """Find the ID of space(s) that contains the least number of equipment"""
    # query15 = 

    """Calculate the total number of days spent by Mekhi Sporer in the weight room."""
    # query16 = 

    """Find the names of member(s) who spent the most time(in days) in the cardio room in the month of May"""
    # query17 = 

    """Find the spaces which have the lowest average occupancy per event."""
    # query18 = 
    
    cursor.execute(query1)
    print(cursor.fetchall())