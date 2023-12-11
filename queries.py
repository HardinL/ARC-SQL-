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
    query2 = """
        SELECT
            P.name, U.department
        FROM
            non_student as S, university_affiliate as U, person as P
        WHERE
            S.card_id = U.card_id AND U.card_id = P.card_id AND S.member_type = 'Faculty'
    """

    """Find the names of the people who were present in either the weight room or the cardio room on 2023-04-01."""    
    query3 = """
        SELECT DISTINCT P.name
        FROM person as P, location_reading as LR, space as S
        WHERE P.card_id = LR.person_id AND LR.timestamp = '2023-04-01 00:00:00' AND 
            (S.description = 'cardio room' OR S.description = 'weight room') AND 
            S.space_id = LR.space_id;
    """

    """Find the names of the people who have attended all events."""
    query4 = """
        SELECT p.name
        FROM person p
        WHERE NOT EXISTS (
            SELECT *
            FROM events e
            WHERE NOT EXISTS (
                SELECT *
                FROM attends a
                WHERE a.card_id = p.card_id
                AND a.event_id = e.event_id
            )
        );
    """

        

        # SELECT DISTINCT P.name
        # FROM person as P
        # WHERE (SELECT COUNT(DISTINCT A.event_id) FROM attends as A WHERE A.card_id = P.card_id) = 
        #     (SELECT COUNT(DISTINCT E.event_id) FROM events as E);

    """List the ID of events whose capacity have reached the maximum capacity of their associated space."""
    query5 = """
        SELECT DISTINCT E.event_id
        FROM events as E, space as S
        WHERE E.capacity >= S.max_capacity AND E.space_id = S.space_id;
    """

    """Find the names of students who have used all the equipment located in the cardio room."""
    query6 = """
        SELECT p.name
        FROM person p
        JOIN student s ON p.card_id = s.card_id
        WHERE NOT EXISTS (
            SELECT e.equipment_id
            FROM equipment e
            JOIN space sp ON e.space_id = sp.space_id AND sp.description = 'cardio room'
            WHERE NOT EXISTS (
                SELECT u.equipment_id
                FROM usage_reading u
                WHERE u.equipment_id = e.equipment_id AND u.card_id = p.card_id
            )
        );
    """
    # SELECT p.name
    # FROM person p
    # WHERE p.card_id NOT IN 
    #                 (SELECT u.card_id FROM usage_reading u WHERE u.card_id 
    #                     NOT IN 
    #                     (SELECT ec.card_id 
    #                      FROM student CROSS JOIN (SELECT  FROM equipment e WHERE e.space_id = (SELECT DISTINCT s.space_id FROM space s WHERE s.space_description = 'cardio room')) AS ec
    #                      WHERE ec.card_id, ec.equipment_id 
    #                      NOT IN 
    #                      (SELECT u.card_id, u.equipment_id FROM usage_reading u)));
    
    #  WHERE card_id NOT IN 
    #                     (SELECT card_id 
                        #  FROM person CROSS JOIN )

    """List the equipment ids and types for equipment that is currently available."""
    query7 = """
        SELECT equipment_id, equipment_type
        FROM equipment
        Where is_available = 1
    """

    """Find names of all employees in ARC."""
    query8 = """
        SELECT DISTINCT P.name
        FROM person as P, employee as E
        WHERE P.card_id = E.card_id;
    """

    """Retrieve the names of people who have attended an event in the yoga studio"""
    query9 = """
        SELECT DISTINCT P.name
        FROM person as P, attends as A, events as E, space as S
        WHERE P.card_id = A.card_id AND A.event_id = E.event_id AND E.space_id = S.space_id AND S.description = 'yoga studio';
    """
        
        # SELECT A.card_id
        # FROM attends as A
        # WHERE A.event_id = (SELECT DISTINCT E.event_id
        #                     FROM attends AS A
        #                     INNER JOIN events AS E
        #                     ON A.event_id = E.event_id
        #                     WHERE E.description LIKE 'Summer Splash Fest');

    """Find all family members who have attended `Summer Splash Fest`. """
    query10 = """
        SELECT person.name
        FROM person
        WHERE person.card_id IN

        (SELECT A.card_id
        FROM attends as A
        WHERE A.event_id = (SELECT DISTINCT E.event_id
                        FROM attends AS A
                        INNER JOIN events AS E
                        ON A.event_id = E.event_id
                        WHERE E.description LIKE 'Summer Splash Fest')
        INTERSECT
        SELECT person.card_id
        FROM person
        WHERE card_id IN (SELECT DISTINCT family.card_id
                        FROM family INNER JOIN attends ON family.card_id));
    """
    
    """Calculate the average hourly rate paid to all employees who are of student type at ARC"""
    query11 = """
        SELECT AVG(E.salary_hour)
        FROM employee as E
        WHERE E.employee_type = 'student';
    """

    """Find the name of the Trainer(s) with the 2nd highest average hourly rate"""
    # `Trainer` (`person_id`, `credentials`)
    # `employee` (`card_id`, `schedule`, `employee_type`, `salary_hour`)
    # `person` (`card_id`, `name`, `dob`, `gender`)

    query12 = """
        WITH AVG_SALARY AS (
            SELECT E.card_id, AVG(E.salary_hour) AS avg_salary
            FROM employee as E, Trainer as T
            WHERE E.card_id = T.person_id
            GROUP BY E.card_id
            ORDER BY avg_salary DESC)
        SELECT P.name
        FROM AVG_SALARY AS A, person AS P
        WHERE A.card_id = P.card_id AND A.avg_salary = (SELECT DISTINCT avg_salary FROM AVG_SALARY LIMIT 1,1);
    """

    """Find the ID of university affiliate(s) that have the highest number of family members that are ARC`s members."""
    query13 = """
        SELECT familyof
        FROM (
        SELECT familyof, COUNT(*) AS familyCount
        FROM family
        GROUP BY familyof
        ) AS subquery
        WHERE familyCount = (
        SELECT MAX(familyCount) 
        FROM (
            SELECT COUNT(*) AS familyCount
            FROM family
            GROUP BY familyof
        ) AS maxSubquery
        )

    """

    """Find the ID of university affiliate(s) that attends the most events"""
    query14 = """
        WITH MOST_EVENTS AS (
            SELECT card_id, COUNT(event_id) AS num_events
            FROM attends
            GROUP BY card_id
            ORDER BY num_events DESC)
        SELECT card_id
        FROM MOST_EVENTS
        WHERE num_events = (SELECT MAX(num_events) FROM MOST_EVENTS);
    """

    """Find the ID of space(s) that contains the highest number of equipment"""
    # PROBABLY WRONG - Testcase gives most equipment not least --> Try gradescope thing later
    query15 = """
        WITH LEAST_EQUIPMENT AS (
            SELECT space_id, COUNT(equipment_id) AS num_equipment
            FROM equipment
            GROUP BY space_id
            ORDER BY num_equipment DESC)
        SELECT space_id
        FROM LEAST_EQUIPMENT
        WHERE num_equipment = (SELECT MAX(num_equipment) FROM LEAST_EQUIPMENT);
    """
        # SELECT *
        # # FROM person INNER JOIN location_reading ON person.card_id = location_reading.person_id AND person.name = "Mekhi Sporer";

        
        
    """Calculate the total number of days spent by Mekhi Sporer in the weight room."""
    query16 = """
        SELECT COUNT(*)
        FROM space 
        INNER JOIN location_reading ON space.space_id = location_reading.space_id 
        WHERE space.description = "weight room" 
        AND location_reading.person_id IN (
            SELECT person.card_id
            FROM person
            WHERE person.name = "Mekhi Sporer"
        );
        
      
    """

    """Find the names of member(s) who spent the most time(in days) in the cardio room in the month of May"""
    query17 = """
        SELECT p.name
        FROM person p, member m, location_reading lr, space s
        WHERE s.description = 'cardio room' AND MONTH(lr.timestamp) = 5 AND p.card_id = m.card_id AND m.card_id = lr.person_id AND lr.space_id = s.space_id
        GROUP BY p.name
        HAVING COUNT(DISTINCT lr.timestamp) = (
            SELECT MAX(day_count)
            FROM (
                SELECT COUNT(DISTINCT lr_sub.timestamp) AS day_count
                FROM person p_sub, member m_sub, location_reading lr_sub, space s_sub
                WHERE s_sub.description = 'cardio room' AND MONTH(lr_sub.timestamp) = 5 AND p_sub.card_id = m_sub.card_id AND m_sub.card_id = lr_sub.person_id AND lr_sub.space_id = s_sub.space_id
                GROUP BY p_sub.card_id
            ) AS subquery
        );
    """

    """Find the spaces which have the lowest average occupancy per event."""
    query18 = """
        SELECT s.description, AVG(subquery.occupancy) AS average_occupancy
        FROM (
            SELECT s.space_id, e.event_id, COUNT(DISTINCT a.card_id) AS occupancy
            FROM space s
            JOIN events e ON s.space_id = e.space_id
            JOIN attends a ON e.event_id = a.event_id
            GROUP BY s.space_id, e.event_id
        ) AS subquery
        JOIN space s ON subquery.space_id = s.space_id
        GROUP BY s.description
        ORDER BY average_occupancy ASC
        LIMIT 1;
    """
    
    cursor.execute(query17)
    print(cursor.fetchall())