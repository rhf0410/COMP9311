--Q1:
drop type if exists RoomRecord cascade;
create type RoomRecord as (valid_room_number integer, bigger_room_number integer);

create or replace function Q1(course_id integer)
    returns RoomRecord
as $$
declare 
	rrecord RoomRecord;
	course_id integer;
	room_num integer := 0;
	room_num2 integer := 0;
begin
    select course into course_id
	from course_enrolments
	where course = $1;
	if not found then
		raise Exception 'INVALID COURSEID';
	end if;
	select count(a.id) into room_num
	from rooms a, 
	(
		SELECT
		count( a.student ) as total_num
	FROM
		course_enrolments a 
	WHERE
		a.course = $1
	)t
	where a.capacity >= t.total_num;
	SELECT
	count( a.id ) into room_num2 
	FROM
		rooms a,
		( SELECT count( a.student ) AS total_num FROM course_enrolments a WHERE a.course = $1) t1,
		( SELECT count( a.student ) AS total_num FROM course_enrolment_waitlist a WHERE a.course = $1) t2 
	WHERE
		a.capacity >= t1.total_num 
		AND a.capacity >= t2.total_num;
	rrecord := (room_num, room_num2);
	return rrecord;
end;
$$ language plpgsql;


--Q2:
CREATE 
	OR REPLACE VIEW highest_grade AS (
	SELECT
		a.course AS course_id,
		max( a.mark ) AS max 
	FROM
		course_enrolments a 
	WHERE
		a.mark IS NOT NULL 
	GROUP BY
		a.course 
	);
CREATE 
	OR REPLACE FUNCTION median ( INTEGER, INTEGER, INTEGER ) RETURNS INTEGER AS $$ SELECT
	avg( t1.mark ) :: INTEGER 
FROM
	(
	SELECT
		row_number ( ) over ( ORDER BY a.mark ) AS row_index,
		a.mark 
	FROM
		course_enrolments a
		JOIN course_staff b ON a.course = b.course 
	WHERE
		a.course = $2 
		AND b.staff = $3 
		AND a.mark IS NOT NULL 
	ORDER BY
		a.mark 
	) t1,
	(
	SELECT
		max( t.row_index ) AS max_index 
	FROM
		(
		SELECT
			row_number ( ) over ( ORDER BY a.mark ) AS row_index,
			a.mark 
		FROM
			course_enrolments a
			JOIN course_staff b ON a.course = b.course 
		WHERE
			a.course = $2 
			AND b.staff = $3 
			AND a.mark IS NOT NULL 
		ORDER BY
			a.mark 
		) t 
	) t2 
WHERE
	t1.row_index - 1 IN (
		FLOOR( cast( ( t2.max_index - 1 ) AS NUMERIC ) / 2 ),
		CEIL( cast( ( t2.max_index - 1 ) AS NUMERIC ) / 2 ) 
	);
$$ LANGUAGE SQL;
CREATE AGGREGATE median_num ( INTEGER, INTEGER ) ( SFUNC = median, stype = INTEGER );
DROP type
IF
	EXISTS TeachingRecord CASCADE;
CREATE type TeachingRecord AS (
	cid INTEGER,
	term CHAR ( 4 ),
	CODE CHAR ( 8 ),
	NAME text,
	uoc INTEGER,
	average_mark INTEGER,
	highest_mark INTEGER,
	median_mark INTEGER,
	totalEnrols INTEGER 
);
CREATE 
	OR REPLACE FUNCTION Q2 ( staff_id INTEGER ) RETURNS setof TeachingRecord AS $$ DECLARE
	staff_id INTEGER;
res TeachingRecord;
BEGIN
	SELECT
		staff INTO staff_id 
	FROM
		course_staff 
	WHERE
		staff = $1;
	IF
		NOT found THEN
			raise Exception 'INVALID STAFFID';
		
	END IF;
	FOR res IN SELECT DISTINCT
	a.id AS cid,
	cast(
		substring( cast( d.YEAR AS VARCHAR ), 3, 2 ) || lower( d.term ) AS CHAR ( 4 ) 
	) AS term,
	c.CODE AS CODE,
	c.NAME AS NAME,
	c.uoc AS uoc,
	avg.avg AS average_mark,
	high.max AS highest_mark,
	median_table.median_number AS median_mark,
	stunum.num_stu AS totalEnrols 
	FROM
		courses a
		JOIN course_enrolments b ON a.id = b.course,
		subjects c,
		semesters d,
		( SELECT a.course AS course_id, avg( a.mark ) :: INTEGER AS avg FROM course_enrolments a GROUP BY a.course ) avg,
		highest_grade high,
		(
		SELECT
			a.course AS course_id,
			count( a.student ) AS num_stu 
		FROM
			course_enrolments a 
		WHERE
			a.mark IS NOT NULL 
		GROUP BY
			a.course 
		) stunum,
		(
		SELECT
			a.course AS course,
			median_num ( a.course, $1 ) AS median_number 
		FROM
			course_enrolments a
			JOIN course_staff b ON a.course = b.course 
		WHERE
			b.staff = $1 
		GROUP BY
			a.course 
		) median_table,
		course_staff staff 
	WHERE
		a.SUBJECT = c.id 
		AND a.semester = d.id 
		AND avg.course_id = a.id 
		AND high.course_id = a.id 
		AND stunum.course_id = a.id 
		AND staff.course = a.id 
		AND median_table.course = a.id 
		AND staff.staff = $1 
		AND stunum.num_stu != 0 
	LOOP
			RETURN next res;
		
	END LOOP;
	RETURN;
	
END;
$$ LANGUAGE plpgsql;


--Q3:
CREATE 
	OR REPLACE FUNCTION course_record ( text, INTEGER, INTEGER ) RETURNS text AS $$ DECLARE
	res text;
res_final text := '';
BEGIN
		FOR res IN SELECT
		c.CODE || ', ' || c.NAME || ', ' || d.NAME || ', ' || e.NAME || ', ' || a.mark || chr ( 10 ) 
	FROM
		course_enrolments a
		JOIN courses b ON a.course = b.id
		JOIN subjects c ON b.SUBJECT = c.id
		JOIN semesters d ON b.semester = d.id
		JOIN orgunits e ON e.id = c.offeredBy 
	WHERE
		a.student = $2 
		AND a.mark IS NOT NULL 
		AND (
			c.offeredBy = $3 
			OR c.offeredBy IN (
				WITH recursive r AS (
				SELECT
					a.OWNER,
					a.member 
				FROM
					orgunit_groups a 
				WHERE
					a.OWNER = $3 UNION ALL
				SELECT
					b.* 
				FROM
					orgunit_groups b,
					r 
				WHERE
					b.OWNER = r.member 
				) SELECT DISTINCT
				r.member 
			FROM
				r 
			) 
		) 
	ORDER BY
		a.mark DESC,
		a.course ASC FETCH FIRST 5 rows only
	LOOP
			res_final := res_final || res;
		
	END LOOP;
	RETURN res_final;
	
END;
$$ LANGUAGE plpgsql;
CREATE AGGREGATE course_record_concat ( INTEGER,INTEGER ) ( SFUNC = course_record, stype = text );
DROP type
IF
	EXISTS CourseRecord CASCADE;
CREATE type CourseRecord AS ( unswid INTEGER, NAME text, records text );
CREATE 
	OR REPLACE FUNCTION Q3 ( org_id INTEGER, num_courses INTEGER, min_score INTEGER ) RETURNS setof CourseRecord AS $$ DECLARE
	res CourseRecord;
org_id INTEGER;
BEGIN
	SELECT
		offeredBy INTO org_id 
	FROM
		subjects 
	WHERE
		offeredBy = $1;
	IF
		NOT found THEN
			raise Exception 'INVALID ORGID';
		
	END IF;
	FOR res IN SELECT
	a.unswid AS unswid,
	a.NAME AS student_name,
	course_record_concat ( t.student, $1 ) AS records 
	FROM
		people a,
		(
		SELECT
			a.student AS student 
		FROM
			course_enrolments a
			JOIN courses b ON a.course = b.id
			JOIN subjects c ON b.SUBJECT = c.id 
		WHERE
			c.offeredBy = $1 
			OR c.offeredBy IN (
				WITH recursive r AS (
				SELECT
					a.OWNER,
					a.member 
				FROM
					orgunit_groups a 
				WHERE
					a.OWNER = $1 UNION ALL
				SELECT
					b.* 
				FROM
					orgunit_groups b,
					r 
				WHERE
					b.OWNER = r.member 
				) SELECT DISTINCT
				r.member 
			FROM
				r 
			) 
		GROUP BY
			a.student 
		HAVING
			sum( CASE WHEN a.course IS NOT NULL THEN 1 ELSE 0 END ) > $2 intersect
		SELECT
			a.student AS student 
		FROM
			course_enrolments a
			JOIN courses b ON a.course = b.id
			JOIN subjects c ON b.SUBJECT = c.id 
		WHERE
			c.offeredBy = $1 
			OR c.offeredBy IN (
				WITH recursive r AS (
				SELECT
					a.OWNER,
					a.member 
				FROM
					orgunit_groups a 
				WHERE
					a.OWNER = $1 UNION ALL
				SELECT
					b.* 
				FROM
					orgunit_groups b,
					r 
				WHERE
					b.OWNER = r.member 
				) SELECT DISTINCT
				r.member 
			FROM
				r 
			) 
		GROUP BY
			a.student 
		HAVING
			sum( CASE WHEN a.mark >= $3 THEN 1 ELSE 0 END ) >= 1 
		) t 
	WHERE
		a.id = t.student 
	GROUP BY
		a.unswid,
		a.NAME
	LOOP
			RETURN next res;
		
	END LOOP;
	RETURN;
	
END;
$$ LANGUAGE plpgsql;
