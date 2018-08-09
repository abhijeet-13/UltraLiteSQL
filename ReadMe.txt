--------
Install:
--------
1. cd to the directory code/
2. Open command prompt
3. Run:
     >> g++ main.cpp -o ultralitesql
4. Run:
     >> ./ultralitesql install


--------
Running:
--------
1. Run:
     >> ./ultralitesql

2. Following commands are supported:
     - SHOW TABLES;
     - DROP TABLE [table_name];
     - CREATE TABLE [table_name] (row_id int primary key, ...);
     - INSERT INTO TABLE [table_name] (...) VALUES (...);
     - UPDATE [table_name] SET col = value WHERE cond_col <op> cond_value;
     - SELECT * / [...] FROM [table_name ] WHERE cond_col <op> cond_value;
     - EXIT;

     - DELETE FROM [table_name] WHERE cond_col <op> cond_value;

3. Examples can be copied from below all together to check results.


---------
Examples:
---------

create table students(
	row_id int primary key, 
	name text not null,
	tag tinyint,
	marks smallint,
	height int,
	ssn bigint not null,
	phone bigint,
	submission datetime,
	birthday date,
	weight real,
	best_time double,
	remarks text
);

show tables;

select * from database_columns;

insert into table students values(1, 'Abhijeet', NULL, 97, NULL, 123456789, NULL, NULL, 1991-08-13, 64.5, 10.4657, 'This is the first record');

insert into table students (row_id, name, ssn) values(2, 'Mark', 123456987);

insert into table students (row_id, name, ssn, marks, height, submission, birthday, weight) values(3, 'Simon', 321456987, 87, 178, 2018-03-17_05:23:56, 1992-06-23, 70.5);

insert into table students values(4, 'Kartikey', 2, 98, 175, 546456789, 4692580330, 2018-03-14_03:14:26, 1991-12-05, 64.5, 10.4657, 'This is the first record');

insert into table students values(5, 'Molly', 3, 63, 165, 123445229, NULL, NULL, 1991-08-13, NULL, 12.8566, 'This is the fifth record');

insert into table students values(6, 'Aditi', 2, 56, 168, 145456789, 4695359599, NULL, 1992-07-04, NULL, 13.4557, NULL);

insert into table students values(7, 'Alexa', 2, 82, 178, 123456765, NULL, 2018-03-10_15:22:43, 1993-03-23, 74.5, 9.0457, NULL);

insert into table students values(8, 'Yaswantha', 3, NULL, NULL, 123487589, 4894848894, NULL, NULL, 85, NULL, 'This is the eighth record');

insert into table students values(9, 'Daniel', NULL, 83, 183, 127456789, NULL, NULL, 1990-10-13, 100.25, 10.5253, NULL);

insert into table students values(10, 'Mark', NULL, 79, 179, 123453539, NULL, NULL, 1989-08-14, 80.5, NULL, NULL);

select name, height, weight from students where height > 170;

select * from students where phone is not null;

select * from students where remarks is null;

select name, marks from students where birthday >= 1991-08-13;

select * from students where weight < 80.0;

select ssn, name, remarks, marks from students where row_id <= 5;

select ssn, ssn, ssn from students;

update students set name = 'mauli' where name = 'molly';

update students set tag = 4 where tag is null;

update students set tag = 6;

update students set marks = NULL;

delete from students where weight is null;

delete from students where birthday = 1991-08-13;

delete from students where row_id >= 9;

delete from students;

drop table students;

show tables;

select * from database_columns;

exit;