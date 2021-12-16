


--drop table Client
create table Client
(ID varchar(50) primary key,
 name varchar(200),
 surname varchar(200));

 --drop table Service
 create table Service
(ID integer primary key,
 server_configuration  varchar(200));


 --drop table Order_
 create table Order_
(ID integer primary key,
 service_id   integer,
 client_id varchar(50),
 service_start_date datetime,
 service_end_date datetime,
 price float,
 foreign key (service_id) references Service(ID),
 foreign key (client_id) references Client(ID)
 
 );



