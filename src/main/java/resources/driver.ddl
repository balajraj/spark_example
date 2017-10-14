create table if not exists driver (
   id bigint not null,
   date_created timestamp not null,
   name varchar(255) not null,
   primary key(id)
) 
