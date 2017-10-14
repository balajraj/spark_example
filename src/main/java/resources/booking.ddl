create table if not exists booking (
 id bigint not null,
 date_created timestamp not null,
 id_driver bigint not null references driver(id),
 id_passenger bigint not null references passenger(id),
 rating integer check (rating > 0) not null,
 start_date timestamp,
 end_date timestamp,
 tour_value bigint check ( tour_value > 0) not null,
 primary key (id)
 )
