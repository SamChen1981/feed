# Create tables that feed needs

drop table if exists personaltimeline1;

create table personaltimeline1 (
  uid BIGINT not null,
  ts BIGINT not null,
  valuekey varchar(255) not null,
  primary key(uid, ts, valuekey)
)engine=InnoDB default charset=utf8;

drop table if exists personaltimeline2;

create table personaltimeline2 (
  uid BIGINT not null,
  ts BIGINT not null,
  valuekey varchar(255) not null,
  primary key(uid, ts, valuekey)
)engine=InnoDB default charset=utf8;

drop table if exists valuestore;

create table valuestore (
  vid BIGINT NOT NULL AUTO_INCREMENT,
  valuekey  varchar(255) not null,
  value varchar(255),
  primary key(vid, valuekey, value)
)engine=InnoDB default charset=utf8;

drop table if exists likeslist;
# json
create table likeslist (
 uid BIGINT not null,
 lid BIGINT not null,
 ts datetime,
 primary key(uid, lid)
)engine=InnoDB default charset=utf8;

drop table if exists fanslist;
# json
create table fanslist (
 uid BIGINT not null,
 fid BIGINT not null,
 ts datetime,
 primary key(uid, fid)
)engine=InnoDB default charset=utf8;

drop table if exists pushfriendstimeline;

# json
create table pushfriendstimeline (
 uid BIGINT not null,
 lid BIGINT not null,
 ts BIGINT not null,
 valuekey varchar(255) not null,
 primary key(uid, lid, ts, valuekey)
)engine=InnoDB default charset=utf8;
