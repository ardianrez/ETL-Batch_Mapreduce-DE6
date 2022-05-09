---PROJECT DE 3

create table dwh_fact_orders (
	"userid" int not null,
	"orderdate" date not null,
	"quantity" int not null,
	"productname" varchar(255) not null,
	"productcategory" varchar(255) not null,
	"price" float not null,
	"salesamount" float not null,
	"PropertyState" varchar(255) not null,
	"PropertyCity" varchar(255) not null);

create table dwh_dim_users (
	"UserID" int not null,
	"UserSex" varchar(255) not null,
	"UserDevice" varchar(255) not null);