CREATE DATABASE bd_mysql_proyecto_eia;
USE bd_mysql_proyecto_eia;
 
create table football_statsLN (    
    teams varchar(100),    
    seasons varchar(50),    
    players varchar(100),    
    matches int,    
    goals int,    
    assists int,
    seasons_ratings decimal(3, 2) );