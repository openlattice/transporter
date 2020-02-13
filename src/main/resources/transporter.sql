create database transporter;
grant all on database transporter to oltest;
\c transporter
create extension if not exists postgres_fdw;
create server if not exists enterprise foreign data wrapper postgres_fdw options (host 'localhost', dbname 'openlattice', port '5433');
create user mapping if not exists for oltest server enterprise options (user 'oltest', password 'oltest');

create schema if not exists ol;

IMPORT FOREIGN SCHEMA public
FROM SERVER enterprise
INTO ol;

alter user oltest set search_path to ol, public;