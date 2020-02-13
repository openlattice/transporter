# Transporter

Transporter is a microservice for synchronizing materialized views
and the query database for top utilizers (and maybe someday permissions).

It works by regularly syncing tables from the data table (and other sources) as follows:

1. Find entities that are missing in 