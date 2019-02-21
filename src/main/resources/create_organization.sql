-- These commands have to be run for each organization database that is created.
CREATE EXTENSION postgres_fdw
CREATE SERVER bp FOREIGN DATA WRAPPER postgres_fdw OPTIONS( host 'blackpanther.openlattice.com', dbname 'openlattice', port '30001')
-- These commands are the