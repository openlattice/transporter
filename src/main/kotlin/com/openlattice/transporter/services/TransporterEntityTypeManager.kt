package com.openlattice.transporter.services

import com.google.common.cache.CacheBuilder
import com.google.common.cache.CacheLoader
import com.openlattice.IdConstants
import com.openlattice.data.storage.MetadataOption
import com.openlattice.data.storage.buildPreparableFiltersSql
import com.openlattice.data.storage.partitions.PartitionManager
import com.openlattice.datastore.services.EdmManager
import com.openlattice.datastore.services.EntitySetManager
import com.openlattice.edm.EntitySet
import com.openlattice.edm.PostgresEdmTypeConverter
import com.openlattice.edm.set.EntitySetFlag
import com.openlattice.edm.type.EntityType
import com.openlattice.edm.type.PropertyType
import com.openlattice.postgres.*
import com.openlattice.postgres.DataTables.LAST_WRITE
import com.openlattice.postgres.PostgresColumn.*
import com.openlattice.postgres.PostgresTable.DATA
import com.openlattice.postgres.streams.BasePostgresIterable
import com.openlattice.postgres.streams.PreparedStatementHolderSupplier
import com.zaxxer.hikari.HikariDataSource
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind
import org.slf4j.LoggerFactory
import java.sql.SQLException
import java.sql.Statement
import java.util.*

const val batchSize = 64_000
const val olSchema = "ol"
const val transporterSchema = "transporter"

val mode = 2

fun PostgresTableDefinition.defaults(): PostgresTableDefinition {
    this.addColumns(
            ENTITY_SET_ID,
            ID_VALUE,
            // this is to avoid confusion when benchmarking code with and without version
            PostgresColumnDefinition(VERSION.name, VERSION.datatype).withDefault(0)
    )
    this.primaryKey(
            ENTITY_SET_ID,
            ID_VALUE
    )
    return this
}

fun quote(name: String): String {
    return "\"$name\""
}

class TransporterEntityTypeManager(
        private val et: EntityType,
        private val dataModelService: EdmManager,
        private val partitionManager: PartitionManager,
        private val entitySetService: EntitySetManager
) {
    companion object {
        private val logger = LoggerFactory.getLogger(TransporterEntityTypeManager::class.java)
        private val loader = object : CacheLoader<EdmPrimitiveTypeKind, Optional<Pair<PostgresDatatype, PostgresDatatype>>>() {
            override fun load(key: EdmPrimitiveTypeKind): Optional<Pair<PostgresDatatype, PostgresDatatype>> {
                return try {
                    val single = PostgresEdmTypeConverter.map(key)
                    val array = PostgresEdmTypeConverter.mapToArrayType(key)
                    Optional.of(Pair(single, array))
                } catch (e: java.lang.UnsupportedOperationException) {
                    logger.warn("Unsupported EDM data type {}", key)
                    Optional.empty()
                }
            }
        }
        private val datatypes = CacheBuilder.newBuilder().build(loader)
    }
    private val table = PostgresTableDefinition(quote("et_${et.id}")).defaults()

    private val validProperties =
        et.properties.asSequence().map(dataModelService::getPropertyType)
                .map { it to datatypes.get(it.datatype) }
                .filter { it.second.isPresent }
                .map {
                    val prop = it.first
                    val col = quote(it.first.id.toString())
                    val (single, _) = it.second.get()
                    table.addColumns(PostgresColumnDefinition(col, single))
                    prop
                }.associateBy { it.id }.toMutableMap()

    private fun executeLog(st: Statement, sql: String): Boolean {
        return try {
            logger.debug(sql)
            st.execute(sql)
        } catch (e: SQLException) {
            logger.error("Error running $sql", e)
            throw e
        }
    }

    public fun createTable(transporter: HikariDataSource, enterprise: HikariDataSource) {
        transporter.connection.use { c ->
            c.autoCommit = false
            c.createStatement().use { st ->
                executeLog(st, "DROP TABLE ${table.name}")
                executeLog(st, table.createTableQuery())
            }
            c.commit()
        }
        enterprise.connection.use { c ->
            c.createStatement().use {st ->
                val drop = "DROP FOREIGN TABLE IF EXISTS transporter.${table.name}"
                val createForeignTable = "IMPORT FOREIGN SCHEMA public LIMIT TO ( ${table.name} ) FROM SERVER transporter INTO transporter;"
                executeLog(st, drop)
                executeLog(st, createForeignTable)
            }
        }
    }

    fun updateAllEntitysets(enterprise: HikariDataSource, transporter: HikariDataSource) {
        val entitySets = entitySetService.getEntitySetsOfType(this.et.id)
                .filter { !it.isLinking && !it.flags.contains(EntitySetFlag.AUDIT) }
                .toSet()
        updateEntitySets(enterprise, transporter, entitySets)
    }

    fun updateEntitySet(enterprise: HikariDataSource, transporter: HikariDataSource, es: EntitySet) {
        updateEntitySets(enterprise, transporter, setOf(es))
    }
    fun updateEntitySets(enterprise: HikariDataSource, transporter: HikariDataSource, sets: Set<EntitySet>) {
        if (!checkValid(sets)) {
            throw IllegalArgumentException("Invalid input sets")
        }
        when (mode) {
            0 -> pullUpdatedColumnarViewFromTransporter(transporter, sets)
            1 -> pushColumnsFromEnterpriseStrategy(enterprise, sets)
            2 -> pullBatchFromIdsAndAssemble(transporter, sets)
        }
    }

    private fun getSourceColumnName(propertyType: PropertyType): String {
        return PostgresDataTables.getSourceDataColumnName(
            datatypes[propertyType.datatype].get().first,
                propertyType.postgresIndexType)
    }

    private fun checkValid(sets: Set<EntitySet>): Boolean {
        return sets.all {
            !it.isLinking && !it.flags.contains(EntitySetFlag.AUDIT)
        }
    }

    private fun entitySetIds(sets: Set<EntitySet>): Set<UUID> {
        return sets.associateBy { it.id }.keys
    }

    private fun partitions(entitySetIds: Set<UUID>): Collection<Int> {
        return partitionManager.getPartitionsByEntitySetId(entitySetIds).values.flatten().toSet()
    }

    private inner class SingleColumnQueryHolder(val propertyType: PropertyType) {
        val srcCol = getSourceColumnName(propertyType)
        val destCol = quote(propertyType.id.toString())
        val tempTable = "current_import_batch"

        fun bpUpdate(): String {
            return "update ${DATA.name} " +
                    "set ${LAST_PROPAGATE.name} = ${LAST_WRITE.name} " +
                    "where ${PARTITION.name} = ANY(?) " +
                    "AND ${ENTITY_SET_ID.name} = ANY(?) " +
                    "AND ${PROPERTY_TYPE_ID.name} = '${propertyType.id}' " +
                    "AND ${ORIGIN_ID.name} = '${IdConstants.EMPTY_ORIGIN_ID.id}' " +
                    "AND ${LAST_PROPAGATE.name} < ${LAST_WRITE.name} " +
                    "RETURNING ${ENTITY_SET_ID.name}, ${ID.name}, ${VERSION.name}, " +
                    "case when ${VERSION.name} > 0 then $srcCol else null end as $destCol"
        }

        fun createTempTable(): String {
            return "with src as (${bpUpdate()}) select * into temp table $tempTable from src"
        }
        fun updateFromTempTable(): String {
            return updateQuery(tempTable)
        }
        fun updateQuery(src: String): String {
            val pk = listOf(ENTITY_SET_ID, ID).joinToString(", ") { it.name }
            val cols = "$pk,$destCol"
            return "INSERT INTO ${table.name} " +
                    "($cols) " +
                    "SELECT $cols " +
                    "FROM $src " +
                    "ON CONFLICT ($pk) DO UPDATE " +
                    "SET $destCol = excluded.$destCol"
        }

        fun oneShot(): String {
            return "WITH src as (${bpUpdate()}) " + updateQuery("src")
        }

        fun cleanupQuery(): String {
            return "DROP TABLE $tempTable"
        }
    }

    private fun pullBatchFromIdsAndAssemble(hds: HikariDataSource, sets: Set<EntitySet>) {
        hds.connection.use { conn ->
            conn.autoCommit = false
            var lastSql = ""
            try {
                val entitySetIds = entitySetIds(sets)
                val entitySetArray = PostgresArrays.createUuidArray(conn, entitySetIds)
                val partitions = PostgresArrays.createIntArray(conn, partitions(entitySetIds))
                val updates = this.validProperties.values.map { pt ->
                    val queries = SingleColumnQueryHolder(pt)
                    if (true) {
                        lastSql = queries.oneShot()
                        conn.prepareStatement(queries.oneShot()).use {ps ->
                            ps.setArray(1, partitions)
                            ps.setArray(2, entitySetArray)
                            ps.executeUpdate()
                        }
                    } else {
                        lastSql = queries.createTempTable()
                        val count1 = conn.prepareStatement(queries.createTempTable()).use { ps ->
                            ps.setArray(1, partitions)
                            ps.setArray(2, entitySetArray)
                            ps.executeUpdate()
                        }
                        val count2 = conn.createStatement().use {
                            lastSql = queries.updateFromTempTable()
                            val count = it.executeUpdate(queries.updateFromTempTable())

                            lastSql = queries.cleanupQuery()
                            it.execute(queries.cleanupQuery())
                            count
                        }
                        if (count1 != count2) {
                            logger.warn("We updated {} rows in data but wrote {} rows to {}", count1, count2, table.name)
                        }
                        count2
                    }
                }.sum()
                logger.info("Updated {} properties in entity type {}", updates, et.type)
                conn.commit()
            } catch (ex: Exception) {
                logger.error("Unable to update transporter: SQL: {}", lastSql, ex)
                conn.rollback()
                throw ex
            }
        }
    }

    fun pushColumnsFromEnterpriseStrategy(hds: HikariDataSource, sets: Set<EntitySet>) {
        val entitySetIds = entitySetIds(sets)
        hds.connection.use { conn ->
            conn.autoCommit = false

            val entitySetArray = PostgresArrays.createUuidArray(conn, entitySetIds)
            val partitions = PostgresArrays.createIntArray(conn, partitions(entitySetIds))
            val propertyTypeIds = PostgresArrays.createUuidArray(conn, validProperties.keys)

            try {
                val tempTable = "transporter_batch"
                val batchQuery = "create temporary table $tempTable on commit drop as " +
                        " SELECT * " +
                        " FROM ${DATA.name} " +
                        " WHERE ${VERSION.name} > 0 " +
                        " AND ${ENTITY_SET_ID.name} = ANY(?) " +
                        " AND ${PARTITION.name} = ANY(?) " +
                        " AND ${PROPERTY_TYPE_ID.name} = ANY(?) " +
                        " AND ${LAST_PROPAGATE.name} < ${LAST_WRITE.name} " +
                        " LIMIT $batchSize"
                val count = conn.prepareStatement(batchQuery).use { st ->
                    st.setArray(1, entitySetArray)
                    st.setArray(2, partitions)
                    st.setArray(3, propertyTypeIds)
                    logger.debug(batchQuery)
                    val count = st.executeUpdate()
                    logger.info("$count rows in batch")
                    count
                }
                if (count > 0) {
                    conn.createStatement().use { st ->
                        val ids = "${ENTITY_SET_ID.name}, ${ID.name}"
                        val newIdCount = st.executeUpdate("with ids as (select distinct $ids from $tempTable) " +
                                "insert into $transporterSchema.${table.name} ($ids) " +
                                "select $ids from ids on conflict do nothing")
                        logger.info("$newIdCount new ids in type ${et.type}")

                        val updatedPropCount = validProperties.map { (id, pt) ->
                            val type = datatypes[pt.datatype].get().first
                            val srcCol = PostgresDataTables.getSourceDataColumnName(type, pt.postgresIndexType)
                            val destCol = quote(id.toString())

                            val updateProperty = "UPDATE $transporterSchema.${table.name} dest " +
                                    " SET $destCol = $srcCol " +
                                    " FROM $tempTable src " +
                                    " WHERE src.${ENTITY_SET_ID.name} = dest.${ENTITY_SET_ID.name} " +
                                    " AND src.${ID.name} = dest.${ID.name} " +
                                    " AND src.${PROPERTY_TYPE_ID.name} = '$id'"
                            logger.debug(updateProperty)
                            st.executeUpdate(updateProperty)
                        }.sum()
                        logger.info("Updated {} property values", updatedPropCount)
                        val flushWriteVersion = "UPDATE ${DATA.name} dest " +
                                " SET ${LAST_PROPAGATE.name} = src.${LAST_WRITE.name} " +
                                " FROM $tempTable src " +
                                " WHERE " + arrayOf(PARTITION, ID, PROPERTY_TYPE_ID).joinToString(" AND ") {
                            "dest.${it.name} = src.${it.name}"
                        }
                        logger.debug("SQL: {}", flushWriteVersion)
                        val lastWriteCount = st.executeUpdate(flushWriteVersion)
                        logger.info("Flushed {} writes", lastWriteCount)
                    }
                }
                logger.info("committing transaction")
                conn.commit()

            } catch (e: Throwable) {
                logger.error("Rolling back transaction", e)
                conn.rollback()
                throw e
            }
        }
    }

    fun pullUpdatedColumnarViewFromTransporter(hds: HikariDataSource, allSets: Set<EntitySet>) {
        val sets = allSets.filter { !it.isLinking }
        if (sets.isEmpty()) {
            return
        }

        val setNames = sets.joinToString(", ") { es -> es.name }
        val queryEntitySets = sets.map(EntitySet::getId).toSet()
        val partitions = partitions(queryEntitySets)

        val filterQuery = "select distinct d.${ID.name} " +
                "from ${DATA.name} d " +
                "left join ${table.name} t " +
                "using (${ENTITY_SET_ID.name}, ${ID.name}) " +
                "where d.${ENTITY_SET_ID.name} = ANY(?) " +
                "AND d.${PARTITION.name} = ANY(?) " +
                "AND d.${VERSION.name} > coalesce(t.${VERSION.name}, 0);"
        val modifiedEntities = BasePostgresIterable<UUID>(
                PreparedStatementHolderSupplier(hds, filterQuery) { ps ->
                    ps.setArray(1, PostgresArrays.createUuidArray(ps.connection, queryEntitySets))
                    ps.setArray(2, PostgresArrays.createIntArray(ps.connection, partitions))
                }) { rs ->
            ResultSetAdapters.id(rs)
        }.toList()

        if (modifiedEntities.isEmpty()) {
            logger.info("No modified data for entity sets {} of type {}", setNames, et.type)
            return
        }

        val (bson_data, _) = buildPreparableFiltersSql(
                1,
                validProperties,
                mapOf(),
                EnumSet.of(MetadataOption.VERSION),
                linking = false,
                idsPresent = true,
                partitionsPresent = true,
                detailed = false
        )
        val columns = validProperties.values.joinToString(", ") { pt ->
            val id = quote(pt.id.toString())
            val type = datatypes[pt.datatype].get().first
            "jsonb_array_elements_text(${PROPERTIES.name} -> '${pt.id}')::${type.sql()} AS $id"
        }
        val src = "SELECT ${ENTITY_SET_ID.name}, ${ID_VALUE.name}, ${VERSION.name}, $columns FROM ($bson_data) src"

        val pk = table.primaryKey.joinToString(", ") { it.name }
        val valueColumns = table.columns.filterNot(table.primaryKey::contains)
        val colList = table.columns.joinToString(", ") { it.name }

        val updateQuery = "WITH src AS ($src) " +
                "INSERT INTO ${table.name} " +
                "($colList) " +
                "SELECT $colList FROM src " +
                "ON CONFLICT ($pk) DO UPDATE " +
                "SET " + valueColumns.joinToString(", ") { "${it.name} = excluded.${it.name}" }
        try {
            hds.connection.use { conn ->
                conn.prepareStatement(updateQuery).use {
                    it.setArray(1, PostgresArrays.createUuidArray(conn, queryEntitySets))
                    it.setArray(2, PostgresArrays.createUuidArray(conn, modifiedEntities))
                    it.setArray( 3, PostgresArrays.createIntArray(conn, partitions))
                    val count = it.executeUpdate()
                    logger.info("{} rows updated for entity sets {} of type {}", count, setNames, et.type)
                }
            }
        } catch (e: SQLException) {
            logger.error("Error from SQL {}", updateQuery, e)
        }
    }
}