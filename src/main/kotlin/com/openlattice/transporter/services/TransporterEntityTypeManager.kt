package com.openlattice.transporter.services

import com.google.common.cache.CacheBuilder
import com.google.common.cache.CacheLoader
import com.openlattice.data.storage.MetadataOption
import com.openlattice.data.storage.buildPreparableFiltersSql
import com.openlattice.data.storage.partitions.PartitionManager
import com.openlattice.datastore.services.EdmManager
import com.openlattice.edm.EntitySet
import com.openlattice.edm.PostgresEdmTypeConverter
import com.openlattice.edm.type.EntityType
import com.openlattice.postgres.*
import com.openlattice.postgres.PostgresColumn.*
import com.openlattice.postgres.PostgresTable.DATA
import com.openlattice.postgres.streams.BasePostgresIterable
import com.openlattice.postgres.streams.PreparedStatementHolderSupplier
import com.zaxxer.hikari.HikariDataSource
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind
import org.slf4j.LoggerFactory
import java.sql.Connection
import java.sql.SQLException
import java.sql.Statement
import java.util.*


fun PostgresTableDefinition.defaults(): PostgresTableDefinition {
    this.addColumns(
            ENTITY_SET_ID,
            ID_VALUE,
            VERSION
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
        dataModelService: EdmManager,
        private val partitionManager: PartitionManager
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
    private val singletonTable = PostgresTableDefinition(quote("u_${et.id}")).defaults()

    private val validProperties =
        et.properties.asSequence().map(dataModelService::getPropertyType)
                .map { it to datatypes.get(it.datatype) }
                .filter { it.second.isPresent }
                .map {
                    val prop = it.first
                    val col = quote(it.first.id.toString())
                    val (single, array) = it.second.get()
                    singletonTable.addColumns(PostgresColumnDefinition(col, single))
                    prop
                }.associateBy { it.id }.toMutableMap()

    private fun executeLog(st: Statement, sql: String) {
        try {
            st.execute(sql)
        } catch (e: SQLException) {
            logger.error("Error running $sql", e)
            throw e;
        }
    }

    public fun createTables(c: Connection) {
        c.createStatement().use { st ->
            executeLog(st, singletonTable.createTableQuery())
        }
    }

    fun updateEntitySet(hds: HikariDataSource, es: EntitySet) {
        if (es.isLinking) {
            // we don't actually want to materialize duplicate data for each linked entity set. we might need to
            // eventually do other housekeeping
            return
        }
        val table = singletonTable

        val queryEntitySets = setOf(es.id)
        val partitions = partitionManager.getPartitionsByEntitySetId(queryEntitySets).values.flatten().toSet()

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
            logger.info("No modified data for entity set {} of type {}", es.name, et.type)
            return
        }

        val (bson_data, _) = buildPreparableFiltersSql(
                1,
                validProperties,
                mapOf(),
                EnumSet.of(MetadataOption.VERSION),
                es.isLinking,
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
                    logger.info("{} rows updated for entity set {} of type {}", count, es.name, et.type)
                }
            }
        } catch (e: SQLException) {
            logger.error("Error from SQL {}", updateQuery, e)
        }
    }
}