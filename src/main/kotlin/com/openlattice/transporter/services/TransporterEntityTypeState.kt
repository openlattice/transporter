package com.openlattice.transporter.services

import com.openlattice.data.storage.buildPreparableFiltersSql
import com.openlattice.datastore.services.EdmManager
import com.openlattice.edm.EntitySet
import com.openlattice.edm.PostgresEdmTypeConverter
import com.openlattice.edm.type.EntityType
import com.openlattice.edm.type.PropertyType
import com.openlattice.postgres.PostgresArrays
import com.openlattice.postgres.PostgresColumn
import com.openlattice.postgres.PostgresColumn.ENTITY_SET_ID
import com.openlattice.postgres.PostgresColumn.ID_VALUE
import com.openlattice.postgres.PostgresColumnDefinition
import com.openlattice.postgres.PostgresTableDefinition
import org.slf4j.LoggerFactory
import java.sql.Connection
import java.sql.SQLException
import java.sql.Statement


fun PostgresTableDefinition.defaults(): PostgresTableDefinition {
    this.addColumns(
            PostgresColumn.ENTITY_SET_ID,
            PostgresColumn.ID_VALUE
    )
    this.primaryKey(
            PostgresColumn.ENTITY_SET_ID,
            PostgresColumn.ID_VALUE
    )
    return this
}

fun quote(name: String): String {
    return "\"$name\""
}

class TransporterEntityTypeState(private val et: EntityType, dataModelService: EdmManager) {
    companion object {
        val logger = LoggerFactory.getLogger(TransporterEntityTypeState::class.java)
    }
    val singletonTable = PostgresTableDefinition(quote("u_${et.id}")).defaults()
    val linkedTable = PostgresTableDefinition(quote("l_${et.id}")).defaults()
    val properties: MutableList<PropertyType> = mutableListOf()
    init {
        et.properties.map(dataModelService::getPropertyType)
                .forEach { pt ->
                    try {
                        val single = PostgresEdmTypeConverter.map(pt.datatype)
                        val array = PostgresEdmTypeConverter.mapToArrayType(pt.datatype)
                        val col = quote(pt.id.toString())
                        singletonTable.addColumns(
                                PostgresColumnDefinition(col, single)
                        )
                        linkedTable.addColumns(
                                PostgresColumnDefinition(col, array)
                        )
                        properties.add(pt)
                    } catch (e: UnsupportedOperationException) {
                        logger.warn("Unsupported EDM data type {} for property {} in entity type {}",
                                pt.datatype, pt.type, et.type)
                    }
        }
    }
    private fun executeLog(st: Statement, sql: String) {
        logger.info("Executing $sql")
        try {
            st.execute(sql)
        } catch (e: SQLException) {
            logger.error("Error running $sql", e)
            throw e;
        }
    }

    public fun createTables(c: Connection) {
        c.createStatement().use {st ->
            logger.info("Creating table ${singletonTable.name}")
            executeLog(st, singletonTable.createTableQuery())
            logger.info("Creating table ${linkedTable.name}")
            executeLog(st, linkedTable.createTableQuery())
            logger.info("Created tables for ${et.type}")
        }

    }

    fun updateEntitySet(conn: Connection, es: EntitySet) {
        val (bson_data, _) = buildPreparableFiltersSql(
                1,
                properties.associate { it.id to it },
                mapOf(),
                setOf(),
                es.isLinking,
                idsPresent = false,
                partitionsPresent = false,
                detailed = false
                )
        val columns = properties.map {
            val id = quote(it.id.toString())
            if (es.isLinking) {
                val type = PostgresEdmTypeConverter.mapToArrayType(it.datatype).sql()
                "jsonb_array_elements_text(${PostgresColumn.PROPERTIES.name} -> '${it.id}')::$type AS $id"
            } else {
                val type = PostgresEdmTypeConverter.map(it.datatype).sql()
                "jsonb_array_elements_text(${PostgresColumn.PROPERTIES.name} -> '${it.id}')::$type AS $id"
            }
        }.joinToString(", ")
        val src = "SELECT ${ID_VALUE.name}, ${ENTITY_SET_ID.name}, $columns FROM ($bson_data) src"
        val table = if (es.isLinking) { linkedTable } else { singletonTable }
        val pk = table.primaryKey.map { it.name }.joinToString(", ")
        val valueColumns = table.columns.filterNot(table.primaryKey::contains)

        val SQL = "WITH src AS ($src) " +
                "INSERT INTO ${table.name} " +
                "SELECT * FROM src " +
                "ON CONFLICT ($pk) DO UPDATE " +
                "SET " + valueColumns.map { "${it.name} = excluded.${it.name}" }.joinToString(", ")

        try {
            conn.prepareStatement(SQL).use {
                it.setArray(1, PostgresArrays.createUuidArray(conn, es.id))
                val count = it.executeUpdate()
                logger.info("{} rows updated for entity set {} of type {}", count, es.name, et.type)
            }
        } catch (e: SQLException) {
            logger.error("Error from SQL {}", SQL, e)
        }
    }
}